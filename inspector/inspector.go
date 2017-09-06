package inspector

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/redis"
	"github.com/ksarch-saas/cc/topo"
)

var (
	ErrNoSeed           = errors.New("inspector: no seed node found")
	ErrInvalidTag       = errors.New("inspector: invalid tag")
	ErrEmptyTag         = errors.New("inspector: empty tag")
	ErrNodeNoAddr       = errors.New("inspector: node flag contains noaddr")
	ErrNodeInHandShake  = errors.New("inspector: node flag contains handshake")
	ErrSeedIsFreeNode   = errors.New("inspector: seed is free node")
	ErrNodeNotExist     = errors.New("inspector: node not exist")
	ErrNodesInfoNotSame = errors.New("inspector: cluster nodes info of seeds are different")
	ErrUnknown          = errors.New("inspector: unknown error")
)

type Inspector struct {
	mutex       *sync.RWMutex
	LocalRegion string //当前地域
	SeedIndex   int
	ClusterTopo *topo.Cluster //集群拓扑
}

func NewInspector() *Inspector {
	sp := &Inspector{
		mutex:       &sync.RWMutex{},
		LocalRegion: meta.LocalRegion(),
	}
	return sp
}

//读取数据重建node
func (self *Inspector) buildNode(line string) (*topo.Node, bool, error) {
	xs := strings.Split(line, " ")
	mod, tag, id, addr, flags, parent := xs[0], xs[1], xs[2], xs[3], xs[4], xs[5]
	node := topo.NewNodeFromString(addr)
	ranges := []string{}
	for _, word := range xs[10:] {
		if strings.HasPrefix(word, "[") {
			word = word[1 : len(word)-1]
			xs := strings.Split(word, "->-")
			if len(xs) == 2 {
				slot, _ := strconv.Atoi(xs[0])
				node.AddMigrating(xs[1], slot)
			}
			xs = strings.Split(word, "-<-")
			if len(xs) == 2 {
				slot, _ := strconv.Atoi(xs[0])
				node.AddImporting(xs[1], slot)
			}
			continue
		}
		ranges = append(ranges, word)
	}

	for _, r := range ranges {
		xs = strings.Split(r, "-")
		if len(xs) == 2 {
			left, _ := strconv.Atoi(xs[0])
			right, _ := strconv.Atoi(xs[1])
			node.AddRange(topo.Range{left, right})
		} else {
			left, _ := strconv.Atoi(r)
			node.AddRange(topo.Range{left, left})
		}
	}

	// basic info
	node.SetId(id)
	node.SetParentId(parent)
	node.SetTag(tag)
	node.SetReadable(mod[0] == 'r')
	node.SetWritable(mod[1] == 'w')
	myself := false
	if strings.Contains(flags, "myself") {
		myself = true
	}
	if strings.Contains(flags, "master") {
		node.SetRole("master")
	} else if strings.Contains(flags, "slave") {
		node.SetRole("slave")
	}
	if strings.Contains(flags, "noaddr") {
		return nil, myself, ErrNodeNoAddr
	}
	if strings.Contains(flags, "handshake") {
		return nil, myself, ErrNodeInHandShake
	}
	if strings.Contains(flags, "fail?") {
		node.SetPFail(true)
		node.IncrPFailCount()
	}
	xs = strings.Split(tag, ":")
	if len(xs) == 3 {
		node.SetRegion(xs[0])
		node.SetZone(xs[1])
		node.SetRoom(xs[2])
	} else if node.Tag != "-" {
		return nil, myself, ErrInvalidTag
	}

	return node, myself, nil
}

//增加节点
func (self *Inspector) MeetNode(node *topo.Node) {
	for _, seed := range meta.Seeds() {
		if seed.Ip == node.Ip && seed.Port == node.Port {
			continue
		}
		_, err := redis.ClusterMeet(seed.Addr(), node.Ip, node.Port)
		if err == nil {
			break
		}
	}
}

//根据一个node初始化集群
func (self *Inspector) initClusterTopo(seed *topo.Node) (*topo.Cluster, error) {

	//使用参考的node获取本地域的Node
	resp, err := redis.ClusterNodesInRegion(seed.Addr(), self.LocalRegion)
	//兼容原生的redis
	if err != nil && strings.HasPrefix(err.Error(), "ERR Wrong CLUSTER subcommand or number of arguments") {
		//server version do not support 'cluster nodes extra [region]'
		resp, err = redis.ClusterNodes(seed.Addr())
	}
	if err != nil {
		return nil, err
	}

	//新建cluster
	cluster := topo.NewCluster(self.LocalRegion)

	var summary topo.SummaryInfo
	var nodeidx *topo.Node
	var cnt int

	lines := strings.Split(resp, "\n")
	cnt = 0

	//解析从参考节点上读取到的信息
	for _, line := range lines {
		if strings.HasPrefix(line, "# ") {
			summary.ReadLine(line)
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		//按照读取到的信息，初始化node
		node, myself, err := self.buildNode(line)
		if err == ErrNodeInHandShake || err == ErrNodeNoAddr {
			continue
		}
		// Fix 'cluster nodes extra' & 'cluster nodes extra region' compatiable
		if node.Region != self.LocalRegion {
			continue
		}
		if err != nil {
			return nil, err
		}
		if node.Ip == "127.0.0.1" {
			node.Ip = seed.Ip
		}
		// 遇到myself，读取该节点的ClusterInfo
		if myself {
			info, err := redis.FetchClusterInfo(node.Addr())
			if err != nil {
				return nil, err
			}
			node.ClusterInfo = info
			node.SummaryInfo = summary
		}
		cluster.AddNode(node)
		nodeidx = node
		cnt++
	}
	if cnt == 1 {
		if nodeidx.IsMaster() && len(nodeidx.Ranges) == 0 {
			glog.Infof("Node %s is free node", nodeidx.Addr())
			nodeidx.SetFree(true)
		}
	}

	return cluster, nil
}

func (self *Inspector) isFreeNode(seed *topo.Node) (bool, *topo.Node) {
	resp, err := redis.ClusterNodesInRegion(seed.Addr(), self.LocalRegion)
	if err != nil && strings.HasPrefix(err.Error(), "ERR Wrong CLUSTER subcommand or number of arguments") {
		//server version do not support 'cluster nodes extra [region]'
		resp, err = redis.ClusterNodes(seed.Addr())
	}
	if err != nil {
		return false, nil
	}
	numNode := 0
	lines := strings.Split(resp, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "# ") {
			continue
		}
		numNode++
	}
	if numNode != 1 {
		return false, nil
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "# ") {
			continue
		}
		node, myself, err := self.buildNode(line)
		if node.Ip == "127.0.0.1" {
			node.Ip = seed.Ip
		}
		// 只看到自己，是主，且没有slots，才认为是FreeNode
		if !myself {
			return false, nil
		}
		if err != nil || len(node.Ranges) > 0 || !node.IsMaster() {
			return false, nil
		} else {
			return true, node
		}
	}
	return false, nil
}

func (self *Inspector) checkClusterTopo(seed *topo.Node, cluster *topo.Cluster) error {

	resp, err := redis.ClusterNodesInRegion(seed.Addr(), self.LocalRegion)
	if err != nil && strings.HasPrefix(err.Error(), "ERR Wrong CLUSTER subcommand or number of arguments") {
		//server version do not support 'cluster nodes extra [region]'
		resp, err = redis.ClusterNodes(seed.Addr())
	}

	//this may lead to BuildClusterTopo update failed for a time
	//the node is step into this state after check IsAlive
	if err != nil && strings.HasPrefix(err.Error(), "LOADING") {
		return nil
	}
	if err != nil {
		return err
	}

	var summary topo.SummaryInfo
	//按行读取summary
	lines := strings.Split(resp, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "# ") {
			summary.ReadLine(line)
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		s, myself, err := self.buildNode(line)
		if err == ErrNodeInHandShake || err == ErrNodeNoAddr {
			continue
		}
		// Fix 'cluster nodes extra' & 'cluster nodes extra region' compatiable
		if s.Region != self.LocalRegion {
			continue
		}
		if err != nil {
			return err
		}
		if s.Ip == "127.0.0.1" {
			s.Ip = seed.Ip
		}
		//当前节点
		node := cluster.FindNode(s.Id)
		if node == nil {
			if s.PFail {
				glog.Warningf("forget dead node %s(%s) should be forgoten", s.Id, s.Addr())
				//redis.ClusterForget(seed.Addr(), s.Id)
			}
			return fmt.Errorf("node not exist %s(%s)", s.Id, s.Addr())
		}

		// 对比节点数据是否相同
		//node 是当前seed上的节点，s是从node上读取的别的节点
		if !node.Compare(s) {
			glog.Infof("%#v vs %#v different", s, node)
			if s.Tag == "-" && node.Tag != "-" {
				// 可能存在处于不被Cluster接受的节点，节点可以看见Cluster，但Cluster看不到它。
				// 一种复现情况情况：某个节点已经死了，系统将其Forget，但是OP并未被摘除该节点，
				// 而是恢复了该节点。
				glog.Warningf("remeet node %s", seed.Addr())
				self.MeetNode(seed)
			}
			return ErrNodesInfoNotSame
		}
		if len(node.Ranges) == 0 && len(s.Ranges) > 0 {
			glog.Warningf("Ranges not equal, use nonempty ranges.")
			node.Ranges = s.Ranges
		}

		if myself {
			info, err := redis.FetchClusterInfo(node.Addr())
			if err != nil {
				return err
			}
			node.ClusterInfo = info
			node.SummaryInfo = summary
		}

		if len(s.Migrating) != 0 {
			node.Migrating = s.Migrating
		}
		if len(s.Importing) != 0 {
			node.Importing = s.Importing
		}

		if s.PFail {
			node.IncrPFailCount()
		}
	}
	return nil
}

//生成ClusterSnapshot
//返回cluster和node
func (self *Inspector) BuildClusterTopo() (*topo.Cluster, []*topo.Node, error) {
	//需要加锁
	self.mutex.Lock()
	defer self.mutex.Unlock()

	//meta中的记录为空，则不进行处理，需要从meta中读取
	if len(meta.Seeds()) == 0 {
		return nil, nil, ErrNoSeed
	}

	// 过滤掉连接不上的节点，只保留一个好的节点
	seeds := []*topo.Node{}
	for _, s := range meta.Seeds() {
		if redis.IsAlive(s.Addr()) {
			seeds = append(seeds, s)
		} else {
			// remove this seed from meta seeds
			// will re-add to seeds if join the cluster again
			meta.RemoveSeed(s.Addr())
		}
	}

	//seed为空，没有活着的节点？
	//TDO: 可以记录一条日志
	if len(seeds) == 0 {
		return nil, seeds, ErrNoSeed
	}

	// 顺序选一个节点，获取nodes数据作为基准，再用其他节点的数据与基准做对比
	if self.SeedIndex >= len(seeds) {
		self.SeedIndex = len(seeds) - 1
	}
	var seed *topo.Node
	//遍历活着的node，进行处理
	for i := 0; i < len(seeds); i++ {
		//随机取出一个node
		seed = seeds[self.SeedIndex]

		//换一下Index，决定下一次的index，保证可以随机，可以换rand算法吧？
		self.SeedIndex++
		self.SeedIndex %= len(seeds)

		//如果节点是非线上的节点，需要换一个
		if seed.Free {
			glog.Info("Seed node is free ", seed.Addr())
		} else {
			break
		}
	}

	//找到一个参考的node，去同步其他的node的状态，返回一个重建好的cluster
	cluster, err := self.initClusterTopo(seed)
	if err != nil {
		glog.Infof("InitClusterTopo failed")
		return nil, seeds, err
	}

	// 检查所有节点返回的信息是不是相同，如果不同说明正在变化中，直接返回等待重试
	if len(seeds) > 1 {
		//遍历新的seeds
		for _, s := range seeds {
			if s == seed {
				continue
			}
			err := self.checkClusterTopo(s, cluster)
			if err != nil {
				free, node := self.isFreeNode(s)
				if free {
					node.Free = true
					glog.Infof("Found free node %s", node.Addr())
					//把FreeNode添加到Cluster中
					cluster.AddNode(node)
				} else {
					glog.Infof("checkClusterTopo failed")
					return cluster, seeds, err
				}
			} else {
				s.Free = false
			}
		}
	}

	// 遍历本地的所有node，构造LocalRegion视图
	for _, s := range cluster.LocalRegionNodes() {

		//节点被超过一半以上的node投票，则记录日志，设置节点为Fail
		if s.PFailCount() > cluster.NumLocalRegionNode()/2 {
			glog.Infof("Found %d/%d PFAIL state on %s, set FAIL",
				s.PFailCount(), cluster.NumLocalRegionNode(), s.Addr())
			s.SetFail(true)
		}
	}

	//所有地域的主才有权限构建主从
	if meta.IsClusterLeader() {
		cluster.BuildReplicaSets()
	}

	//更新本地记录的node列表
	meta.MergeSeeds(cluster.LocalRegionNodes())
	self.ClusterTopo = cluster

	return cluster, seeds, nil
}
