package state

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/redis"
	"github.com/ksarch-saas/cc/topo"
)

var (
	ErrNodeNotExist = errors.New("cluster: node not exist")
)

//集群状态信息
type ClusterState struct {
	version    int64                 // 更新消息处理次数
	cluster    *topo.Cluster         // 集群拓扑快照
	nodeStates map[string]*NodeState // 节点状态机
}

//新建一个状态
func NewClusterState() *ClusterState {
	cs := &ClusterState{
		version:    0,
		nodeStates: map[string]*NodeState{},
	}
	return cs
}

//查询cluster中的所有节点的状态机
func (cs *ClusterState) AllNodeStates() map[string]*NodeState {
	return cs.nodeStates
}

//按照节点id进行排序，返回第一个节点
func (cs *ClusterState) GetFirstNodeState() *NodeState {
	// only choose health node
	var keys []string
	for _, ns := range cs.nodeStates {
		if ns.Node().Fail {
			continue
		}
		keys = append(keys, ns.Node().Id)
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return cs.nodeStates[keys[0]]
}

//更新本地域所有节点的状态
func (cs *ClusterState) UpdateRegionNodes(region string, nodes []*topo.Node) {
	//版本+1
	cs.version++
	now := time.Now()

	//会记录一条日志
	log.Verbosef("CLUSTER", "Update region %s %d nodes", region, len(nodes))

	// 添加不存在的节点，版本号+1
	//遍历传入的node列表
	for _, n := range nodes {
		//确保只更新本地域的节点的状态
		if n.Region != region {
			continue
		}

		//读取node现在的状态
		nodeState := cs.nodeStates[n.Id]
		//如果node现在的状态为空，则创建一个新的状态，直接赋值为新的状态
		if nodeState == nil {
			nodeState = NewNodeState(n, cs.version)
			cs.nodeStates[n.Id] = nodeState
		} else {
			//更新node状态的版本
			nodeState.version = cs.version

			//向Stream中发送不同状态变换的日志，新的状态和老的状态不匹配的时候发送
			//考虑的是三种状态:Fail\Readable\Writeable
			if nodeState.node.Fail != n.Fail {
				log.Eventf(n.Addr(), "Fail state changed, %v -> %v", nodeState.node.Fail, n.Fail)
			}
			if nodeState.node.Readable != n.Readable {
				log.Eventf(n.Addr(), "Readable state changed, %v -> %v", nodeState.node.Readable, n.Readable)
			}
			if nodeState.node.Writable != n.Writable {
				log.Eventf(n.Addr(), "Writable state changed, %v -> %v", nodeState.node.Writable, n.Writable)
			}
			//更新node状态
			nodeState.node = n
		}
		nodeState.updateTime = now
	}

	// 删除已经下线的节点
	for id, n := range cs.nodeStates {
		if n.node.Region != region {
			continue
		}
		//遍历每个node，版本没有更新，就删除node
		nodeState := cs.nodeStates[id]
		if nodeState.version != cs.version {
			log.Warningf("CLUSTER", "Delete node %s", nodeState.node)
			delete(cs.nodeStates, id)
		}
	}

	// NB：低效？
	//重新构建拓扑
	cs.BuildClusterSnapshot()
}

//查询当前的集群的拓扑状态
func (cs *ClusterState) GetClusterSnapshot() *topo.Cluster {
	return cs.cluster
}

//构建拓扑
func (cs *ClusterState) BuildClusterSnapshot() {
	// __CC__没什么意义，跟Region区别开即可
	cluster := topo.NewCluster("__CC__")
	for _, ns := range cs.nodeStates {
		cluster.AddNode(ns.node)
	}

	//设置cluster的主从关系
	err := cluster.BuildReplicaSets()
	// 出现这种情况，很可能是启动时节点还不全
	if err != nil {
		log.Info("CLUSTER ", "Build cluster snapshot failed ", err)
		return
	}
	cs.cluster = cluster
}

//根据节点id，查询节点
func (cs *ClusterState) FindNode(nodeId string) *topo.Node {
	ns := cs.FindNodeState(nodeId)
	if ns == nil {
		return nil
	}
	return ns.node
}

//根据节点id，查询节点现在的状态
func (cs *ClusterState) FindNodeState(nodeId string) *NodeState {
	return cs.nodeStates[nodeId]
}

func (cs *ClusterState) DebugDump() {
	var keys []string
	for k := range cs.nodeStates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Println("Cluster Debug Information:")
	for _, key := range keys {
		fmt.Print("  ")
		cs.nodeStates[key].DebugDump()
	}
}

//查找节点的从节点
func (cs *ClusterState) FindReplicaSetByNode(nodeId string) *topo.ReplicaSet {
	if cs.cluster != nil {
		return cs.cluster.FindReplicaSetByNode(nodeId)
	} else {
		return nil
	}
}

/// helpers

// 获取分片内主地域中ReplOffset最大的节点ID
//master_repl_offset  全局的数据同步偏移量
func (cs *ClusterState) MaxReploffSlibing(nodeId string, region string, slaveOnly bool) (string, error) {
	//查找到node的ReplicaSet
	rs := cs.FindReplicaSetByNode(nodeId)
	if rs == nil {
		return "", ErrNodeNotExist
	}

	rmap := cs.FetchReplOffsetInReplicaSet(rs)

	var maxVal int64 = -1
	maxId := ""
	for id, val := range rmap {
		node := cs.FindNode(id)
		if slaveOnly && node.IsMaster() {
			continue
		}
		if node.Region != region {
			continue
		}
		if val > maxVal {
			maxVal = val
			maxId = id
		}
	}

	return maxId, nil
}

//节点数据复制偏移量
type reploff struct {
	NodeId string //节点id
	Offset int64  //offset
}

// 失败返回-1
//从redis读取offset
func fetchReplOffset(addr string) int64 {
	//从redis中读取info
	info, err := redis.FetchInfo(addr, "Replication")
	if err != nil {
		return -1
	}
	if info.Get("role") == "master" {
		offset, err := info.GetInt64("master_repl_offset")
		if err != nil {
			return -1
		} else {
			return offset
		}
	}
	offset, err := info.GetInt64("slave_repl_offset")
	if err != nil {
		return -1
	}
	return offset
}

// 获取分片内ReplOffset节点，包括Master
func (cs *ClusterState) FetchReplOffsetInReplicaSet(rs *topo.ReplicaSet) map[string]int64 {
	//读取所有的node
	nodes := rs.AllNodes()
	c := make(chan reploff, len(nodes))

	//遍历node
	for _, node := range nodes {
		go func(id, addr string) {
			//从redis上读取offset
			offset := fetchReplOffset(addr)
			c <- reploff{id, offset}
		}(node.Id, node.Addr())
	}

	rmap := map[string]int64{}
	for i := 0; i < len(nodes); i++ {
		off := <-c
		rmap[off.NodeId] = off.Offset
	}
	return rmap
}

//执行故障迁移任务
func (cs *ClusterState) RunFailoverTask(oldMasterId, newMasterId string) {
	//新节点状态
	new := cs.FindNodeState(newMasterId)
	//老节点状态
	old := cs.FindNodeState(oldMasterId)

	//找不到老的节点就不再执行故障迁移
	if old == nil {
		log.Warningf(oldMasterId, "Can't run failover task, the old dead master lost")
		return
	}
	//找不到新节点，也不执行故障迁移任务
	if new == nil {
		log.Warningf(oldMasterId, "Can't run failover task, new master lost (%s)", newMasterId)
		//更新状态机：故障迁移结束
		old.AdvanceFSM(cs, CMD_FAILOVER_END_SIGNAL)
		return
	}

	// 通过新主广播消息：屏蔽old的读写
	redis.DisableRead(new.Addr(), old.Id())
	redis.DisableWrite(new.Addr(), old.Id())

	c := make(chan error, 1)

	//单独的协程中执行从节点的迁移，主要是slot数据迁移
	//迁移的结果写入到channel中
	go func() {
		//choose failover force or takeover in case of arbiter
		cluster := cs.cluster
		rs := cluster.FindReplicaSetByNode(old.Id())
		//cluster有仲裁者或者集群没有down的情况下，执行从的数据迁移
		//否则会执行强制数据迁移
		//强制和费强制的区别?
		if cluster.HasArbiter() || cluster.IsClusterDown() {
			//use failover takeover
			c <- redis.SetAsMasterWaitSyncDone(new.Addr(), true, true, rs)
		} else {
			//use failover force
			c <- redis.SetAsMasterWaitSyncDone(new.Addr(), true, false, rs)
		}
	}()

	//处理迁移结果：写入到Stream中
	select {
	case err := <-c:
		if err != nil {
			log.Eventf(old.Addr(), "Failover request done with error(%v).", err)
		} else {
			log.Eventf(old.Addr(), "Failover request done, new master %s(%s).", new.Id(), new.Addr())
		}
	case <-time.After(20 * time.Minute):
		log.Eventf(old.Addr(), "Failover timedout, new master %s(%s)", new.Id(), new.Addr())
	}

	// 重新读取一次，因为可能已经更新了
	//先尝试看一下本地存储的状态是否已经更新了，如果没有更新，从Redis中读取一遍，会重试10次
	roleChanged := false
	node := cs.FindNode(newMasterId)
	if node.IsMaster() {
		roleChanged = true
	} else {
		for i := 0; i < 10; i++ {
			info, err := redis.FetchInfo(node.Addr(), "Replication")
			if err == nil && info.Get("role") == "master" {
				roleChanged = true
				break
			}
			log.Warningf(old.Addr(),
				"Role of new master %s(%s) has not yet changed, will check 5 seconds later.",
				new.Id(), new.Addr())
			time.Sleep(5 * time.Second)
		}
	}

	//判断这次迁移是否成功，记录一些相应的日志，没有做实际的处理，需要人工介入处理
	if roleChanged {
		log.Eventf(old.Addr(), "New master %s(%s) role change success", node.Id, node.Addr())
		// 处理迁移过程中的异常问题，将故障节点（旧主）的slots转移到新主上
		oldNode := cs.FindNode(oldMasterId)
		if oldNode != nil && oldNode.Fail && oldNode.IsMaster() && len(oldNode.Ranges) != 0 {
			log.Warningf(old.Addr(),
				"Some node carries slots info(%v) about the old master, waiting for MigrateManager to fix it.",
				oldNode.Ranges)
		} else {
			log.Info(old.Addr(), "Good, no slot need to be fix after failover.")
		}
	} else {
		log.Warningf(old.Addr(), "Failover failed, please check cluster state.")
		log.Warningf(old.Addr(), "The dead master will goto OFFLINE state and then goto WAIT_FAILOVER_BEGIN state to try failover again.")
	}

	//更新状态机，迁移结束
	old.AdvanceFSM(cs, CMD_FAILOVER_END_SIGNAL)

	// 广播打开新主的写入，因为给slave加Write没有效果
	// 所以即便Failover失败，也不会产生错误
	redis.EnableWrite(new.Addr(), new.Id())
}
