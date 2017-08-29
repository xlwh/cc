package topo

import (
	"errors"
	"strings"

	"github.com/golang/glog"
)

//全局变量:出错信息
var (
	ErrInvalidParentId = errors.New("topo: invalid parent id, master not exist")
)

//Fail节点数组
type FailureInfo struct {
	Seeds []*Node
}

//集群信息描述
type ClusterInfo struct {
	ClusterState                 string //集群状态
	ClusterSlotsAssigned         int    //集群的已经分配的slot数目
	ClusterSlotsOk               int    //正常的slot数目
	ClusterSlotsPfail            int    //PFail状态的数目
	ClusterSlotsFail             int    //Fail状态的Slot数目
	ClusterKnownNodes            int    //集群中已知的node数目
	ClusterSize                  int    //集群大小??
	ClusterCurrentEpoch          int    //集群当前Epoch?
	ClusterMyEpoch               int
	ClusterStatsMessagesSent     int //集群发送的消息数量
	ClusterStatsMessagesReceived int //集群接收到的消息的数量
}

//Redis集群
type Cluster struct {
	localRegion      string           //集群地域
	localRegionNodes []*Node          //集群当前地域的Node列表
	nodes            []*Node          //所有的node
	replicaSets      []*ReplicaSet    //集群的从node集合
	idTable          map[string]*Node //集群的idTable
}

//指定region，创建clsuter实例
//除了range指定以外，其他都是没有指定的
func NewCluster(region string) *Cluster {
	c := &Cluster{}
	c.localRegion = region
	c.localRegionNodes = []*Node{}
	c.nodes = []*Node{}
	c.replicaSets = []*ReplicaSet{}
	c.idTable = map[string]*Node{}
	return c
}

//在集群中增加node
func (self *Cluster) AddNode(s *Node) {
	//在idTable中添加node的id
	self.idTable[s.Id] = s
	//在nodes中增加node
	self.nodes = append(self.nodes, s)

	//如果node是本地域的，还需要在localRegion的node列表中增加node
	if s.Region == self.localRegion {
		self.localRegionNodes = append(self.localRegionNodes, s)
	}
}

//返回cluster中的所有的Node
func (self *Cluster) AllNodes() []*Node {
	return self.nodes
}

//查询当前cluster中有多少个node
func (self *Cluster) NumNode() int {
	return len(self.nodes)
}

//查询本地有多少个node
func (self *Cluster) LocalRegionNodes() []*Node {
	return self.localRegionNodes
}

//查询Master节点的数量
func (self *Cluster) MasterNodes() []*Node {
	nodes := []*Node{}
	for _, node := range self.AllNodes() {
		if node.IsMaster() {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

//返回cluster的size = MasterNodesNum
func (self *Cluster) Size() int {
	return len(self.MasterNodes())
}

//返回当前地域的所有的Node
func (self *Cluster) RegionNodes(region string) []*Node {
	nodes := []*Node{}
	for _, n := range self.nodes {
		if n.Region == region {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

//本地的所有的node数目
func (self *Cluster) NumLocalRegionNode() int {
	return len(self.localRegionNodes)
}

//根据nodeId，查找node
func (self *Cluster) FindNode(id string) *Node {
	return self.idTable[id]
}

//根据slot,查找所在的Node
func (self *Cluster) FindNodeBySlot(slot int) *Node {

	//遍历所有的node
	for _, node := range self.AllNodes() {
		if node.IsMaster() {
			for _, r := range node.Ranges {
				//判断slot是否在range内
				if slot >= r.Left && slot <= r.Right {
					return node
				}
			}
		}
	}
	return nil
}

//根据id查找从节点
func (self *Cluster) FindReplicaSetByNode(id string) *ReplicaSet {

	//遍历replicaSet
	for _, rs := range self.replicaSets {
		if rs.HasNode(id) {
			return rs
		}
	}
	return nil
}

//集群的地域信息
func (self *Cluster) Region() string {
	return self.localRegion
}

//查找Fail信息
func (self *Cluster) FailureNodes() []*Node {
	ss := []*Node{}
	for _, s := range self.localRegionNodes {
		if s.Fail {
			ss = append(ss, s)
		}
	}
	return ss
}

//构建从节点
func (self *Cluster) BuildReplicaSets() error {
	replicaSets := []*ReplicaSet{}

	//处理Master
	for _, s := range self.nodes {
		if s.IsMaster() {
			rs := NewReplicaSet()
			rs.SetMaster(s)
			replicaSets = append(replicaSets, rs)
		}
	}

	for _, s := range self.nodes {
		if !s.IsMaster() {
			//根据parent，查找node
			master := self.FindNode(s.ParentId)
			if master == nil {
				glog.Info("CLUSTER ", "ParentId:", s.ParentId, " Myself is ", s.Addr())
				return ErrInvalidParentId
			}

			for _, rs := range replicaSets {
				if rs.Master == master {
					//把节点设置为Master的Slave
					rs.AddSlave(s)
				}
			}
		}
	}

	//返回构建好的主从关系拓扑列表
	self.replicaSets = replicaSets
	return nil
}

type ByMasterId []*ReplicaSet
type ByNodeState []*ReplicaSet
type ByNodeSlot []*Node

// sort by master id
func (a ByMasterId) Len() int           { return len(a) }
func (a ByMasterId) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByMasterId) Less(i, j int) bool { return a[i].Master.Id < a[j].Master.Id }

// sort by master slot left range
func (a ByNodeSlot) Len() int      { return len(a) }
func (a ByNodeSlot) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByNodeSlot) Less(i, j int) bool {
	return a[i].Ranges[0].Left < a[j].Ranges[0].Left
}

// sort replicas by node state
func (a ByNodeState) Len() int      { return len(a) }
func (a ByNodeState) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByNodeState) Less(i, j int) bool {
	//inner function to check a relicas is normal
	inner := func(idx int) bool {
		if a[idx].Master.PFail || a[idx].Master.Fail || a[idx].Master.Free ||
			(!a[idx].Master.Readable && !a[idx].Master.Writable) {
			return true
		}
		for _, node := range a[idx].Slaves {
			if node.PFail || node.Fail || node.Free ||
				(!node.Readable && !node.Writable) {
				return true
			}
		}
		return false
	}
	if len(a[i].Slaves) < len(a[j].Slaves) {
		return false
	}
	if inner(i) && !inner(j) {
		return false
	}
	return true
}

func (self *Cluster) ReplicaSets() []*ReplicaSet {
	return self.replicaSets
}

func (self *Cluster) NumReplicaSets() int {
	return len(self.replicaSets)
}

func (self *Cluster) String() string {
	return ""
}

func (self *Cluster) HasArbiter() bool {
	//check cluster arbiter nodes
	for _, n := range self.nodes {
		if n.Role == "master" && strings.Contains(n.Tag, "Arbiter") {
			return true
		}
	}
	return false
}

// when cluster down, use failover takeover command
func (self *Cluster) IsClusterDown() bool {
	numFailed := 0
	for _, node := range self.MasterNodes() {
		if node.Fail {
			numFailed++
		}
	}
	if numFailed >= (self.Size()+1)/2 {
		return true
	}
	return false
}
