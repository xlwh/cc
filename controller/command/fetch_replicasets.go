package command

import (
	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/topo"
)

type FetchReplicaSetsCommand struct{}

type FetchReplicaSetsResult struct {
	ReplicaSets []*topo.ReplicaSet //ReplicaSet
	NodeStates  map[string]string  //节点状态
}

func (self *FetchReplicaSetsCommand) Execute(c *cc.Controller) (cc.Result, error) {
	cs := c.ClusterState
	//读取拓扑
	snapshot := cs.GetClusterSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	//更新一下拓扑，保障拿到的是最新的拓扑
	snapshot.BuildReplicaSets()

	nodeStates := map[string]string{}

	//读取状态
	nss := cs.AllNodeStates()
	for id, n := range nss {
		nodeStates[id] = n.CurrentState()
	}
	//读取所有的ReplicaSets
	result := FetchReplicaSetsResult{
		ReplicaSets: snapshot.ReplicaSets(),
		NodeStates:  nodeStates,
	}
	return result, nil
}
