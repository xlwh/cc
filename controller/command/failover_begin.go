package command

import (
	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/state"
)

type FailoverBeginCommand struct {
	NodeId string
}

func (self *FailoverBeginCommand) Execute(c *cc.Controller) (cc.Result, error) {
	cs := c.ClusterState
	//读取节点现在的状态
	node := cs.FindNodeState(self.NodeId)
	if node == nil {
		return nil, ErrNodeNotExist
	}

	//更新状态机：节点的状态进入故障迁移
	err := node.AdvanceFSM(cs, state.CMD_FAILOVER_BEGIN_SIGNAL)

	return nil, err
}
