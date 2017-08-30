package command

import (
	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/redis"
)

type DisableWriteCommand struct {
	NodeId string
}

//和屏蔽读的逻辑一致
func (self *DisableWriteCommand) Execute(c *cc.Controller) (cc.Result, error) {
	cs := c.ClusterState
	target := cs.FindNode(self.NodeId)
	if target == nil {
		return nil, ErrNodeNotExist
	}
	if target.Fail {
		return nil, ErrNodeIsDead
	}
	var err error
	ns := cs.GetFirstNodeState()
	_, err = redis.DisableWrite(ns.Addr(), target.Id)
	if err == nil {
		return nil, nil
	}
	for _, ns = range cs.AllNodeStates() {
		_, err = redis.DisableWrite(ns.Addr(), target.Id)
		if err == nil {
			return nil, nil
		}
	}
	return nil, err
}
