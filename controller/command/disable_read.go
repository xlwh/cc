package command

import (
	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/redis"
)

type DisableReadCommand struct {
	NodeId string
}

//执行屏蔽读命令
func (self *DisableReadCommand) Execute(c *cc.Controller) (cc.Result, error) {
	//读取集群状态
	cs := c.ClusterState
	//根据id读取要执行ddisable的节点对象
	target := cs.FindNode(self.NodeId)
	if target == nil {
		return nil, ErrNodeNotExist
	}
	//如果节点处于已经挂了的状态，则不进行操作了
	if target.Fail {
		return nil, ErrNodeIsDead
	}

	//从集群上获取第一个节点，读取状态
	var err error
	ns := cs.GetFirstNodeState()
	//向第一个节点发送屏蔽读命令
	_, err = redis.DisableRead(ns.Addr(), target.Id)
	if err == nil {
		return nil, nil
	}
	//向所有的节点发送node
	for _, ns = range cs.AllNodeStates() {
		_, err = redis.DisableRead(ns.Addr(), target.Id)
		if err == nil {
			return nil, nil
		}
	}
	return nil, err
}
