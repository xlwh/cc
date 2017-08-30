package command

import (
	"fmt"

	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/redis"
)

type FailoverTakeoverCommand struct {
	NodeId string
}

//把节点恢复正常
func (self *FailoverTakeoverCommand) Execute(c *cc.Controller) (cc.Result, error) {
	cs := c.ClusterState
	node := cs.FindNode(self.NodeId)
	if node == nil {
		return nil, ErrNodeNotExist
	}
	//如果这个节点已经是主了，则不进行处理
	if node.IsMaster() {
		return nil, ErrNodeIsMaster
	}
	mm := c.MigrateManager

	//如果在这个时候还存在迁移中的任务，则不执行故障恢复操作
	if len(mm.AllTasks()) > 0 {
		return nil, fmt.Errorf("Migrate task exists, cancel task to continue.")
	}
	rs := cs.FindReplicaSetByNode(self.NodeId)
	//广播故障恢复命令
	_, err := redis.ClusterTakeover(node.Addr(), rs)
	if err != nil {
		return nil, err
	}
	//广播允许写入
	ns := cs.GetFirstNodeState()
	_, err = redis.EnableWrite(ns.Addr(), self.NodeId)
	if err == nil {
		return nil, nil
	}
	//判断是否执行成功
	for _, ns = range cs.AllNodeStates() {
		_, err = redis.EnableWrite(ns.Addr(), self.NodeId)
		if err == nil {
			return nil, nil
		}
	}
	return nil, err
}
