package command

import (
	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/redis"
	"github.com/ksarch-saas/cc/state"
	"github.com/ksarch-saas/cc/topo"
)

type UpdateRegionCommand struct {
	Region string
	Nodes  []*topo.Node
}

//【==========================核心方法=======================】
//更新状态机，广播摘除故障实例
//只是屏蔽读写，没有进行迁移
func (self *UpdateRegionCommand) Execute(c *cc.Controller) (cc.Result, error) {
	if len(self.Nodes) == 0 {
		return nil, nil
	}

	// 更新Cluster拓扑
	cs := c.ClusterState

	//更新本地域所有节点的状态
	cs.UpdateRegionNodes(self.Region, self.Nodes)

	// 首先更新迁移任务状态，以便发现故障时，在处理故障之前就暂停迁移任务
	//主要的任务就是建立起数据迁移任务
	//Step1 进行数据迁移
	cluster := cs.GetClusterSnapshot()
	if cluster != nil {
		mm := c.MigrateManager
		mm.HandleNodeStateChange(cluster)
	}

	//Step2 屏蔽读或者写
	for _, ns := range cs.AllNodeStates() {
		node := ns.Node()
		// Slave auto enable read ?
		if !node.IsMaster() && !node.Fail && !node.Readable && node.MasterLinkStatus == "up" {
			if meta.GetAppConfig().AutoEnableSlaveRead {
				redis.EnableRead(node.Addr(), node.Id)
			}
		}
		// Master auto enable write ?
		if node.IsMaster() && !node.Fail && !node.Writable {
			if meta.GetAppConfig().AutoEnableMasterWrite {
				redis.EnableWrite(node.Addr(), node.Id)
			}
		}
		// Fix chained replication: slave's parent is slave.
		//在本地域，并且节点不是主
		//把grandpa设置为这个节点的从
		if meta.LocalRegion() == self.Region && !node.IsMaster() {
			parent := cs.FindNode(node.ParentId)
			// Parent is not master?
			if parent != nil && !parent.IsMaster() {
				grandpa := cs.FindNode(parent.ParentId)
				if grandpa != nil {
					_, err := redis.ClusterReplicate(node.Addr(), grandpa.Id)
					if err == nil {
						log.Warningf(node.Addr(), "Fix chained replication, (%s->%s->%s)=>(%s->%s)",
							node, parent, grandpa, node, grandpa)
					}
				} else {
					log.Warningf(node.Addr(), "Found chained replication, (%s->%s->nil), cannot fix.",
						node, parent)
				}
			}
		}

		// 更新Region内Node的状态机
		ns.AdvanceFSM(cs, state.CMD_NONE)
	}

	return nil, nil
}
