package command

import (
	"fmt"

	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/redis"
)

type MakeReplicaSetCommand struct {
	NodeIds []string
}

// 将一组新的节点（Master,NotFail,NoSlots）设置成一个ReplicaSet
// 需要保证节点覆盖所有Region
func (self *MakeReplicaSetCommand) Execute(c *cc.Controller) (cc.Result, error) {
	cs := c.ClusterState

	masterNodeId := ""
	//服务的master所在的地域
	masterRegion := meta.MasterRegion()
	//所有的地域
	regions := meta.AllRegions()

	regionExist := map[string]bool{}
	for _, r := range regions {
		regionExist[r] = false
	}

	// 检查节点状态
	for _, nodeId := range self.NodeIds {
		//搜索node
		node := cs.FindNode(nodeId)
		if node == nil {
			return nil, ErrNodeNotExist
		}
		//节点挂的情况，打印一个日志
		if node.Fail {
			return nil, fmt.Errorf("Node %s has failed", nodeId)
		}
		//确定节点是主，设置主操作
		if !node.IsMaster() {
			return nil, fmt.Errorf("Node %s is not master, newly added node must be master.", nodeId)
		}
		if len(node.Ranges) != 0 {
			return nil, fmt.Errorf("Node %s is not empty, newly added node must be empty.", nodeId)
		}
		regionExist[node.Region] = true

		if node.Region == masterRegion {
			masterNodeId = nodeId
		}
	}

	// 检查地域覆盖
	for region, exist := range regionExist {
		if exist == false {
			return nil, fmt.Errorf("Lack node in region %s", region)
		}
	}

	// 设置主从关系
	for _, nodeId := range self.NodeIds {
		if nodeId == masterNodeId {
			continue
		}
		//设置从
		node := cs.FindNode(nodeId)
		_, err := redis.ClusterReplicate(node.Addr(), masterNodeId)
		if err != nil {
			return nil, fmt.Errorf("Set REPLICATE failed(%v), please set relationship manually.", err)
		}
	}
	return nil, nil
}
