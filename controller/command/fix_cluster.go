package command

import (
	"time"

	"errors"

	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/state"
	"github.com/ksarch-saas/cc/topo"
)

type FixClusterCommand struct {
}

type FixClusterResult struct {
	Result bool
}

func (self *FixClusterCommand) Type() cc.CommandType {
	return cc.CLUSTER_COMMAND
}

func (self *FixClusterCommand) Execute(c *cc.Controller) (cc.Result, error) {
	//这个方法最常用啦
	cs := c.ClusterState
	snapshot := cs.GetClusterSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	snapshot.BuildReplicaSets()

	nodeStates := map[string]string{}
	nss := cs.AllNodeStates()
	for id, n := range nss {
		nodeStates[id] = n.CurrentState()
	}
	rss := snapshot.ReplicaSets()

	totalNum := 0 //总节点数
	totalRepli := 0
	failedNodes := []*topo.Node{}  //挂掉的节点
	freeNodes := []*topo.Node{}    //空闲的节点
	defectMaster := []*topo.Node{} //存在问题的Master

	//遍历ReplicaSets，找出Fail节点和Free节点
	for _, rs := range rss {
		//check failed nodes and free nodes
		if rs.Master != nil && rs.Master.IsArbiter() {
			continue
		}
		//更新totalNum
		totalNum = totalNum + len(rs.AllNodes())
		//range== 0 && state.StateRunning 这样的节点会被认为是Free节点
		if len(rs.Master.Ranges) == 0 && nodeStates[rs.Master.Id] == state.StateRunning {
			//free节点
			freeNodes = append(freeNodes, rs.Master)
		} else {
			if len(rs.AllNodes()) > 1 {
				totalRepli = totalRepli + 1
			}
			for _, node := range rs.AllNodes() {
				//如果节点不在运行当中，则认为是Fail节点
				if nodeStates[node.Id] != state.StateRunning {
					failedNodes = append(failedNodes, node)
				}
			}
		}
	}

	log.Infof("CLUSTER", "freeNodes=%d failedNodes=%d", len(freeNodes), len(failedNodes))
	//没有找到空闲的和Fail的节点
	if len(freeNodes) == 0 && len(failedNodes) == 0 {
		return nil, nil
	}

	//如果Free节点的量和Fail节点不相等，则不执行Fix
	if len(freeNodes) != len(failedNodes) ||
		(totalNum-len(failedNodes))%(totalRepli) != 0 {
		log.Infof("CLUSTER", "totalNum=%d totalRepli=%d freeNodes=%d failedNodes=%d",
			totalNum-len(failedNodes), totalRepli, len(freeNodes), len(failedNodes))
		return nil, errors.New("cluster fix check error, please check")
	}
	avgReplica := int((totalNum - len(failedNodes)) / totalRepli)

	replicaBroken := func(rs *topo.ReplicaSet) bool {
		for _, n := range rs.AllNodes() {
			if nodeStates[n.Id] != state.StateRunning {
				return true
			}
		}
		return false
	}
	//遍历rss
	for _, rs := range rss {
		if rs.Master != nil && rs.Master.IsArbiter() ||
			nodeStates[rs.Master.Id] != state.StateRunning {
			continue
		}
		if len(rs.AllNodes()) < avgReplica && len(rs.Master.Ranges) > 0 &&
			nodeStates[rs.Master.Id] == state.StateRunning {
			defectMaster = append(defectMaster, rs.Master)
		}
		if len(rs.AllNodes()) == avgReplica && replicaBroken(rs) == true {
			defectMaster = append(defectMaster, rs.Master)
		}
	}
	// forget offline nodes
	for _, node := range failedNodes {
		forgetCmd := ForgetAndResetNodeCommand{
			NodeId: node.Id,
		}
		forgetCmd.Execute(c)
		log.Eventf(node.Addr(), "Forget and reset failed node")
	}

	//meet & replicate
	for _, node := range freeNodes {
		meetCmd := MeetNodeCommand{
			NodeId: node.Id,
		}
		meetCmd.Execute(c)
		log.Eventf(node.Addr(), "Meet cluster")
		// give some time to gossip
		time.Sleep(5 * time.Second)
	}

	for idx, node := range freeNodes {
		//disable read
		disableReadCmd := DisableReadCommand{
			NodeId: node.Id,
		}
		disableReadCmd.Execute(c)
		log.Eventf(node.Addr(), "Disable read flag")

		//replicate
		replicateCmd := ReplicateCommand{
			ChildId:  node.Id,
			ParentId: defectMaster[idx].Id,
		}
		replicateCmd.Execute(c)
		log.Eventf(node.Addr(), "Replicate %s to %s", node.Addr(), defectMaster[idx].Addr())
	}

	result := FixClusterResult{Result: true}
	return result, nil
}
