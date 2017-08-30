package command

import cc "github.com/ksarch-saas/cc/controller"

type AppStatusCommand struct{}

//app状态的描述结构
type AppStatusResult struct {
	Partitions   int //分区
	Replicas     int //副本数？
	FailedNodes  int //挂掉的节点数
	ReplicaEqual bool
	NodeMax      int64
	NodeMin      int64
	NodeAvg      int64
}

//执行命令
//从拓扑上读取app的信息，包括内存，node数等信息
func (self *AppStatusCommand) Execute(c *cc.Controller) (cc.Result, error) {
	//集群状态
	cs := c.ClusterState
	//读取当前集群的拓扑
	snapshot := cs.GetClusterSnapshot()
	if snapshot == nil {
		return nil, nil
	}

	result := &AppStatusResult{
		Partitions:   0,
		Replicas:     0,
		FailedNodes:  0,
		ReplicaEqual: true,
		NodeMax:      0,
		NodeMin:      1024 * 1024 * 1024 * 1024,
		NodeAvg:      0,
	}

	//更新一遍主从信息
	snapshot.BuildReplicaSets()
	nodeState := map[string]string{}
	nss := cs.AllNodeStates()

	//读取现在node上的状态
	for id, ns := range nss {
		nodeState[id] = ns.CurrentState()
	}

	//重新读取主从关系
	rss := snapshot.ReplicaSets()
	first := true
	var totalMem int64 //内存大小

	//遍历ReplicaSets
	for _, rs := range rss {
		if rs.Master != nil && rs.Master.IsArbiter() {
			continue
		}
		if rs.Master != nil {
			//获取内存使用量
			usedMem := rs.Master.UsedMemory

			//设置内存使用大小占用量
			if result.NodeMax < usedMem {
				result.NodeMax = usedMem
			}
			if result.NodeMin > usedMem {
				result.NodeMin = usedMem
			}
			//设置总的内存量
			totalMem += usedMem
		}
		if first {
			first = false
			//分区数=len(ReplicaSets)
			result.Partitions = len(rss)
			//节点数
			result.Replicas = len(rs.AllNodes())
		} else {
			if result.Replicas != len(rs.AllNodes()) {
				result.ReplicaEqual = false
			}
		}
	}
	if len(rss) > 0 {
		//平均内存占用
		result.NodeAvg = totalMem / int64(len(rss))
	}

	for _, ns := range nodeState {
		if ns != "RUNNING" {
			result.FailedNodes++
		}
	}

	return result, nil
}
