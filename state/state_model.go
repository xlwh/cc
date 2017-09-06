package state

import (
	"time"

	"github.com/ksarch-saas/cc/fsm"
	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/redis"
)

const (
	StateRunning           = "RUNNING"
	StateWaitFailoverBegin = "WAIT_FAILOVER_BEGIN"
	StateWaitFailoverEnd   = "WAIT_FAILOVER_END"
	StateOffline           = "OFFLINE"
	StateStandby           = "STANDBY"
)

//读取节点当前的状态
func getNodeState(i interface{}) *NodeState {
	ctx := i.(StateContext)
	ns := ctx.NodeState
	return ns
}

var (
	RunningState = &fsm.State{
		Name: StateRunning,
		OnEnter: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Enter RUNNING state")
		},
		OnLeave: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Leave RUNNING state")
		},
	}

	WaitFailoverBeginState = &fsm.State{
		Name: StateWaitFailoverBegin,
		OnEnter: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Enter WAIT_FAILOVER_BEGIN state")
		},
		OnLeave: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Leave WAIT_FAILOVER_BEGIN state")
		},
	}

	//等待迁移结束
	WaitFailoverEndState = &fsm.State{
		Name: StateWaitFailoverEnd,
		OnEnter: func(i interface{}) {
			//记录事件日志
			log.Event(getNodeState(i).Addr(), "Enter WAIT_FAILOVER_END state")

			ctx := i.(StateContext)
			ns := ctx.NodeState

			//更新meta中的记录
			record := &meta.FailoverRecord{
				AppName:   meta.AppName(),
				NodeId:    ns.Id(),
				NodeAddr:  ns.Addr(),
				Timestamp: time.Now(),
				Region:    ns.Region(),
				Tag:       ns.Tag(),
				Role:      ns.Role(),
				Ranges:    ns.Ranges(),
			}
			err := meta.AddFailoverRecord(record)
			if err != nil {
				log.Warningf(ns.Addr(), "state: add failover record failed, %v", err)
			}
		},
		OnLeave: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Leave WAIT_FAILOVER_END state")

			ctx := i.(StateContext)
			ns := ctx.NodeState

			if ns.Role() == "master" {
				err := meta.UnmarkFailoverDoing()
				if err != nil {
					log.Warningf(ns.Addr(), "state: unmark FAILOVER_DOING status failed, %v", err)
				}
			}
		},
	}

	OfflineState = &fsm.State{
		Name: StateOffline,
		OnEnter: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Enter OFFLINE state")
		},
		OnLeave: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Leave OFFLINE state")
		},
	}

	StandbyState = &fsm.State{
		Name: StateStandby,
		OnEnter: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Enter STANDBY state")
		},
		OnLeave: func(i interface{}) {
			log.Event(getNodeState(i).Addr(), "Leave STANDBY state")
		},
	}
)

/// Constraints

var (
	//Slave故障迁移限定条件
	SlaveAutoFailoverConstraint = func(i interface{}) bool {
		ctx := i.(StateContext)
		cs := ctx.ClusterState //集群状态
		ns := ctx.NodeState    //节点状态

		rs := cs.FindReplicaSetByNode(ns.Id())
		if rs == nil {
			return false
		}
		//读取app配置
		app := meta.GetAppConfig()
		if app.SlaveFailoverLimit {
			// 至少还有一个节点
			localRegionNodes := rs.RegionNodes(ns.node.Region)
			if len(localRegionNodes) < 2 {
				return false
			}
			// 最多一个故障节点(FAIL或不处于Running状态)
			//不能同时处理多个节点
			for _, node := range localRegionNodes {
				if node.Id == ns.Id() {
					continue
				}
				nodeState := cs.FindNodeState(node.Id)
				if node.Fail || nodeState.CurrentState() != StateRunning {
					return false
				}
			}
		}
		//满足限定条件，可以进行故障迁移
		log.Info(getNodeState(i).Addr(), "Can failover slave")
		return true
	}

	//Master迁移限定条件
	MasterAutoFailoverConstraint = func(i interface{}) bool {
		ctx := i.(StateContext)
		cs := ctx.ClusterState
		ns := ctx.NodeState

		// 如果AutoFailover没开，且不是执行Failover的信号
		if !meta.AutoFailover() && ctx.Input.Command != CMD_FAILOVER_BEGIN_SIGNAL {
			log.Warning(ns.Addr(), "Check constraint failed, autofailover off or no FL begin signal")
			return false
		}

		//找不到replicaset
		rs := cs.FindReplicaSetByNode(ns.Id())
		if rs == nil {
			log.Warning(ns.Addr(), "Check constraint failed, can not find replicaset by the failure node")
			return false
		}
		// Region至少还有一个节点
		localRegionNodes := rs.RegionNodes(ns.node.Region)
		if len(localRegionNodes) < 2 {
			log.Warningf(ns.Addr(), "Check constraint failed, %s region nodes %d < 2\n",
				ns.node.Region, len(localRegionNodes))
			return false
		}
		// 最多一个故障节点(FAIL或不处于Running状态)
		for _, node := range localRegionNodes {
			if node.Id == ns.Id() {
				continue
			}
			nodeState := cs.FindNodeState(node.Id)
			if node.Fail || (nodeState.CurrentState() != StateRunning && nodeState.CurrentState() != StateStandby) {
				log.Warning(ns.Addr(), "Check constraint failed, more than one failure nodes")
				return false
			}
		}
		// 是否有其他Failover正在进行
		//同时只能有一个任务进行
		doing, err := meta.IsDoingFailover()
		if err != nil {
			log.Warningf(ns.Addr(), "Fetch failover status failed, %v", err)
			return false
		}
		if doing {
			// get doing failover record, if record last for more than 1min, delete doing record
			record, err := meta.DoingFailoverRecord()
			if err == nil {
				if record.Timestamp.Add(1 * time.Millisecond).Before(time.Now()) {
					err = meta.UnmarkFailoverDoing()
					if err != nil {
						log.Warning(ns.Addr(), "UnmarkFailoverDoing failed last for 1 min, %v", err)
					}
					log.Infof(ns.Addr(), "UnmarkFailoverDoing last for 1 min")
				}
			}

			log.Warning(ns.Addr(), "There is another failover doing")
			return false
		}
		// 最近是否进行过Failover
		lastTime, err := meta.LastFailoverTime()
		if err != nil {
			log.Warningf(ns.Addr(), "Get last failover time failed, %v", err)
			return false
		}
		app := meta.GetAppConfig()
		if lastTime != nil && time.Since(*lastTime) < app.AutoFailoverInterval {
			log.Warningf(ns.Addr(), "Failover too soon, lastTime: %v", *lastTime)
			return false
		}

		record := &meta.FailoverRecord{
			AppName:   meta.AppName(),
			NodeId:    ns.Id(),
			NodeAddr:  ns.Addr(),
			Timestamp: time.Now(),
			Region:    ns.Region(),
			Tag:       ns.Tag(),
			Ranges:    ns.Ranges(),
		}
		err = meta.MarkFailoverDoing(record)
		if err != nil {
			log.Warning(ns.Addr(), "Can not mark FAILOVER_DOING status")
			return false
		}
		log.Info(ns.Addr(), "Can do failover for master")
		return true
	}

	MasterGotoOfflineConstraint = func(i interface{}) bool {
		ctx := i.(StateContext)
		ns := ctx.NodeState

		// 已经被处理过了，处于Standby
		if ns.node.IsStandbyMaster() {
			return true
		}
		return false
	}

	//slaveFail的处理的限制条件
	SlaveFailoverHandler = func(i interface{}) {
		ctx := i.(StateContext)
		cs := ctx.ClusterState
		ns := ctx.NodeState

		for _, n := range cs.AllNodeStates() {
			if n.Addr() == ns.Addr() {
				continue
			}
			//没有做一定的限制条件，直接disable
			resp, err := redis.DisableRead(n.Addr(), ns.Id())
			if err == nil {
				log.Infof(ns.Addr(), "Disable read of slave: %s %s", resp, ns.Id())
				break
			}
		}
	}

	//节点重新上线
	StandbyGotoRunningHandler = func(i interface{}) {
		ctx := i.(StateContext)
		ns := ctx.NodeState

		app := meta.GetAppConfig()
		//只要在app开启了配置的情况下，就可以自动Enable
		if app.AutoEnableMasterWrite {
			resp, err := redis.EnableRead(ns.Addr(), ns.Id())
			if err == nil {
				log.Infof(ns.Addr(), "Enable read of new master: %s %s", resp, ns.Id())
			}
		}
	}

	MasterFailoverHandler = func(i interface{}) {
		ctx := i.(StateContext)
		cs := ctx.ClusterState
		ns := ctx.NodeState

		masterRegion := meta.MasterRegion()
		masterId, err := cs.MaxReploffSlibing(ns.Id(), masterRegion, true)
		if err != nil {
			log.Warningf(ns.Addr(), "No slave can be used for failover %s", ns.Id())
			// 放到另一个线程做，避免死锁
			go ns.AdvanceFSM(cs, CMD_FAILOVER_END_SIGNAL)
		} else {
			go cs.RunFailoverTask(ns.Id(), masterId)
		}
	}

	MasterGotoOfflineHandler = func(i interface{}) {
		ctx := i.(StateContext)
		cs := ctx.ClusterState
		ns := ctx.NodeState

		for _, n := range cs.AllNodeStates() {
			resp, err := redis.DisableRead(n.Addr(), ns.Id())
			if err == nil {
				log.Infof(ns.Addr(), "Disable read of the already dead master: %s %s", resp, ns.Id())
			}
			resp, err = redis.DisableWrite(n.Addr(), ns.Id())
			if err == nil {
				log.Infof(ns.Addr(), "Disable read of the already dead master: %s %s", resp, ns.Id())
				break
			}
		}
	}
)

var (
	RedisNodeStateModel = fsm.NewStateModel()
)

func init() {
	//设置状态，有五种状态
	RedisNodeStateModel.AddState(RunningState)           //运行中
	RedisNodeStateModel.AddState(WaitFailoverBeginState) //开始进行故障迁移
	RedisNodeStateModel.AddState(WaitFailoverEndState)   //故障迁移结束
	RedisNodeStateModel.AddState(OfflineState)           //离线
	RedisNodeStateModel.AddState(StandbyState)           //备用状态

	/// State: (WaitFailoverRunning)

	// (a0) Running封禁了，进入Offline状态
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateRunning,
		To:         StateStandby,
		Input:      Input{F, F, ANY, ANY, ANY}, //Input{read, write, FAIL/NFAIL, role, cmd}
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	// (a1) 节点挂了，且未封禁
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateRunning,
		To:         StateWaitFailoverBegin,
		Input:      Input{T, ANY, FAIL, ANY, ANY},
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	// (a2) 节点挂了，且未封禁
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateRunning,
		To:         StateWaitFailoverBegin,
		Input:      Input{ANY, T, FAIL, ANY, ANY},
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	// (a3) 节点挂了，从，且未封禁，且可以自动进行Failover
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateRunning,
		To:         StateWaitFailoverEnd,
		Input:      Input{T, ANY, FAIL, S, ANY},
		Priority:   1,
		Constraint: SlaveAutoFailoverConstraint,
		Apply:      SlaveFailoverHandler,
	})

	// (a4) 节点挂了，主，未封禁，且可以自动进行Failover
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateRunning,
		To:         StateWaitFailoverEnd,
		Input:      Input{T, T, FAIL, M, ANY},
		Priority:   1,
		Constraint: MasterAutoFailoverConstraint,
		Apply:      MasterFailoverHandler,
	})

	/// State: (WaitFailoverBegin)

	// (b0) 节点恢复了
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateWaitFailoverBegin,
		To:         StateRunning,
		Input:      Input{ANY, ANY, FINE, ANY, ANY},
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	// (b10) 主节点，Autofailover或手动继续执行Failover
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateWaitFailoverBegin,
		To:         StateWaitFailoverEnd,
		Input:      Input{ANY, ANY, FAIL, M, ANY},
		Priority:   0,
		Constraint: MasterAutoFailoverConstraint,
		Apply:      MasterFailoverHandler,
	})

	// (b11) 主节点，已经处理过了
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateWaitFailoverBegin,
		To:         StateOffline,
		Input:      Input{ANY, ANY, FAIL, M, ANY},
		Priority:   0,
		Constraint: MasterGotoOfflineConstraint,
		Apply:      MasterGotoOfflineHandler,
	})

	// (b2) 从节点，AutoFailover或手动继续执行Failover
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateWaitFailoverBegin,
		To:         StateWaitFailoverEnd,
		Input:      Input{ANY, ANY, FAIL, S, ANY},
		Priority:   0,
		Constraint: SlaveAutoFailoverConstraint,
		Apply:      SlaveFailoverHandler,
	})

	// (b3) 从节点，已经处于封禁状态，转到OFFLINE
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateWaitFailoverBegin,
		To:         StateOffline,
		Input:      Input{F, F, FAIL, S, ANY},
		Priority:   1,
		Constraint: nil,
		Apply:      nil,
	})

	/// State: (WaitFailoverEnd)

	// (c0) 等待Failover执行结束信号
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateWaitFailoverEnd,
		To:         StateOffline,
		Input:      Input{ANY, ANY, ANY, ANY, CMD_FAILOVER_END_SIGNAL},
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	// (c1) 从挂了，且已经封禁
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateWaitFailoverEnd,
		To:         StateOffline,
		Input:      Input{F, F, FAIL, S, ANY},
		Priority:   1,
		Constraint: nil,
		Apply:      nil,
	})

	// (c2) 从在Failover过程中恢复了，结束FailoverEnd状态
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateWaitFailoverEnd,
		To:         StateOffline,
		Input:      Input{ANY, ANY, FINE, S, ANY},
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	/// State: (Offline)

	// (d0) 节点存活状态恢复正常
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateOffline,
		To:         StateStandby,
		Input:      Input{ANY, ANY, FINE, ANY, ANY},
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	// (d1) 是主节，且挂了，需要进行Failover
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:     StateOffline,
		To:       StateWaitFailoverBegin,
		Input:    Input{F, F, FAIL, M, ANY},
		Priority: 0,
		Constraint: func(i interface{}) bool {
			// Master故障，进行Failover之后，故障的节点仍然被标记为master。
			// 所以我们需要判断这个Master是否已经被处理过了。
			// 判断依据是节点处于FAIL状态，且没有slots
			ctx := i.(StateContext)
			ns := ctx.NodeState

			if ns.node.IsStandbyMaster() {
				return false
			}
			log.Warningf(ns.Addr(), "Found offline non standby master, will try to failover(%v,%v).",
				ns.Role(), ns.Ranges())
			return true
		},
		Apply: nil,
	})

	/// State: (Standby)

	// Standby节点挂掉进入Offline
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateStandby,
		To:         StateOffline,
		Input:      Input{F, F, FAIL, S, ANY},
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	// Standby节点进入Running
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateStandby,
		To:         StateRunning,
		Input:      Input{T, ANY, FINE, ANY, ANY},
		Priority:   0,
		Constraint: nil,
		Apply:      nil,
	})

	// Standby节点进入Running
	RedisNodeStateModel.AddTransition(&fsm.Transition{
		From:       StateStandby,
		To:         StateRunning,
		Input:      Input{ANY, T, FINE, ANY, ANY},
		Priority:   0,
		Constraint: nil,
		Apply:      StandbyGotoRunningHandler,
	})
}

type StateContext struct {
	Input        Input         //输入
	ClusterState *ClusterState //集群状态
	NodeState    *NodeState    //节点状态
}
