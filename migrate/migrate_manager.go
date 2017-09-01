package migrate

import (
	"errors"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/redis"
	"github.com/ksarch-saas/cc/streams"
	"github.com/ksarch-saas/cc/topo"
)

var (
	ErrMigrateAlreadyExist = errors.New("mig: task is running on the node")
	ErrMigrateNotExist     = errors.New("mig: no task running on the node")
	ErrReplicatSetNotFound = errors.New("mig: replica set not found")
	ErrNodeNotFound        = errors.New("mig: node not found")
	ErrSourceNodeFail      = errors.New("mig: source node failure")
	ErrTargetNodeFail      = errors.New("mig: target node failure")
	ErrCanNotRecover       = errors.New("mig: can not recover")
	ErrRebalanceTaskExist  = errors.New("mig: rebalancing task exist")
)

/// Migrate

type MigrateManager struct {
	tasks           []*MigrateTask //任务列表
	rebalanceTask   *RebalanceTask //
	lastTaskEndTime time.Time
	mutex           *sync.Mutex
}

func NewMigrateManager() *MigrateManager {
	m := &MigrateManager{tasks: []*MigrateTask{}, mutex: &sync.Mutex{}}
	go m.CheckAndRunTask()
	return m
}

func (m *MigrateManager) LockQ() {
	m.mutex.Lock()
}

func (m *MigrateManager) UnLockQ() {
	m.mutex.Unlock()
}

//创建数据迁移task
func (m *MigrateManager) CreateTask(sourceId, targetId string, ranges []topo.Range, cluster *topo.Cluster) (*MigrateTask, error) {
	sourceRS := cluster.FindReplicaSetByNode(sourceId)
	targetRS := cluster.FindReplicaSetByNode(targetId)
	if sourceRS == nil || targetRS == nil {
		return nil, ErrReplicatSetNotFound
	}

	//创建task增加到task队列中去
	task := NewMigrateTask(cluster, sourceRS, targetRS, ranges)
	err := m.AddTask(task)
	if err != nil {
		return nil, err
	}
	return task, nil
}

func (m *MigrateManager) AddTask(task *MigrateTask) error {
	if len(task.ranges) != 1 || task.ranges[0].Left+1 != task.ranges[0].Right {
		//add task to zk
		taskMeta := task.ToMeta()
		err := meta.AddMigrateTask(taskMeta)
		if err != nil {
			return err
		}
	}
	m.LockQ()
	defer m.UnLockQ()
	m.tasks = append(m.tasks, task)

	return nil
}

func (m *MigrateManager) AppendTask(task *MigrateTask) error {
	m.LockQ()
	defer m.UnLockQ()
	m.tasks = append(m.tasks, task)
	return nil
}

//检查和运行task
func (m *MigrateManager) CheckAndRunTask() {
	tickCh := time.NewTicker(time.Second * 5).C
	for {
		select {
		case <-tickCh:
			glog.Info("Check new migrate task, task queue length: ", len(m.tasks))
			app := meta.GetAppConfig()
			for idx, task := range m.tasks {
				glog.Infof("Task (%s) status:%s ", task.TaskName(), stateNames[task.CurrentState()])
				if idx >= app.MigrateConcurrency {
					break
				}
				if task.CurrentState() == StateNew {
					glog.Info("Set task running ", task)

					// update task state in case of next loop state check
					//更新状态机
					task.SetState(StateRunning)
					//运行迁移任务
					go task.Run()
				} else if task.CurrentState() == StateDone || task.CurrentState() == StateCancelled {
					glog.Info("Remove task when task is done or be cancelled", task)
					m.RemoveTask(task, true)
				}

			}
		}
	}
}

func (m *MigrateManager) RemoveTask(task *MigrateTask, zkCascade bool) error {
	pos := -1
	m.LockQ()
	defer m.UnLockQ()
	for i, t := range m.tasks {
		if t == task {
			pos = i
		}
	}
	if pos != -1 {
		m.lastTaskEndTime = time.Now()
		m.tasks = append(m.tasks[:pos], m.tasks[pos+1:]...)
		if zkCascade {
			err := meta.RemoveMigrateTask(task.ToMeta().TaskId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *MigrateManager) FindTasksByNode(nodeId string) []*MigrateTask {
	ts := m.FindTasksByTarget(nodeId)
	t := m.FindTaskBySource(nodeId)
	if t != nil {
		ts = append(ts, t)
	}
	return ts
}

func (m *MigrateManager) AllTasks() []*MigrateTask {
	return m.tasks
}

func (m *MigrateManager) FindTasksByTarget(nodeId string) []*MigrateTask {
	ts := []*MigrateTask{}

	for _, t := range m.tasks {
		if t.TargetNode().Id == nodeId {
			ts = append(ts, t)
		}
	}
	return ts
}

func (m *MigrateManager) FindTaskBySource(nodeId string) *MigrateTask {
	for _, t := range m.tasks {
		if t.SourceNode().Id == nodeId {
			return t
		}
	}
	return nil
}

// 更新任务状态机
func (m *MigrateManager) handleTaskChange(task *MigrateTask, cluster *topo.Cluster) error {
	fromNode := cluster.FindNode(task.SourceNode().Id)
	toNode := cluster.FindNode(task.TargetNode().Id)
	tname := task.TaskName()

	if fromNode == nil {
		log.Infof(tname, "Source node %s(%s) not exist", fromNode.Addr(), fromNode.Id)
		return ErrNodeNotFound
	}
	if toNode == nil {
		log.Infof(tname, "Target node %s(%s) not exist", toNode.Addr(), toNode.Id)
		return ErrNodeNotFound
	}

	// 角色变化说明该分片进行了主从切换
	if !fromNode.IsMaster() || !toNode.IsMaster() {
		log.Warningf(tname, "%s role change, cancel migration task %s\n", fromNode.Id[:6], task.TaskName())
		task.SetState(StateCancelling)
		return ErrSourceNodeFail
	}

	// 如果是源节点挂了，直接取消，等待主从切换之后重建任务
	if fromNode.Fail {
		log.Infof(tname, "Cancel migration task %s\n", task.TaskName())
		task.SetState(StateCancelling)
		return ErrSourceNodeFail
	}
	// 如果目标节点挂了，需要记录当前的ReplicaSet，观察等待主从切换
	if toNode.Fail {
		if task.CurrentState() == StateRunning {
			task.SetState(StateTargetNodeFailure)
			task.SetBackupReplicaSet(task.TargetReplicaSet())
			return ErrTargetNodeFail
		}
	} else if task.CurrentState() != StateNew {
		task.SetState(StateRunning)
		task.SetBackupReplicaSet(nil)
	}
	// 如果目标节点已经进行了Failover(重新选主)，我们需要找到对应的新主
	// 方法是从BackupReplicaSet里取一个从来查找
	if toNode.IsStandbyMaster() {
		brs := task.BackupReplicaSet()
		if brs == nil {
			task.SetState(StateCancelling)
			log.Info(tname, "No backup replicaset found, controller maybe restarted after target master failure, can not do recovery.")
			return ErrCanNotRecover
		}
		slaves := brs.Slaves
		if len(slaves) == 0 {
			task.SetState(StateCancelling)
			log.Info(tname, "The dead target master has no slave, cannot do recovery.")
			return ErrCanNotRecover
		} else {
			rs := cluster.FindReplicaSetByNode(slaves[0].Id)
			if rs == nil {
				task.SetState(StateCancelling)
				log.Info(tname, "No replicaset for slave of dead target master found")
				return ErrCanNotRecover
			}
			task.ReplaceTargetReplicaSet(rs)
			log.Infof(tname, "Recover dead target node to %s(%s)",
				rs.Master.Id, rs.Master.Addr())
		}
	}
	return nil
}

//处理节点状态改变，进行故障迁移等事情
//只允许一个任务在运行
//遍历处理node中的Migrating和Importing
//这里要关注它选择的哪个节点去进行数据迁移，策略是什么
func (m *MigrateManager) HandleNodeStateChange(cluster *topo.Cluster) {
	// 如果存在迁移任务，先跳过，等结束后再处理
	if len(m.tasks) > 0 {
		goto done
	}

	// 遍历新的拓扑处理主节点的迁移任务重建
	for _, node := range cluster.AllNodes() {
		//Fail的节点不处理
		//这里可以记录一条日志?
		if node.Fail {
			continue
		}

		// Wait a while
		//距离上一次迁移任务少于5ms暂时不处理
		if time.Now().Sub(m.lastTaskEndTime) < 5*time.Second {
			continue
		}

		//处理数据迁移出:遍历取出待迁移的nodeid:slot
		for id, slots := range node.Migrating {
			// 根据slot生成ranges
			ranges := []topo.Range{}
			//遍历节点上的slot,读取等待迁移的slot
			for _, slot := range slots {
				// 如果是自己
				if id == node.Id {
					redis.SetSlot(node.Addr(), slot, redis.SLOT_STABLE, "")
				} else {
					ranges = append(ranges, topo.Range{Left: slot, Right: slot})
				}
			}

			// Source  优先是从Master上处理
			//???????????????
			source := node
			if !node.IsMaster() {
				//node
				srs := cluster.FindReplicaSetByNode(node.Id)
				if srs != nil {
					//找到这个节点的主
					source = srs.Master
				}
			}

			// Target  读取待处理的node
			rs := cluster.FindReplicaSetByNode(id)

			//如果source已经挂了，那这份数据就没法迁移了?
			//这里记录一个日志
			if source.Fail || rs.Master.Fail {
				continue
			}

			//创建一个数据迁移任务
			_, err := m.CreateTask(source.Id, rs.Master.Id, ranges, cluster)
			//记录日志
			if err != nil {
				log.Warningf(node.Addr(), "Can not recover migrate task, %v", err)
			} else {
				log.Warningf(node.Addr(), "Will recover migrating task for node %s(%s) with MIGRATING info"+
					", Task(Source:%s, Target:%s).", node.Id, node.Addr(), source.Addr(), rs.Master.Addr())
				goto done
			}
		}

		//处理数据迁入
		for id, slots := range node.Importing {
			// 根据slot生成ranges
			ranges := []topo.Range{}
			for _, slot := range slots {
				// 如果是自己
				if id == node.Id {
					redis.SetSlot(node.Addr(), slot, redis.SLOT_STABLE, "")
				} else {
					ranges = append(ranges, topo.Range{Left: slot, Right: slot})
				}
			}
			// Target
			target := node
			if !node.IsMaster() {
				trs := cluster.FindReplicaSetByNode(node.Id)
				if trs != nil {
					target = trs.Master
				}
			}
			if target.IsStandbyMaster() {
				s := cluster.FindNodeBySlot(ranges[0].Left)
				if s != nil {
					log.Warningf(node.Addr(), "Reset migrate task target to %s(%s)", s.Id, s.Addr())
					target = s
				}
			}
			// Source
			rs := cluster.FindReplicaSetByNode(id)
			if target.Fail || rs.Master.Fail {
				continue
			}

			//创建数据迁移命令
			_, err := m.CreateTask(rs.Master.Id, target.Id, ranges, cluster)
			if err != nil {
				log.Warningf(node.Addr(), "Can not recover migrate task, %v", err)
			} else {
				log.Warningf(node.Addr(), "Will recover migrating task for node %s(%s) with IMPORTING info"+
					", Task(Source:%s,Target:%s).", node.Id, node.Addr(), rs.Master.Addr(), target.Addr())
				goto done
			}
		}
	}

done:
	for _, task := range m.tasks {
		//没有处理完成，继续重试
		if task.CurrentState() != StateDone {
			m.handleTaskChange(task, cluster)
		}
	}
}

func (m *MigrateManager) RunRebalanceTask(plans []*MigratePlan, cluster *topo.Cluster) error {
	if m.rebalanceTask != nil {
		return ErrRebalanceTaskExist
	}
	now := time.Now()
	rbtask := &RebalanceTask{plans, &now, nil}
	m.rebalanceTask = rbtask
	go m.rebalance(rbtask, cluster)
	return nil
}

// rebuild tasks from zk
func (m *MigrateManager) RebuildTasks(migrateMetas []*meta.MigrateMeta, cluster *topo.Cluster) {
	if migrateMetas != nil {
		for _, mm := range migrateMetas {
			task, err := m.CreateTask(mm.SourceId, mm.TargetId, mm.Ranges, cluster)
			if err == nil {
				log.Info(task.TaskName(), "Load task from zk")
			} else {
				log.Warning(task.TaskName(), "CreateTask failed, %v", err)
			}
		}
	}
}

func (m *MigrateManager) rebalance(rbtask *RebalanceTask, cluster *topo.Cluster) {
	// 启动所有任务，失败则等待一会进行重试
	for {
		allRunning := true
		for _, plan := range rbtask.Plans {
			if plan.task == nil {
				task, err := m.CreateTask(plan.SourceId, plan.TargetId, plan.Ranges, cluster)
				if err == nil {
					log.Infof(task.TaskName(), "Rebalance task created, %v", task)
					plan.task = task
				} else {
					allRunning = false
				}
			}
		}
		if allRunning {
			break
		}
		streams.RebalanceStateStream.Pub(*m.rebalanceTask)
		time.Sleep(5 * time.Second)
	}
	// 等待结束
	for {
		allDone := true
		for _, plan := range rbtask.Plans {
			state := plan.task.CurrentState()
			if state != StateDone && state != StateCancelled {
				allDone = false
			}
		}
		if allDone {
			break
		}
		streams.RebalanceStateStream.Pub(*m.rebalanceTask)
		time.Sleep(5 * time.Second)
	}
	now := time.Now()
	m.rebalanceTask.EndTime = &now
	streams.RebalanceStateStream.Pub(*m.rebalanceTask)
	m.rebalanceTask = nil
}

/// helpers

func SetSlotToNode(rs *topo.ReplicaSet, slot int, targetId string) error {
	// 先清理从节点的MIGRATING状态
	for _, node := range rs.Slaves {
		if node.Fail {
			continue
		}
		err := redis.SetSlot(node.Addr(), slot, redis.SLOT_NODE, targetId)
		if err != nil {
			return err
		}
	}
	err := redis.SetSlot(rs.Master.Addr(), slot, redis.SLOT_NODE, targetId)
	if err != nil {
		return err
	}
	return nil
}

func SetSlotStable(rs *topo.ReplicaSet, slot int) error {
	// 先清理从节点的MIGRATING状态
	for _, node := range rs.Slaves {
		if node.Fail {
			continue
		}
		err := redis.SetSlot(node.Addr(), slot, redis.SLOT_STABLE, "")
		if err != nil {
			return err
		}
	}
	err := redis.SetSlot(rs.Master.Addr(), slot, redis.SLOT_STABLE, "")
	if err != nil {
		return err
	}
	return nil
}
