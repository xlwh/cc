package command

import (
	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/migrate"
	"github.com/ksarch-saas/cc/topo"
)

type MigrateCommand struct {
	SourceId string       //源id
	TargetId string       //目标id
	Ranges   []topo.Range //range
}

//故障迁移：只是创建一个task
func (self *MigrateCommand) Execute(c *cc.Controller) (cc.Result, error) {
	cs := c.ClusterState
	cluster := cs.GetClusterSnapshot()
	if cluster != nil {
		mm := c.MigrateManager
		//创建迁移计划
		_, err := mm.CreateTask(self.SourceId, self.TargetId, self.Ranges, cluster)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrClusterSnapshotNotReady
	}
	return nil, nil
}

type MigratePauseCommand struct {
	SourceId string
}

//可以暂停迁移
func (self *MigratePauseCommand) Execute(c *cc.Controller) (cc.Result, error) {
	mm := c.MigrateManager
	task := mm.FindTaskBySource(self.SourceId)
	if task == nil {
		return nil, ErrMigrateTaskNotExist
	}
	//更新一下任务状态
	task.SetState(migrate.StatePausing)
	return nil, nil
}

type MigrateResumeCommand struct {
	SourceId string
}

func (self *MigrateResumeCommand) Execute(c *cc.Controller) (cc.Result, error) {
	mm := c.MigrateManager
	task := mm.FindTaskBySource(self.SourceId)
	if task == nil {
		return nil, ErrMigrateTaskNotExist
	}
	task.SetState(migrate.StateRunning)
	return nil, nil
}

type MigrateCancelCommand struct {
	SourceId string
}

func (self *MigrateCancelCommand) Execute(c *cc.Controller) (cc.Result, error) {
	mm := c.MigrateManager
	task := mm.FindTaskBySource(self.SourceId)
	if task == nil {
		return nil, ErrMigrateTaskNotExist
	}
	task.SetState(migrate.StateCancelling)
	return nil, nil
}
