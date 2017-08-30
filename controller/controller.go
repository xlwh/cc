package controller

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/migrate"
	"github.com/ksarch-saas/cc/state"
)

var (
	ErrProcessCommandTimedout = errors.New("controller: process command timeout")
	ErrNotClusterLeader       = errors.New("controller: not cluster leader")
	ErrNotRegionLeader        = errors.New("controller: not region leader")
)

type Controller struct {
	mutex          sync.Mutex
	ClusterState   *state.ClusterState
	MigrateManager *migrate.MigrateManager
}

func NewController() *Controller {
	c := &Controller{
		MigrateManager: migrate.NewMigrateManager(),
		ClusterState:   state.NewClusterState(),
		mutex:          sync.Mutex{},
	}
	return c
}

//命令执行
func (c *Controller) ProcessCommand(command Command, timeout time.Duration) (result Result, err error) {
	//两种命令:region和cluster要做区分判断
	switch command.Type() {
	case REGION_COMMAND:
		if !meta.IsRegionLeader() {
			return nil, ErrNotRegionLeader
		}
	case CLUSTER_COMMAND:
		if !meta.IsClusterLeader() {
			return nil, ErrNotClusterLeader
		}
	}

	// 一次处理一条命令，也即同一时间只能在做一个状态变换
	commandType := strings.Split(reflect.TypeOf(command).String(), ".")
	commandName := ""
	if len(commandType) == 2 && commandType[1] != "UpdateRegionCommand" {
		commandName = commandType[1]
	}
	if commandName != "" {
		log.Infof("OP", "Command: %s, Event:Start", commandName)
	}
	// command must be excuted serial
	if command.Mutex() == MUTEX_COMMAND {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}

	resultCh := make(chan Result)
	errorCh := make(chan error)

	//c.ClusterState.DebugDump()

	go func() {
		//执行命令，处理错误放到channel
		result, err := command.Execute(c)
		if err != nil {
			errorCh <- err
		} else {
			resultCh <- result
		}
	}()

	//处理channel中的数据
	select {
	case result = <-resultCh:
	case err = <-errorCh:
	case <-time.After(timeout):
		err = ErrProcessCommandTimedout
	}
	if commandName != "" {
		log.Infof("OP", "Command: %s, Event:End", commandName)
	}
	return
}
