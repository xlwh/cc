package command

import (
	"errors"
)

//异常和错误的定义
var (
	ErrNodeNotFree             = errors.New("this is not a free node")
	ErrNodeIsFree              = errors.New("this is a free node")
	ErrNodeIsDead              = errors.New("node is dead")
	ErrNodeNotExist            = errors.New("node not exist")
	ErrNodeNotEmpty            = errors.New("node not empty")
	ErrNodeNotMaster           = errors.New("node is not master")
	ErrNodeIsMaster            = errors.New("node is master")
	ErrMigrateTaskNotExist     = errors.New("migration task not exist")
	ErrClusterSnapshotNotReady = errors.New("cluster snapshot not ready")
)
