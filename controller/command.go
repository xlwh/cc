package controller

type Result interface{}

type CommandType int

const (
	REGION_COMMAND CommandType = iota
	CLUSTER_COMMAND
	MUTEX_COMMAND
	NOMUTEX_COMMAND
)

//定义了一个接口，可以让每个命令都实现这个接口
type Command interface {
	Type() CommandType
	Mutex() CommandType
	Execute(*Controller) (Result, error) //命令执行
}
