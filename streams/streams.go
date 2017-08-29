package streams

import (
	"time"

	"github.com/ksarch-saas/cc/topo"
)

//节点状态数据流结构
type NodeStateStreamData struct {
	*topo.Node        //节点
	State      string //状态
	Version    int64  //版本
}

//slot迁移出数据流结构定义
type MigrateStateStreamData struct {
	SourceId       string       //源来id
	TargetId       string       //目标id
	State          string       //状态
	Ranges         []topo.Range //slot range
	CurrRangeIndex int          //当前的index
	CurrSlot       int          //当前的slot
}

//日志数据流
type LogStreamData struct {
	Level   string    //日志级别
	Time    time.Time //时间
	Target  string    //目标
	Message string    //日志数据
}

//几种数据流实例，大小都是4096
var (
	NodeStateStream      = NewStream("NodeStateStream", 4096)
	MigrateStateStream   = NewStream("MigrateStateStream", 4096)
	RebalanceStateStream = NewStream("RebalanceStateStream", 4096)
	LogStream            = NewStream("LogStream", 4096)
)

//启动所有类别的数据流
func StartAllStreams() {
	go NodeStateStream.Run()
	go MigrateStateStream.Run()
	go RebalanceStateStream.Run()
	go LogStream.Run()
}
