package main

import (
	"flag"
	"strings"

	"github.com/golang/glog"
	"github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/frontend"
	"github.com/ksarch-saas/cc/inspector"
	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/streams"
	"github.com/ksarch-saas/cc/topo"
	"github.com/ksarch-saas/cc/utils"
)

//一些全局变量的定义
var (
	appName     string //app名字
	localRegion string //cc部署的地域
	seeds       string //redis服务节点
	zkHosts     string //zk列表
	zkService   string //zk的bns配置
	httpPort    int    //cc的http端口
	wsPort      int    //这个端口还不知道干嘛的
)

//初始化，配置启动命令行解析，会带着一些默认配置
func init() {
	flag.StringVar(&appName, "appname", "", "app name")
	flag.StringVar(&localRegion, "local-region", "", "local region")
	flag.StringVar(&seeds, "seeds", "", "redis cluster seeds, seperate by comma")
	flag.StringVar(&zkHosts, "zkhosts", "", "zk hosts, seperate by comma")
	flag.StringVar(&zkService, "zkservice", "", "naming service locate zookeeper")
	flag.IntVar(&httpPort, "http-port", 0, "http port")
	flag.IntVar(&wsPort, "ws-port", 0, "ws port")
}

//服务的入口
func main() {
	//解析命令行输入的参数
	flag.Parse()

	//创建一个seedNode数组，用来保存seed列表
	seedNodes := []*topo.Node{}
	//遍历输入的seed，创建node
	for _, addr := range strings.Split(seeds, ",") {
		glog.Info(addr)
		n := topo.NewNodeFromString(addr)
		if n == nil {
			glog.Fatal("invalid seeds %s", addr)
		}
		seedNodes = append(seedNodes, n)
	}

	//判断端口合法性
	if httpPort == 0 {
		glog.Fatal("invalid http port")
		flag.PrintDefaults()
	}
	if wsPort == 0 {
		glog.Fatal("invalid websocket port")
		flag.PrintDefaults()
	}

	//如果没有配置zkHosts则会默认使用BNS
	if zkHosts == "" {
		glog.Info("zkHosts not set, trying to locate by naming service")
		var err error
		zkHosts, err = utils.ResolvZkFromNaming(zkService)
		if err != nil {
			glog.Fatal("Resolve zkhosts failed")
		}
		glog.Infof("Resolve zkhosts from naming service, result is %s", zkHosts)
	}

	//创建一个channel，用来输入错误信息
	initCh := make(chan error)

	//启动meta服务
	go meta.Run(appName, localRegion, httpPort, wsPort, zkHosts, seedNodes, initCh)

	//处理meta启动中的错误
	err := <-initCh
	if err != nil {
		glog.Fatal(err)
	}

	//启动所有的流
	streams.StartAllStreams()

	//订阅日志流(另外的分支，可以先不看)
	streams.LogStream.Sub(log.WriteFileHandler)
	streams.LogStream.Sub(log.WriteRingBufferHandler)

	//启动检查器
	sp := inspector.NewInspector()
	go sp.Run()

	//启动frontend
	c := controller.NewController()
	fe := frontend.NewFrontEnd(c, httpPort, wsPort)
	fe.Run()
}
