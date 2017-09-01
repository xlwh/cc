package inspector

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/ksarch-saas/cc/frontend/api"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/topo"
	"github.com/ksarch-saas/cc/utils"
)

//获取Leader的地址
func MkUrl(path string) string {
	return "http://" + meta.LeaderHttpAddress() + path
}

//把本地的node信息同步给cluster controller
func SendRegionTopoSnapshot(nodes []*topo.Node, failureInfo *topo.FailureInfo) error {
	params := &api.RegionSnapshotParams{
		Region:      meta.LocalRegion(), //来源地域
		PostTime:    time.Now().Unix(),  //发送时间
		Nodes:       nodes,              //节点列表
		FailureInfo: failureInfo,        //挂了的节点信息
	}

	//超时时间设定了30 ms
	resp, err := utils.HttpPost(MkUrl(api.RegionSnapshotPath), params, 30*time.Second)
	if err != nil {
		return err
	}
	if resp.Errno != 0 {
		return fmt.Errorf("%d %s", resp.Errno, resp.Errmsg)
	}
	return nil
}

func containsNode(node *topo.Node, nodes []*topo.Node) bool {
	for _, n := range nodes {
		if n.Id == node.Id {
			return true
		}
	}
	return false
}

//判断一个集群是不是挂了
//判断的依据:超过一半的mater或者一半的node都挂了
func (self *Inspector) IsClusterDamaged(cluster *topo.Cluster, seeds []*topo.Node) bool {
	// more than half masters dead
	numFail := 0
	for _, node := range cluster.MasterNodes() {
		if node.Fail {
			numFail++
		}
	}
	if numFail >= (cluster.Size()+1)/2 {
		return true
	}

	// more than half nodes dead
	if len(seeds) > cluster.NumLocalRegionNode()/2 {
		return false
	}
	for _, seed := range seeds {
		c, err := self.initClusterTopo(seed)
		if err != nil {
			return false
		}
		for _, node := range c.LocalRegionNodes() {
			// nodes not in seeds must be pfail
			if !containsNode(node, seeds) && !node.PFail {
				return false
			}
		}
	}
	glog.Info("more than half nodes dead")
	return true
}

//针对 len(failnodes) == len (freednodes) 情况自动修复
//自动恢复被摘除的节点
func FixClusterCircle() {
	// 定时fixcluster，针对 len(failnodes) == len (freednodes) 情况自动修复
	app := meta.GetAppConfig()
	trickerTime := app.FixClusterCircle

	if trickerTime == 0 {
		trickerTime = meta.DEFAULT_FIXCLUSTER_CIRCLE
	}
	tickChan := time.NewTicker(time.Second * time.Duration(trickerTime)).C
	for {
		select {
		case <-tickChan:
			app = meta.GetAppConfig()
			autouFixCluster := app.AutoFixCluster
			glog.Infof("ClusterLeader:%s, RegionLeader:%s", meta.ClusterLeaderZNodeName(), meta.RegionLeaderZNodeName())
			if meta.IsClusterLeader() && autouFixCluster {
				addr := meta.LeaderHttpAddress()
				url := "http://" + addr + api.FixClusterPath
				_, err := utils.HttpPost(url, nil, 0)
				if err != nil {
					glog.Info(err.Error())
				}
			}
		}
	}
}

//运行inspector，是一个核心的方法
func (self *Inspector) Run() {
	//周期性修复
	go FixClusterCircle()
	appconfig := meta.GetAppConfig()

	// FetchClusterNodesInterval not support heat loading
	//定时器，定时运行任务
	//【核心方法】
	tickChan := time.NewTicker(appconfig.FetchClusterNodesInterval).C
	for {
		select {
		case <-tickChan:
			//只有leader才有权限处理
			if !meta.IsRegionLeader() {
				continue
			}

			//和redis交互，拉取拓扑，更新本地的拓扑，会把Fail的节点去掉
			cluster, seeds, err := self.BuildClusterTopo()
			if err != nil {
				glog.Infof("build cluster topo failed, %v", err)
			}

			//没有拉取到拓扑，本次不更新
			//这里可以记录日志
			if cluster == nil {
				continue
			}

			//获取fail信息
			var failureInfo *topo.FailureInfo
			//controller在主地域并且集群都挂了的情况，记录Fail，发送给cluster leader
			if meta.IsInMasterRegion() && self.IsClusterDamaged(cluster, seeds) {
				//把这个集群内的所有的node都放到FailInfo里面发出去
				failureInfo = &topo.FailureInfo{Seeds: seeds}
			}
			//更新Node
			var nodes []*topo.Node
			if err == nil {
				nodes = cluster.LocalRegionNodes()
			}

			//把信息发给cluster leader进行同步
			err = SendRegionTopoSnapshot(nodes, failureInfo)
			if err != nil {
				glog.Infof("send snapshot failed, %v", err)
			}
		}
	}
}
