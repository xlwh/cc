package meta

import (
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"
)

//从zk中读取controller配置，同步到本地meta中的记录
func (m *Meta) CheckLeaders(watch bool) (string, string, <-chan zookeeper.Event, error) {
	zkPath := m.ccDirPath
	zconn := m.zconn

	var children []string
	var stat *zookeeper.Stat
	var watcher <-chan zookeeper.Event
	var err error

	if watch {
		children, stat, watcher, err = zconn.ChildrenW(zkPath)
	} else {
		children, stat, err = zconn.Children(zkPath)
	}
	if err != nil {
		return "", "", watcher, err
	}

	glog.Infof("Total controllers %d", stat.NumChildren)

	needRejoin := true
	clusterMinSeq := -1
	clusterLeader := ""
	regionMinSeq := -1
	regionLeader := ""

	for _, child := range children {
		xs := strings.Split(child, "_")
		seq, _ := strconv.Atoi(xs[2])
		region := xs[1]
		// Cluster Leader, should be in MasterRegion
		if MasterRegion() == region {
			if clusterMinSeq < 0 {
				clusterMinSeq = seq
				clusterLeader = child
			}
			if seq < clusterMinSeq {
				clusterMinSeq = seq
				clusterLeader = child
			}
		}
		// Region Leader
		if m.localRegion == region {
			if regionMinSeq < 0 {
				regionMinSeq = seq
				regionLeader = child
			}
			if seq < regionMinSeq {
				regionMinSeq = seq
				regionLeader = child
			}
		}
		// Rejoin
		if m.selfZNodeName == child {
			needRejoin = false
		}
	}

	if needRejoin {
		err := m.RegisterLocalController()
		if err != nil {
			return "", "", watcher, err
		}
	}

	return clusterLeader, regionLeader, watcher, nil
}

//注册cluster配置状态监听
func (m *Meta) handleClusterLeaderConfigChanged(znode string, watch <-chan zookeeper.Event) {
	for {
		event := <-watch
		if event.Type == zookeeper.EventNodeDataChanged {
			if m.clusterLeaderZNodeName != znode {
				glog.Info("meta: region leader has changed")
				break
			}
			c, w, err := m.FetchControllerConfig(znode)
			if err == nil {
				m.clusterLeaderConfig = c
				glog.Info("meta: cluster leader config changed.")
			} else {
				glog.Infof("meta: fetch controller config failed, %v", err)
			}
			watch = w
		} else {
			glog.Infof("meta: unexpected event coming, %v", event)
			break
		}
	}
}

//注册本地controller配置变化监听
func (m *Meta) handleRegionLeaderConfigChanged(znode string, watch <-chan zookeeper.Event) {
	for {
		event := <-watch
		if event.Type == zookeeper.EventNodeDataChanged {
			if m.regionLeaderZNodeName != znode {
				glog.Info("meta: region leader has changed")
				break
			}
			c, w, err := m.FetchControllerConfig(znode)
			if err == nil {
				m.regionLeaderConfig = c
				glog.Info("meta: region leader config changed.")
			} else {
				glog.Infof("meta: fetch controller config failed, %v", err)
			}
			watch = w
		} else {
			glog.Infof("meta: unexpected event coming, %v", event)
			break
		}
	}
}

//服务选主
func (m *Meta) ElectLeader() (<-chan zookeeper.Event, error) {
	//检查更新本地COntroller配置
	clusterLeader, regionLeader, watcher, err := m.CheckLeaders(true)
	if err != nil {
		return watcher, err
	}
	//如果第一次没有拿到配置，就一直重试直到成功
	if clusterLeader == "" || regionLeader == "" {
		for {
			glog.Info("meta: get leaders failed. will retry")
			time.Sleep(1 * time.Second)
			clusterLeader, regionLeader, watcher, err = m.CheckLeaders(true)
			if clusterLeader != "" {
				break
			}
		}
	}

	glog.Infof("meta: clusterleader:%s, regionleader:%s", clusterLeader, regionLeader)

	//从zk中读取到的最新的controller主信息和本地记录的不一致
	if m.clusterLeaderZNodeName != clusterLeader {
		// 获取ClusterLeader配置
		c, w, err := m.FetchControllerConfig(clusterLeader)
		if err != nil {
			return watcher, err
		}
		//重新设置cluster主
		m.clusterLeaderConfig = c
		m.clusterLeaderZNodeName = clusterLeader

		//注册状态变化监听
		go m.handleClusterLeaderConfigChanged(clusterLeader, w)
	}

	//从zk中读取到的最新的region的主的信息和本地meta中记录的不一致
	if m.regionLeaderZNodeName != regionLeader {
		c, w, err := m.FetchControllerConfig(regionLeader)
		if err != nil {
			return watcher, err
		}
		//重新设置region配置
		m.regionLeaderConfig = c
		m.regionLeaderZNodeName = regionLeader

		//注册状态变化的监听
		go m.handleRegionLeaderConfigChanged(regionLeader, w)
	}
	return watcher, nil
}
