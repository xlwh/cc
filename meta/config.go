package meta

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/ksarch-saas/cc/topo"
	zookeeper "github.com/samuel/go-zookeeper/zk"
)

const (
	DEFAULT_AUTOFAILOVER_INTERVAL        time.Duration = 5 * time.Minute // 5min
	DEFAULT_FETCH_CLUSTER_NODES_INTERVAL time.Duration = 1 * time.Second
	DEFAULT_MIGRATE_KEYS_EACH_TIME                     = 100
	DEFAULT_MIGRATE_KEYS_STEP                          = 1
	DEFAULT_MIGRATE_TIMEOUT                            = 2000
	DEFAULT_MIGRATE_CONCURRENCY                        = 3
	DEFAULT_FIXCLUSTER_CIRCLE                          = 10
)

//服务app的配置
type AppConfig struct {
	AppName                   string //app名字
	AutoEnableSlaveRead       bool   //自动屏蔽读
	AutoEnableMasterWrite     bool   //自动屏蔽写
	AutoFailover              bool   //自动故障迁移
	AutoFailoverInterval      time.Duration
	MasterRegion              string   //Mater所在的地域
	Regions                   []string //区域
	MigrateKeysEachTime       int
	MigrateKeysStep           int //key迁移的进度
	MigrateTimeout            int //迁移超时设置
	SlaveFailoverLimit        bool
	FetchClusterNodesInterval time.Duration
	MigrateConcurrency        int
	FixClusterCircle          int  //fix周期
	AutoFixCluster            bool //自动fix集群特性
}

//controller配置
type ControllerConfig struct {
	Ip       string
	HttpPort int
	WsPort   int
	Region   string
}

//故障迁移记录
type FailoverRecord struct {
	AppName   string //App名
	NodeId    string
	NodeAddr  string
	Timestamp time.Time
	Region    string
	Tag       string
	Role      string       //角色
	Ranges    []topo.Range //slot范围
}

//监听zk中app配置改变，类似于配置热加载的特性
func (m *Meta) handleAppConfigChanged(watch <-chan zookeeper.Event) {
	for {
		event := <-watch
		if event.Type == zookeeper.EventNodeDataChanged || event.Type == zookeeper.EventNotWatching {
			a, w, err := m.FetchAppConfig()
			if err != nil {
				for {
					glog.Warningf("meta: fetch app config again, %v", err)
					time.Sleep(1 * time.Second)
					a, w, err = m.FetchAppConfig()
					if err == nil {
						break
					}
				}
			}
			if a.MigrateKeysEachTime == 0 {
				a.MigrateKeysEachTime = DEFAULT_MIGRATE_KEYS_EACH_TIME
			}
			if a.MigrateKeysStep == 0 {
				a.MigrateKeysStep = DEFAULT_MIGRATE_KEYS_STEP
			}
			if a.MigrateTimeout == 0 {
				a.MigrateTimeout = DEFAULT_MIGRATE_TIMEOUT
			}
			if a.AutoFailoverInterval == 0 {
				a.AutoFailoverInterval = DEFAULT_AUTOFAILOVER_INTERVAL
			}
			if a.FetchClusterNodesInterval == 0 {
				a.FetchClusterNodesInterval = DEFAULT_FETCH_CLUSTER_NODES_INTERVAL
			}
			if a.MigrateConcurrency == 0 {
				a.MigrateConcurrency = DEFAULT_MIGRATE_CONCURRENCY
			}
			if a.FixClusterCircle == 0 {
				a.FixClusterCircle = DEFAULT_FIXCLUSTER_CIRCLE
			}
			m.appConfig.Store(a)
			glog.Warning("meta: app config changed.", a)
			watch = w
		} else {
			glog.Warningf("meta: unexpected event coming, %v", event)
			break
		}
	}
}

//从zk中拉取app配置
func (m *Meta) FetchAppConfig() (*AppConfig, <-chan zookeeper.Event, error) {
	zconn := m.zconn
	appName := m.appName
	data, _, watch, err := zconn.GetW("/r3/app/" + appName)
	if err != nil {
		return nil, watch, err
	}
	var c AppConfig
	err = json.Unmarshal([]byte(data), &c)
	if err != nil {
		return nil, watch, fmt.Errorf("meta: parse app config error, %v", err)
	}
	if c.AppName != appName {
		return nil, watch, fmt.Errorf("meta: local appname is different from zk, %s <-> %s", appName, c.AppName)
	}
	if c.MasterRegion == "" {
		return nil, watch, fmt.Errorf("meta: master region not set")
	}
	if len(c.Regions) == 0 {
		return nil, watch, fmt.Errorf("meta: regions empty")
	}
	if c.MigrateKeysEachTime == 0 {
		c.MigrateKeysEachTime = DEFAULT_MIGRATE_KEYS_EACH_TIME
	}
	if c.MigrateKeysStep == 0 {
		c.MigrateKeysStep = DEFAULT_MIGRATE_KEYS_STEP
	}
	if c.MigrateTimeout == 0 {
		c.MigrateTimeout = DEFAULT_MIGRATE_TIMEOUT
	}
	if c.AutoFailoverInterval == 0 {
		c.AutoFailoverInterval = DEFAULT_AUTOFAILOVER_INTERVAL
	}
	if c.FetchClusterNodesInterval == 0 {
		c.FetchClusterNodesInterval = DEFAULT_FETCH_CLUSTER_NODES_INTERVAL
	}
	if c.MigrateConcurrency == 0 {
		c.MigrateConcurrency = DEFAULT_MIGRATE_CONCURRENCY
	}
	if c.FixClusterCircle == 0 {
		c.FixClusterCircle = DEFAULT_FIXCLUSTER_CIRCLE
	}
	return &c, watch, nil
}

//在zk中注册一个新的controller
func (m *Meta) RegisterLocalController() error {
	zconn := m.zconn
	zkPath := fmt.Sprintf(m.ccDirPath + "/cc_" + m.localRegion + "_")
	conf := &ControllerConfig{
		Ip:       m.localIp,
		HttpPort: m.httpPort,
		Region:   m.localRegion,
		WsPort:   m.wsPort,
	}
	data, err := json.Marshal(conf)
	if err != nil {
		return err
	}
	path, err := zconn.Create(zkPath, data, zookeeper.FlagEphemeral|zookeeper.FlagSequence, zookeeper.WorldACL(PERM_FILE))
	if err == nil {
		xs := strings.Split(path, "/")
		m.selfZNodeName = xs[len(xs)-1]
	}
	return err
}

//从zk中拉取controller配置
func (m *Meta) FetchControllerConfig(zkNode string) (*ControllerConfig, <-chan zookeeper.Event, error) {
	data, _, watch, err := m.zconn.GetW(m.ccDirPath + "/" + zkNode)
	if err != nil {
		return nil, watch, err
	}
	var c ControllerConfig
	err = json.Unmarshal([]byte(data), &c)
	if err != nil {
		return nil, watch, err
	}
	return &c, watch, nil
}

//判断是否在做故障迁移
func (m *Meta) IsDoingFailover() (bool, error) {
	exist, _, err := m.zconn.Exists("/r3/failover/doing")
	if err == nil {
		return exist, nil
	} else {
		return true, err
	}
}

func (m *Meta) MarkFailoverDoing(record *FailoverRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	path, err := m.zconn.Create("/r3/failover/doing", data,
		zookeeper.FlagEphemeral, zookeeper.WorldACL(PERM_FILE))
	if err != nil {
		return err
	}
	glog.Warningf("meta: mark doing failover at %s", path)
	return nil
}

func (m *Meta) DoingFailoverRecord() (*FailoverRecord, error) {
	data, _, err := m.zconn.Get("/r3/failover/doing")
	if err != nil {
		return nil, err
	}

	var record FailoverRecord
	err = json.Unmarshal([]byte(data), &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (m *Meta) UnmarkFailoverDoing() error {
	err := m.zconn.Delete("/r3/failover/doing", -1)
	if err != nil {
		return err
	}
	glog.Warning("meta: unmark doing failover")
	return nil
}

func (m *Meta) LastFailoverRecord() (*FailoverRecord, error) {
	children, stat, err := m.zconn.Children("/r3/failover/history")
	if err != nil {
		return nil, err
	}
	if stat.NumChildren == 0 {
		return nil, nil
	}

	last := children[len(children)-1]
	data, _, err := m.zconn.Get("/r3/failover/history/" + last)
	if err != nil {
		return nil, err
	}

	var record FailoverRecord
	err = json.Unmarshal([]byte(data), &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (m *Meta) AddFailoverRecord(record *FailoverRecord) error {
	zkPath := fmt.Sprintf("/r3/failover/history/record_%s_%s", record.AppName, record.Region)
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	path, err := m.zconn.Create(zkPath, data, zookeeper.FlagSequence, zookeeper.WorldACL(PERM_FILE))
	glog.Warningf("meta: failover record created at %s", path)
	return nil
}
