package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/topo"
)

var (
	ErrNotCutter   = errors.New("redis: the server is not a cutter")
	ErrConnFailed  = errors.New("redis: connection error")
	ErrPingFailed  = errors.New("redis: ping error")
	ErrServer      = errors.New("redis: server error")
	ErrInvalidAddr = errors.New("redis: invalid address string")
	poolMap        map[string]*redis.Pool //redis connection pool for each server
	poolMutex      *sync.RWMutex
)

const (
	SLOT_MIGRATING = "MIGRATING" //slot迁出
	SLOT_IMPORTING = "IMPORTING" //data迁入
	SLOT_STABLE    = "STABLE"
	SLOT_NODE      = "NODE"

	NUM_RETRY     = 3 //重试次数
	CONN_TIMEOUT  = 5 * time.Second
	READ_TIMEOUT  = 120 * time.Second //读超时
	WRITE_TIMEOUT = 120 * time.Second //写超时
)

//连接redis，如果不在连接池中，重新连接，如果在连接池中，则直接获取连接池中的
func dial(addr string) (redis.Conn, error) {
	//初始化连接池
	if poolMap == nil {
		poolMap = make(map[string]*redis.Pool)
	}
	if poolMutex == nil {
		poolMutex = &sync.RWMutex{}
	}

	inner := func(addr string) (redis.Conn, error) {
		//连接池加锁
		poolMutex.RLock()
		_, ok := poolMap[addr]
		poolMutex.RUnlock()
		//节点不在连接池中才处理
		if !ok {
			//not exist in map
			poolMutex.Lock()
			poolMap[addr] = &redis.Pool{
				MaxIdle:     3,
				IdleTimeout: 240 * time.Second,
				Dial: func() (redis.Conn, error) {
					c, err := redis.DialTimeout("tcp", addr, CONN_TIMEOUT, READ_TIMEOUT, WRITE_TIMEOUT)
					if err != nil {
						return nil, err
					}
					return c, nil
				},
				//连接上后会ping一下节点，检查是不是正常的节点
				TestOnBorrow: func(c redis.Conn, t time.Time) error {
					_, err := c.Do("PING")
					return err
				},
			}
			poolMutex.Unlock()
		}
		poolMutex.RLock()
		pool, ok := poolMap[addr]
		poolMutex.RUnlock()
		if ok {
			return pool.Get(), nil
		} else {
			return nil, ErrConnFailed
		}
	}
	resp, err := inner(addr)
	if err == nil {
		return resp, nil
	}
	return nil, err
}

/// Misc

//判断redis是否还是活着的
func IsAlive(addr string) bool {
	conn, err := dial(addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	resp, err := redis.String(conn.Do("PING"))
	if err != nil || resp != "PONG" {
		return false
	}
	return true
}

/// Cluster
//指定redis节点，进行数据的复制
func ReplicateTarget(addr string, targetId string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = redis.String(conn.Do("cluster", "replicate", targetId))
	return err
}

//将一个节点设置为Master
func SetAsMasterWaitSyncDone(addr string, waitSyncDone bool, takeover bool, rs *topo.ReplicaSet) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	//广播数据迁移
	//change failover force to failover takeover, in case of arbiter vote
	if takeover {
		_, err = redis.String(conn.Do("cluster", "failover", "takeover"))
	} else {
		_, err = redis.String(conn.Do("cluster", "failover", "force"))
	}

	if err != nil {
		return err
	}

	if !waitSyncDone {
		return nil
	}

	//一直抓取info，判断数据是否迁移完成
	for {
		info, err := FetchInfo(addr, "replication")
		//5s重试一次
		time.Sleep(5 * time.Second)
		if err == nil {
			n, err := info.GetInt64("connected_slaves")
			if err != nil {
				continue
			}
			done := true
			for i := int64(0); i < n; i++ {
				repl := info.Get(fmt.Sprintf("slave%d", i))
				if !strings.Contains(repl, "online") {
					done = false
				}
			}
			if done {
				return nil
			}
		}
	}
	return nil
}

//从redis cluster上读取所有的node
func ClusterNodes(addr string) (string, error) {
	inner := func(addr string) (string, error) {
		conn, err := dial(addr)
		if err != nil {
			return "", ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("cluster", "nodes", "extra"))
		if err != nil {
			return "", err
		}
		return resp, nil
	}
	//指定重试次数进行重试
	retry := NUM_RETRY
	var err error
	var resp string
	for retry > 0 {
		resp, err = inner(addr)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return "", err
}

//读取当前地域的node
func ClusterNodesInRegion(addr string, region string) (string, error) {
	inner := func(addr string, region string) (string, error) {
		conn, err := dial(addr)
		if err != nil {
			return "", ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("cluster", "nodes", "extra", region))
		if err != nil {
			return "", err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp string
	for retry > 0 {
		resp, err = inner(addr, region)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return "", err
}

//读取redis上记录的info
func FetchClusterInfo(addr string) (topo.ClusterInfo, error) {
	clusterInfo := topo.ClusterInfo{}
	conn, err := dial(addr)
	if err != nil {
		return clusterInfo, ErrConnFailed
	}
	defer conn.Close()

	resp, err := redis.String(conn.Do("cluster", "info"))
	if err != nil {
		return clusterInfo, err
	}

	lines := strings.Split(resp, "\n")
	for _, line := range lines {
		xs := strings.Split(strings.TrimSpace(line), ":")
		if len(xs) != 2 {
			continue
		}
		switch xs[0] {
		case "cluster_state":
			clusterInfo.ClusterState = xs[1]
		case "cluster_slots_assigned":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterSlotsAssigned = n
		case "cluster_slots_ok":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterSlotsOk = n
		case "cluster_slots_pfail":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterSlotsPfail = n
		case "cluster_slots_fail":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterSlotsFail = n
		case "cluster_known_nodes":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterKnownNodes = n
		case "cluster_size":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterSize = n
		case "cluster_current_epoch":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterCurrentEpoch = n
		case "cluster_my_epoch":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterMyEpoch = n
		case "cluster_stats_messages_sent":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterStatsMessagesSent = n
		case "cluster_stats_messages_received":
			n, _ := strconv.Atoi(xs[1])
			clusterInfo.ClusterStatsMessagesReceived = n
		}
	}
	return clusterInfo, nil
}

//执行chmod命令
func ClusterChmod(addr, id, op string) (string, error) {
	inner := func(addr, id, op string) (string, error) {
		conn, err := dial(addr)
		if err != nil {
			return "", ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("cluster", "chmod", op, id))
		if err != nil {
			return "", err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp string
	for retry > 0 {
		resp, err = inner(addr, id, op)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return "", err
}

//设置节点不可读
func DisableRead(addr, id string) (string, error) {
	return ClusterChmod(addr, id, "-r")
}

//设置节点可读
func EnableRead(addr, id string) (string, error) {
	return ClusterChmod(addr, id, "+r")
}

//设置节点不可写
func DisableWrite(addr, id string) (string, error) {
	return ClusterChmod(addr, id, "-w")
}

//设置节点可写
func EnableWrite(addr, id string) (string, error) {
	return ClusterChmod(addr, id, "+w")
}

//执行集群故障迁移
func ClusterFailover(addr string, rs *topo.ReplicaSet) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	// 先正常Failover试试，如果主挂了再试试Force
	resp, err := redis.String(conn.Do("cluster", "failover"))
	if err != nil {
		if strings.HasPrefix(err.Error(), "ERR Master is down or failed") {
			resp, err = redis.String(conn.Do("cluster", "failover", "force"))
		}
		if err != nil {
			return "", err
		}
	}

	// 拉取info判断故障迁移结果
	for i := 0; i < 30; i++ {
		info, err := FetchInfo(addr, "Replication")
		if err != nil {
			return resp, err
		}
		if info.Get("role") == "slave" {
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	return resp, nil
}

//故障恢复
func ClusterTakeover(addr string, rs *topo.ReplicaSet) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	//发送takeover命令
	resp, err := redis.String(conn.Do("cluster", "failover", "takeover"))
	if err != nil {
		return "", err
	}

	// 30s
	for i := 0; i < 30; i++ {
		info, err := FetchInfo(addr, "Replication")
		if err != nil {
			return resp, err
		}
		if info.Get("role") == "slave" {
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	return resp, nil
}

//数据复制
func ClusterReplicate(addr, targetId string) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	resp, err := redis.String(conn.Do("cluster", "replicate", targetId))
	if err != nil {
		return "", err
	}

	return resp, nil
}

//广播新节点加入消息
func ClusterMeet(seedAddr, newIp string, newPort int) (string, error) {
	conn, err := dial(seedAddr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	resp, err := redis.String(conn.Do("cluster", "meet", newIp, newPort))
	if err != nil {
		return "", err
	}

	return resp, nil
}

//屏蔽节点
func ClusterForget(seedAddr, nodeId string) (string, error) {
	conn, err := dial(seedAddr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	resp, err := redis.String(conn.Do("cluster", "forget", nodeId))
	if err != nil {
		return "", err
	}

	return resp, nil
}

//reset
func ClusterReset(addr string, hard bool) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	flag := "soft"
	if hard {
		flag = "hard"
	}

	resp, err := redis.String(conn.Do("cluster", "", flag))
	if err != nil {
		return "", err
	}

	return resp, nil
}

/// Info

type RedisInfo map[string]string

func FetchInfo(addr, section string) (*RedisInfo, error) {
	inner := func(addr, section string) (*RedisInfo, error) {
		conn, err := dial(addr)
		if err != nil {
			return nil, ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("info", section))
		if err != nil {
			return nil, err
		}
		infomap := map[string]string{}
		lines := strings.Split(resp, "\r\n")
		for _, line := range lines {
			xs := strings.Split(line, ":")
			if len(xs) != 2 {
				continue
			}
			key := xs[0]
			value := xs[1]
			infomap[key] = value
		}

		redisInfo := RedisInfo(infomap)
		return &redisInfo, nil
	}
	retry := NUM_RETRY
	var err error
	var redisInfo *RedisInfo
	for retry > 0 {
		redisInfo, err = inner(addr, section)
		if err == nil {
			return redisInfo, nil
		}
		retry--
	}
	return nil, err
}

func (info *RedisInfo) Get(key string) string {
	return (*info)[key]
}

func (info *RedisInfo) GetInt64(key string) (int64, error) {
	return strconv.ParseInt((*info)[key], 10, 64)
}

/// Migrate

func SetSlot(addr string, slot int, action, toId string) error {
	conn, err := dial(addr)
	if err != nil {
		return ErrConnFailed
	}
	defer conn.Close()

	if action == SLOT_STABLE {
		_, err = redis.String(conn.Do("cluster", "setslot", slot, action))
	} else {
		_, err = redis.String(conn.Do("cluster", "setslot", slot, action, toId))
	}
	if err != nil {
		return err
	}
	return nil
}

func CountKeysInSlot(addr string, slot int) (int, error) {
	inner := func(addr string, slot int) (int, error) {
		conn, err := dial(addr)
		if err != nil {
			return 0, ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.Int(conn.Do("cluster", "countkeysinslot", slot))
		if err != nil {
			return 0, err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp int
	for retry > 0 {
		resp, err = inner(addr, slot)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return 0, err
}

func GetKeysInSlot(addr string, slot, num int) ([]string, error) {
	inner := func(addr string, slot, num int) ([]string, error) {
		conn, err := dial(addr)
		if err != nil {
			return nil, ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.Strings(conn.Do("cluster", "getkeysinslot", slot, num))
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp []string
	for retry > 0 {
		resp, err = inner(addr, slot, num)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return nil, err
}

func Migrate(addr, toIp string, toPort int, key string, timeout int) (string, error) {
	inner := func(addr, toIp string, toPort int, key string, timeout int) (string, error) {
		conn, err := dial(addr)
		if err != nil {
			return "", ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("migrate", toIp, toPort, key, 0, timeout))
		if err != nil && strings.Contains(err.Error(), "BUSYKEY") {
			log.Warningf("Migrate", "Found BUSYKEY '%s', will overwrite it.", key)
			resp, err = redis.String(conn.Do("migrate", toIp, toPort, key, 0, timeout, "replace"))
		}
		if err != nil {
			return "", err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp string
	for retry > 0 {
		resp, err = inner(addr, toIp, toPort, key, timeout)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return "", err
}

func MigrateByMultiKeys(addr, toIp string, toPort int, key []string, timeout int) (string, error) {
	inner := func(addr, toIp string, toPort int, key []string, timeout int) (string, error) {
		if len(key) == 0 {
			return "", nil
		}
		conn, err := dial(addr)
		if err != nil {
			return "", ErrConnFailed
		}
		defer conn.Close()

		_joinArgs := func(toIp string, toPort int, keys []string, timeout int, replace bool) []interface{} {
			var args []interface{}
			args = append(args, toIp)
			args = append(args, toPort)
			args = append(args, "")
			args = append(args, 0)
			args = append(args, timeout)
			if replace {
				args = append(args, "replace")
			}
			args = append(args, "keys")
			for _, key := range keys {
				args = append(args, key)
			}

			return args
		}
		migrateArgs := _joinArgs(toIp, toPort, key, timeout, false)

		resp, err := redis.String(conn.Do("migrate", migrateArgs...))
		if err != nil && strings.Contains(err.Error(), "BUSYKEY") {
			log.Warningf("MigrateByMultiKeys", "Found BUSYKEYS '%s', will overwrite it.", strings.Join(key, " "))
			migrateArgs := _joinArgs(toIp, toPort, key, timeout, true)
			resp, err = redis.String(conn.Do("migrate", migrateArgs...))
		}
		if err != nil {
			return "", err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp string
	for retry > 0 {
		resp, err = inner(addr, toIp, toPort, key, timeout)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return "", err
}

// used by cli
func ClusterNodesWithoutExtra(addr string) (string, error) {
	inner := func(addr string) (string, error) {
		conn, err := dial(addr)
		if err != nil {
			return "", ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("cluster", "nodes"))
		if err != nil {
			return "", err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp string
	for retry > 0 {
		resp, err = inner(addr)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return "", err
}

func AddSlotRange(addr string, start, end int) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "connect failed", ErrConnFailed
	}
	defer conn.Close()
	var resp string
	for i := start; i <= end; i++ {
		resp, err = redis.String(conn.Do("cluster", "addslots", i))
		if err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func FlushAll(addr string) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "connect failed", ErrConnFailed
	}
	defer conn.Close()
	resp, err := redis.String(conn.Do("flushall"))
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func Slot2Node(addr string, slot int, dest string) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "connect failed", ErrConnFailed
	}
	defer conn.Close()
	resp, err := redis.String(conn.Do("slot2node", slot, dest))
	if err != nil {
		return resp, err
	}
	return resp, nil
}

//执行cli命令
func RedisCli(addr string, cmd string, args ...interface{}) (interface{}, error) {
	conn, err := dial(addr)
	if err != nil {
		return "connect failed", ErrConnFailed
	}
	defer conn.Close()
	reply, err := conn.Do(cmd, args...)
	if err != nil {
		return redis.String(reply, err)
	}
	switch reply.(type) {
	case []interface{}:
		rs, _ := redis.Strings(reply, nil)
		return rs, err
	default:
		return redis.String(reply, nil)
	}
}
