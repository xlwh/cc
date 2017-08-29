package topo

import (
	"fmt"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
)

//记录Slot的起始点
type Range struct {
	Left  int
	Right int
}

//range信息转换成为字符串返回
//调用Sprintf输出
func (r Range) String() string {
	if r.Left == r.Right {
		return fmt.Sprint(r.Left)
	}
	return fmt.Sprintf("%d-%d", r.Left, r.Right)
}

//slots数目
func (r Range) NumSlots() int {
	return (r.Right - r.Left + 1)
}

//在一个node上，可能有多个ranges
//用一个数组表示
type Ranges []Range

//将所有的range字符串输出
func (rs Ranges) String() string {
	slots := ""
	//遍历每个range调用String方法输出
	for _, r := range rs {
		slots += r.String() + ","
	}
	if len(slots) > 0 {
		return slots[:len(slots)-1]
	}
	return slots
}

//所有range上的slot的数目
func (rs Ranges) NumSlots() int {
	sum := 0
	for _, r := range rs {
		sum += r.NumSlots()
	}
	return sum
}

//节点的信息
type SummaryInfo struct {
	UsedMemory              int64  //内存使用
	Keys                    int64  //key数量
	Expires                 int64  //过期key数量
	MasterLinkStatus        string //主链接状态
	MasterSyncLeftBytes     int64
	ReplOffset              int64
	Loading                 bool    //节点负载
	RdbBgsaveInProgress     bool    //RDB进度
	InstantaneousOpsPerSec  int     //每秒的操作
	InstantaneousInputKbps  float64 //入口网卡占用
	InstantaneousOutputKbps float64 //出口网卡占用
}

//按行读取，设置SummaryInfo
//k:v
func (s *SummaryInfo) ReadLine(line string) {
	xs := strings.Split(strings.TrimSpace(line), " ")
	xs = strings.Split(xs[1], ":")
	s.SetField(xs[0], xs[1])
}

//解析k:v，设置到Summary中去
func (s *SummaryInfo) SetField(key, val string) {
	switch key {
	case "used_memory":
		v, _ := strconv.ParseInt(val, 10, 64)
		s.UsedMemory = v
	case "db0_keys":
		v, _ := strconv.ParseInt(val, 10, 64)
		s.Keys = v
	case "db0_expires":
		v, _ := strconv.ParseInt(val, 10, 64)
		s.Expires = v
	case "master_link_status":
		s.MasterLinkStatus = val
	case "master_sync_left_bytes":
		v, _ := strconv.ParseInt(val, 10, 64)
		s.MasterSyncLeftBytes = v
	case "repl_offset":
		v, _ := strconv.ParseInt(val, 10, 64)
		s.ReplOffset = v
	case "loading":
		s.Loading = (val == "1")
	case "rdb_bgsave_in_progress":
		s.RdbBgsaveInProgress = (val == "1")
	case "instantaneous_ops_per_sec":
		v, _ := strconv.Atoi(val)
		s.InstantaneousOpsPerSec = v
	case "instantaneous_input_kbps":
		v, _ := strconv.ParseFloat(val, 64)
		s.InstantaneousInputKbps = v
	case "instantaneous_output_kbps":
		v, _ := strconv.ParseFloat(val, 64)
		s.InstantaneousOutputKbps = v
	}
}

//Redis节点的数据结构描述
type Node struct {
	Ip          string           //节点ip
	Port        int              //节点端口
	Id          string           //节点id
	ParentId    string           //父节点id
	Migrating   map[string][]int //迁移节点，故障的节点列表string:int[]
	Importing   map[string][]int //迁入节点列表string:int[]
	Readable    bool             //是否可读
	Writable    bool             //是否可写
	PFail       bool             //是否是主观故障
	Fail        bool             //是否处于故障状态
	Free        bool             //是否是游离于集群之外的节点（ClusterNodes信息里只有一个节点，主，无slots）
	Role        string           //节点角色
	Tag         string           //节点Tag
	Region      string           //节点所在地域
	Zone        string           //节点区域
	Room        string           //节点物理机房(jx\bjyz\gzns)
	Ranges      []Range
	FailCount   int    //挂掉的节点的数量
	hostname    string //机器名
	SummaryInfo        //总的信息
	ClusterInfo        //集群信息
}

//给定string列表，创建node
//服务初始化的时候用到这个
//解析出ip和端口，调用node创建方法进行创建
func NewNodeFromString(addr string) *Node {
	xs := strings.Split(addr, ":")
	if len(xs) != 2 {
		return nil
	}
	port, err := strconv.Atoi(xs[1])
	if err != nil {
		return nil
	}
	if xs[0] == "" {
		xs[0] = "127.0.0.1"
	}
	return NewNode(xs[0], port)
}

//创建节点
func NewNode(ip string, port int) *Node {
	//正则判断是否是IP
	matched, _ := regexp.MatchString("\\d+\\.\\d+\\.\\d+\\.\\d+", ip)
	//如果不是ID，则会尝试解析一下
	if !matched {
		// 'ip' is a hostname
		ips, err := net.LookupIP(ip)
		if err != nil {
			panic("can not resolve address of " + ip)
		}
		ip = ips[0].String()
	}

	//创建node
	node := Node{
		Ip:        ip,
		Port:      port,
		Ranges:    []Range{},
		Migrating: map[string][]int{},
		Importing: map[string][]int{},
	}

	//返回创建好的node的地址
	return &node
}

//获取node的ip:port
func (s *Node) Addr() string {
	return fmt.Sprintf("%s:%d", s.Ip, s.Port)
}

//设置node的id
func (s *Node) SetId(id string) *Node {
	s.Id = id
	return s
}

//设置node的父id（不知道有什么作用呢?）
func (s *Node) SetParentId(pid string) *Node {
	s.ParentId = pid
	return s
}

//指定nodeID，添加迁移中的sloat
func (s *Node) AddMigrating(nodeId string, slot int) *Node {
	s.Migrating[nodeId] = append(s.Migrating[nodeId], slot)
	return s
}

//指定nodeID设定slot为迁入sloat
func (s *Node) AddImporting(nodeId string, slot int) *Node {
	s.Importing[nodeId] = append(s.Importing[nodeId], slot)
	return s
}

//设置node可读状态
func (s *Node) SetReadable(val bool) *Node {
	s.Readable = val
	return s
}

//设置node的可写状态
func (s *Node) SetWritable(val bool) *Node {
	s.Writable = val
	return s
}

//设置node主观Fail
func (s *Node) SetPFail(val bool) *Node {
	s.PFail = val
	return s
}

//设置节点fail
func (s *Node) SetFail(val bool) *Node {
	s.Fail = val
	return s
}

//读取PFailCount
func (s *Node) PFailCount() int {
	return s.FailCount
}

//PFailCount++
//可以让别的节点给这个节点投票的接口
func (s *Node) IncrPFailCount() {
	s.FailCount++
}

//判断这个节点是否是主节点
func (s *Node) IsMaster() bool {
	return s.Role == "master"
}

//这个节点是否是备用的Master
func (s *Node) IsStandbyMaster() bool {
	return (s.Role == "master" && s.Fail && len(s.Ranges) == 0)
}

//设置节点的角色
func (s *Node) SetRole(val string) *Node {
	s.Role = val
	return s
}

//设置Tag
func (s *Node) SetTag(val string) *Node {
	s.Tag = val
	return s
}

func (s *Node) SetRegion(val string) *Node {
	s.Region = val
	return s
}

func (s *Node) SetZone(val string) *Node {
	s.Zone = val
	return s
}

func (s *Node) SetFree(free bool) *Node {
	s.Free = free
	return s
}

func (s *Node) SetRoom(val string) *Node {
	s.Room = val
	return s
}

func (s *Node) AddRange(r Range) {
	s.Ranges = append(s.Ranges, r)
}

//判断这个节点是否是一个空的Node，sloat为空
func (s *Node) Empty() bool {
	return len(s.Ranges) == 0
}

//节点上的slot数目
func (s *Node) NumSlots() int {
	total := 0
	for _, r := range s.Ranges {
		total += r.NumSlots()
	}
	return total
}

//给定range数目，把所有的slot均匀分配一遍
//应该是需在适当的必要情况下，需要对slot重置
func (s *Node) RangesSplitN(n int) [][]Range {
	total := s.NumSlots()
	//计算出每个range上的slot数目
	numSlotsPerPart := int(math.Ceil(float64(total) / float64(n)))

	//保存每部分的slot的id
	parts := [][]Range{}
	//保存range
	ranges := []Range{}
	//每个range上需要放的node数目
	need := numSlotsPerPart

	//遍历当前node上的range，分别进行处理
	for i := len(s.Ranges) - 1; i >= 0; i-- {
		rang := s.Ranges[i]
		//range中的slot数目
		num := rang.NumSlots()

		//当前range上的slot比need少
		if need > num {
			need -= num //need = need - num
			ranges = append(ranges, rang)
			//相等
		} else if need == num {
			ranges = append(ranges, rang)
			parts = append(parts, ranges)

			//ranges重置为空
			ranges = []Range{}
			need = numSlotsPerPart

			//在range上的数目比预期的多了
		} else {
			ranges = append(ranges, Range{
				Left:  rang.Right - need + 1,
				Right: rang.Right,
			})
			parts = append(parts, ranges)
			remain := Range{rang.Left, rang.Right - need}
			need = numSlotsPerPart
			for remain.NumSlots()-numSlotsPerPart > 0 {
				ranges = []Range{Range{
					Left:  remain.Right - need + 1,
					Right: remain.Right,
				}}
				parts = append(parts, ranges)
				remain = Range{remain.Left, remain.Right - need}
			}
			ranges = []Range{remain}
			need = numSlotsPerPart - remain.NumSlots()
		}
	}

	if len(ranges) > 0 {
		parts = append(parts, ranges)
	}
	return parts
}

//比较两个node是否相同
func (s *Node) Compare(t *Node) bool {
	b := true
	b = b && (s.Port == t.Port)
	b = b && (s.ParentId == t.ParentId)
	b = b && (s.Readable == t.Readable)
	b = b && (s.Writable == t.Writable)
	b = b && (s.Role == t.Role)
	b = b && (s.Tag == t.Tag)
	return b
}

//获取节点的机器名
func (s *Node) Hostname() string {
	if s.hostname == "" {
		addr, err := net.LookupAddr(s.Ip)
		if len(addr) == 0 || err != nil {
			panic("unknown host for " + s.Ip)
		}
		s.hostname = strings.TrimSuffix(addr[0], ".baidu.com")
		s.hostname = strings.TrimSuffix(addr[0], ".baidu.com.")
	}
	return s.hostname
}

//获取机器的机房信息
func (s *Node) MachineRoom() string {
	xs := strings.Split(s.Hostname(), ".")
	if len(xs) != 2 {
		panic("invalid host name: " + s.Hostname())
	}
	return xs[1]
}

func (s *Node) String() string {
	return fmt.Sprintf("%s(%s)", s.Addr(), s.Id)
}

func (s *Node) IsArbiter() bool {
	return s.Role == "master" && strings.Contains(s.Tag, "Arbiter")
}
