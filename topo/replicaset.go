package topo

//主从结构存储
type ReplicaSet struct {
	Master *Node
	Slaves []*Node
}

//创建一个空的
func NewReplicaSet() *ReplicaSet {
	rs := &ReplicaSet{}
	return rs
}

//设置主节点
func (s *ReplicaSet) SetMaster(node *Node) {
	s.Master = node
}

//给主添加一个从
func (s *ReplicaSet) AddSlave(node *Node) {
	s.Slaves = append(s.Slaves, node)
}

func (s *ReplicaSet) AllNodes() []*Node {
	return append(s.Slaves, s.Master)
}

func (s *ReplicaSet) RegionNodes(region string) []*Node {
	nodes := []*Node{}
	for _, n := range s.AllNodes() {
		if n.Region == region {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (s *ReplicaSet) HasNode(nodeId string) bool {
	return s.FindNode(nodeId) != nil
}

func (s *ReplicaSet) FindNode(nodeId string) *Node {
	if nodeId == s.Master.Id {
		return s.Master
	}
	for _, node := range s.Slaves {
		if nodeId == node.Id {
			return node
		}
	}
	return nil
}

// 该RS是否覆盖所有Region
func (s *ReplicaSet) IsCoverAllRegions(regions []string) bool {
	regionMap := map[string]bool{}
	for _, r := range regions {
		regionMap[r] = false
	}
	for _, n := range s.AllNodes() {
		regionMap[n.Region] = true
	}
	for _, v := range regionMap {
		if v == false {
			return false
		}
	}
	return true
}
