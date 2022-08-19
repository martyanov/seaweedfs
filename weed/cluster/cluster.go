package cluster

import (
	"math"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/rpc/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
)

const (
	MasterType       = "master"
	VolumeServerType = "volumeServer"
	FilerType        = "filer"
)

type FilerGroupName string
type DataCenter string
type Rack string

type Leaders struct {
	leaders [3]rpc.ServerAddress
}
type ClusterNode struct {
	Address    rpc.ServerAddress
	Version    string
	counter    int
	CreatedTs  time.Time
	DataCenter DataCenter
	Rack       Rack
}
type GroupMembers struct {
	members map[rpc.ServerAddress]*ClusterNode
	leaders *Leaders
}
type ClusterNodeGroups struct {
	groupMembers map[FilerGroupName]*GroupMembers
	sync.RWMutex
}
type Cluster struct {
	filerGroups *ClusterNodeGroups
}

func newClusterNodeGroups() *ClusterNodeGroups {
	return &ClusterNodeGroups{
		groupMembers: map[FilerGroupName]*GroupMembers{},
	}
}
func (g *ClusterNodeGroups) getGroupMembers(filerGroup FilerGroupName, createIfNotFound bool) *GroupMembers {
	members, found := g.groupMembers[filerGroup]
	if !found && createIfNotFound {
		members = &GroupMembers{
			members: make(map[rpc.ServerAddress]*ClusterNode),
			leaders: &Leaders{},
		}
		g.groupMembers[filerGroup] = members
	}
	return members
}

func (m *GroupMembers) addMember(dataCenter DataCenter, rack Rack, address rpc.ServerAddress, version string) *ClusterNode {
	if existingNode, found := m.members[address]; found {
		existingNode.counter++
		return nil
	}
	t := &ClusterNode{
		Address:    address,
		Version:    version,
		counter:    1,
		CreatedTs:  time.Now(),
		DataCenter: dataCenter,
		Rack:       rack,
	}
	m.members[address] = t
	return t
}
func (m *GroupMembers) removeMember(address rpc.ServerAddress) bool {
	if existingNode, found := m.members[address]; !found {
		return false
	} else {
		existingNode.counter--
		if existingNode.counter <= 0 {
			delete(m.members, address)
			return true
		}
	}
	return false
}

func (g *ClusterNodeGroups) AddClusterNode(filerGroup FilerGroupName, nodeType string, dataCenter DataCenter, rack Rack, address rpc.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, true)
	if t := m.addMember(dataCenter, rack, address, version); t != nil {
		return ensureGroupLeaders(m, true, filerGroup, nodeType, address)
	}
	return nil
}
func (g *ClusterNodeGroups) RemoveClusterNode(filerGroup FilerGroupName, nodeType string, address rpc.ServerAddress) []*master_pb.KeepConnectedResponse {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, false)
	if m == nil {
		return nil
	}
	if m.removeMember(address) {
		return ensureGroupLeaders(m, false, filerGroup, nodeType, address)
	}
	return nil
}
func (g *ClusterNodeGroups) ListClusterNode(filerGroup FilerGroupName) (nodes []*ClusterNode) {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, false)
	if m == nil {
		return nil
	}
	for _, node := range m.members {
		nodes = append(nodes, node)
	}
	return
}
func (g *ClusterNodeGroups) IsOneLeader(filerGroup FilerGroupName, address rpc.ServerAddress) bool {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, false)
	if m == nil {
		return false
	}
	return m.leaders.isOneLeader(address)
}
func (g *ClusterNodeGroups) ListClusterNodeLeaders(filerGroup FilerGroupName) (nodes []rpc.ServerAddress) {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, false)
	if m == nil {
		return nil
	}
	return m.leaders.GetLeaders()
}

func NewCluster() *Cluster {
	return &Cluster{
		filerGroups: newClusterNodeGroups(),
	}
}

func (cluster *Cluster) getGroupMembers(filerGroup FilerGroupName, nodeType string, createIfNotFound bool) *GroupMembers {
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.getGroupMembers(filerGroup, createIfNotFound)
	}
	return nil
}

func (cluster *Cluster) AddClusterNode(ns, nodeType string, dataCenter DataCenter, rack Rack, address rpc.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroupName(ns)
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.AddClusterNode(filerGroup, nodeType, dataCenter, rack, address, version)
	case MasterType:
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    true,
				},
			},
		}
	}
	return nil
}

func (cluster *Cluster) RemoveClusterNode(ns string, nodeType string, address rpc.ServerAddress) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroupName(ns)
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.RemoveClusterNode(filerGroup, nodeType, address)
	case MasterType:
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    false,
				},
			},
		}
	}
	return nil
}

func (cluster *Cluster) ListClusterNode(filerGroup FilerGroupName, nodeType string) (nodes []*ClusterNode) {
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.ListClusterNode(filerGroup)
	case MasterType:
	}
	return
}

func (cluster *Cluster) ListClusterNodeLeaders(filerGroup FilerGroupName, nodeType string) (nodes []rpc.ServerAddress) {
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.ListClusterNodeLeaders(filerGroup)
	case MasterType:
	}
	return
}

func (cluster *Cluster) IsOneLeader(filerGroup FilerGroupName, nodeType string, address rpc.ServerAddress) bool {
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.IsOneLeader(filerGroup, address)
	case MasterType:
	}
	return false
}

func ensureGroupLeaders(m *GroupMembers, isAdd bool, filerGroup FilerGroupName, nodeType string, address rpc.ServerAddress) (result []*master_pb.KeepConnectedResponse) {
	if isAdd {
		if m.leaders.addLeaderIfVacant(address) {
			// has added the address as one leader
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   true,
					IsAdd:      true,
				},
			})
		} else {
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   false,
					IsAdd:      true,
				},
			})
		}
	} else {
		if m.leaders.removeLeaderIfExists(address) {

			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   true,
					IsAdd:      false,
				},
			})

			// pick the freshest one, since it is less likely to go away
			var shortestDuration int64 = math.MaxInt64
			now := time.Now()
			var candidateAddress rpc.ServerAddress
			for _, node := range m.members {
				if m.leaders.isOneLeader(node.Address) {
					continue
				}
				duration := now.Sub(node.CreatedTs).Nanoseconds()
				if duration < shortestDuration {
					shortestDuration = duration
					candidateAddress = node.Address
				}
			}
			if candidateAddress != "" {
				m.leaders.addLeaderIfVacant(candidateAddress)
				// added a new leader
				result = append(result, &master_pb.KeepConnectedResponse{
					ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
						NodeType: nodeType,
						Address:  string(candidateAddress),
						IsLeader: true,
						IsAdd:    true,
					},
				})
			}
		} else {
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   false,
					IsAdd:      false,
				},
			})
		}
	}
	return
}

func (leaders *Leaders) addLeaderIfVacant(address rpc.ServerAddress) (hasChanged bool) {
	if leaders.isOneLeader(address) {
		return
	}
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == "" {
			leaders.leaders[i] = address
			hasChanged = true
			return
		}
	}
	return
}
func (leaders *Leaders) removeLeaderIfExists(address rpc.ServerAddress) (hasChanged bool) {
	if !leaders.isOneLeader(address) {
		return
	}
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == address {
			leaders.leaders[i] = ""
			hasChanged = true
			return
		}
	}
	return
}
func (leaders *Leaders) isOneLeader(address rpc.ServerAddress) bool {
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == address {
			return true
		}
	}
	return false
}
func (leaders *Leaders) GetLeaders() (addresses []rpc.ServerAddress) {
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] != "" {
			addresses = append(addresses, leaders.leaders[i])
		}
	}
	return
}
