package weed_server

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func (ms *MasterServer) RaftListClusterServers(ctx context.Context, req *master_pb.RaftListClusterServersRequest) (*master_pb.RaftListClusterServersResponse, error) {
	resp := &master_pb.RaftListClusterServersResponse{}

	if ms.Topo.Raft == nil {
		return resp, nil
	}

	servers := ms.Topo.Raft.GetConfiguration().Configuration().Servers

	for _, server := range servers {
		resp.ClusterServers = append(resp.ClusterServers, &master_pb.RaftListClusterServersResponse_ClusterServers{
			Id:       string(server.ID),
			Address:  string(server.Address),
			Suffrage: server.Suffrage.String(),
		})
	}
	return resp, nil
}

func (ms *MasterServer) RaftAddServer(ctx context.Context, req *master_pb.RaftAddServerRequest) (*master_pb.RaftAddServerResponse, error) {
	resp := &master_pb.RaftAddServerResponse{}

	if ms.Topo.Raft == nil {
		return resp, nil
	}

	if ms.Topo.Raft.State() != raft.Leader {
		return nil, fmt.Errorf("raft add server %s failed: %s is no current leader", req.Id, ms.Topo.Raft.String())
	}

	var idxFuture raft.IndexFuture
	if req.Voter {
		idxFuture = ms.Topo.Raft.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(req.Address), 0, 0)
	} else {
		idxFuture = ms.Topo.Raft.AddNonvoter(raft.ServerID(req.Id), raft.ServerAddress(req.Address), 0, 0)
	}

	if err := idxFuture.Error(); err != nil {
		return nil, err
	}
	return resp, nil
}

func (ms *MasterServer) RaftRemoveServer(ctx context.Context, req *master_pb.RaftRemoveServerRequest) (*master_pb.RaftRemoveServerResponse, error) {
	resp := &master_pb.RaftRemoveServerResponse{}

	if ms.Topo.Raft == nil {
		return resp, nil
	}

	if ms.Topo.Raft.State() != raft.Leader {
		return nil, fmt.Errorf("raft remove server %s failed: %s is no current leader", req.Id, ms.Topo.Raft.String())
	}

	if !req.Force {
		ms.clientChansLock.RLock()
		_, ok := ms.clientChans[fmt.Sprintf("%s@%s", cluster.MasterType, req.Id)]
		ms.clientChansLock.RUnlock()
		if ok {
			return resp, fmt.Errorf("raft remove server %s failed: client connection to master exists", req.Id)
		}
	}

	idxFuture := ms.Topo.Raft.RemoveServer(raft.ServerID(req.Id), 0, 0)
	if err := idxFuture.Error(); err != nil {
		return nil, err
	}
	return resp, nil
}
