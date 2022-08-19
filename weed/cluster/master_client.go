package cluster

import (
	"context"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/rpc/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
)

func ListExistingPeerUpdates(master rpc.ServerAddress, grpcDialOption grpc.DialOption, filerGroup string, clientType string) (existingNodes []*master_pb.ClusterNodeUpdate) {

	if grpcErr := rpc.WithMasterClient(false, master, grpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: clientType,
			FilerGroup: filerGroup,
		})

		glog.V(0).Infof("the cluster has %d %s\n", len(resp.ClusterNodes), clientType)
		for _, node := range resp.ClusterNodes {
			existingNodes = append(existingNodes, &master_pb.ClusterNodeUpdate{
				NodeType:    FilerType,
				Address:     node.Address,
				IsLeader:    node.IsLeader,
				IsAdd:       true,
				CreatedAtNs: node.CreatedAtNs,
			})
		}
		return err
	}); grpcErr != nil {
		glog.V(0).Infof("connect to %s: %v", master, grpcErr)
	}
	return
}
