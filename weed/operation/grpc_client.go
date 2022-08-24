package operation

import (
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
)

func WithVolumeServerClient(streamingMode bool, volumeServer rpc.ServerAddress, grpcDialOption grpc.DialOption, fn func(volume_server_pb.VolumeServerClient) error) error {

	return rpc.WithGrpcClient(streamingMode, func(grpcConnection *grpc.ClientConn) error {
		client := volume_server_pb.NewVolumeServerClient(grpcConnection)
		return fn(client)
	}, volumeServer.ToGrpcAddress(), false, grpcDialOption)

}

func WithMasterServerClient(streamingMode bool, masterServer rpc.ServerAddress, grpcDialOption grpc.DialOption, fn func(masterClient master_pb.SeaweedClient) error) error {

	return rpc.WithGrpcClient(streamingMode, func(grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(client)
	}, masterServer.ToGrpcAddress(), false, grpcDialOption)

}
