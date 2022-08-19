package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (entry *Entry) IsInRemoteOnly() bool {
	return len(entry.Chunks) == 0 && entry.Remote != nil && entry.Remote.RemoteSize > 0
}

func MapFullPathToRemoteStorageLocation(localMountedDir util.FullPath, remoteMountedLocation *rpc.RemoteStorageLocation, fp util.FullPath) *rpc.RemoteStorageLocation {
	remoteLocation := &rpc.RemoteStorageLocation{
		Name:   remoteMountedLocation.Name,
		Bucket: remoteMountedLocation.Bucket,
		Path:   remoteMountedLocation.Path,
	}
	remoteLocation.Path = string(util.FullPath(remoteLocation.Path).Child(string(fp)[len(localMountedDir):]))
	return remoteLocation
}

func MapRemoteStorageLocationPathToFullPath(localMountedDir util.FullPath, remoteMountedLocation *rpc.RemoteStorageLocation, remoteLocationPath string) (fp util.FullPath) {
	return localMountedDir.Child(remoteLocationPath[len(remoteMountedLocation.Path):])
}

func CacheRemoteObjectToLocalCluster(filerClient filer_pb.FilerClient, remoteConf *rpc.RemoteConfiguration, remoteLocation *rpc.RemoteStorageLocation, parent util.FullPath, entry *filer_pb.Entry) error {
	return filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CacheRemoteObjectToLocalCluster(context.Background(), &filer_pb.CacheRemoteObjectToLocalClusterRequest{
			Directory: string(parent),
			Name:      entry.Name,
		})
		return err
	})
}
