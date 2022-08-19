package command

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func followUpdatesAndUploadToRemote(option *RemoteSyncOptions, filerSource *source.FilerSource, mountedDir string) error {

	// read filer remote storage mount mappings
	_, _, remoteStorageMountLocation, remoteStorage, detectErr := filer.DetectMountInfo(option.grpcDialOption, rpc.ServerAddress(*option.filerAddress), mountedDir)
	if detectErr != nil {
		return fmt.Errorf("read mount info: %v", detectErr)
	}

	eachEntryFunc, err := makeEventProcessor(remoteStorage, mountedDir, remoteStorageMountLocation, filerSource)
	if err != nil {
		return err
	}

	processEventFnWithOffset := rpc.AddOffsetFunc(eachEntryFunc, 3*time.Second, func(counter int64, lastTsNs int64) error {
		lastTime := time.Unix(0, lastTsNs)
		glog.V(0).Infof("remote sync %s progressed to %v %0.2f/sec", *option.filerAddress, lastTime, float64(counter)/float64(3))
		return remote_storage.SetSyncOffset(option.grpcDialOption, rpc.ServerAddress(*option.filerAddress), mountedDir, lastTsNs)
	})

	lastOffsetTs := collectLastSyncOffset(option, option.grpcDialOption, rpc.ServerAddress(*option.filerAddress), mountedDir, *option.timeAgo)

	option.clientEpoch++
	return rpc.FollowMetadata(rpc.ServerAddress(*option.filerAddress), option.grpcDialOption, "filer.remote.sync", option.clientId, option.clientEpoch,
		mountedDir, []string{filer.DirectoryEtcRemote}, lastOffsetTs.UnixNano(), 0, 0, processEventFnWithOffset, rpc.TrivialOnError)
}

func makeEventProcessor(remoteStorage *rpc.RemoteConfiguration, mountedDir string, remoteStorageMountLocation *rpc.RemoteStorageLocation, filerSource *source.FilerSource) (rpc.ProcessMetadataFunc, error) {
	client, err := remote_storage.GetRemoteStorage(remoteStorage)
	if err != nil {
		return nil, err
	}

	handleEtcRemoteChanges := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if message.NewEntry == nil {
			return nil
		}
		if message.NewEntry.Name == filer.REMOTE_STORAGE_MOUNT_FILE {
			mappings, readErr := filer.UnmarshalRemoteStorageMappings(message.NewEntry.Content)
			if readErr != nil {
				return fmt.Errorf("unmarshal mappings: %v", readErr)
			}
			if remoteLoc, found := mappings.Mappings[mountedDir]; found {
				if remoteStorageMountLocation.Bucket != remoteLoc.Bucket || remoteStorageMountLocation.Path != remoteLoc.Path {
					glog.Fatalf("Unexpected mount changes %+v => %+v", remoteStorageMountLocation, remoteLoc)
				}
			} else {
				glog.V(0).Infof("unmounted %s exiting ...", mountedDir)
				os.Exit(0)
			}
		}
		if message.NewEntry.Name == remoteStorage.Name+filer.REMOTE_STORAGE_CONF_SUFFIX {
			conf := &rpc.RemoteConfiguration{}
			if err := proto.Unmarshal(message.NewEntry.Content, conf); err != nil {
				return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, message.NewEntry.Name, err)
			}
			remoteStorage = conf
			if newClient, err := remote_storage.GetRemoteStorage(remoteStorage); err == nil {
				client = newClient
			} else {
				return err
			}
		}

		return nil
	}

	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if strings.HasPrefix(resp.Directory, filer.DirectoryEtcRemote) {
			return handleEtcRemoteChanges(resp)
		}

		if filer_pb.IsEmpty(resp) {
			return nil
		}
		if filer_pb.IsCreate(resp) {
			if !filer.HasData(message.NewEntry) {
				return nil
			}
			glog.V(2).Infof("create: %+v", resp)
			if !shouldSendToRemote(message.NewEntry) {
				glog.V(2).Infof("skipping creating: %+v", resp)
				return nil
			}
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.NewParentPath, message.NewEntry.Name), remoteStorageMountLocation)
			if message.NewEntry.IsDirectory {
				glog.V(0).Infof("mkdir  %s", remote_storage.FormatLocation(dest))
				return client.WriteDirectory(dest, message.NewEntry)
			}
			glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
			remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewEntry, dest)
			if writeErr != nil {
				return writeErr
			}
			return updateLocalEntry(&remoteSyncOptions, message.NewParentPath, message.NewEntry, remoteEntry)
		}
		if filer_pb.IsDelete(resp) {
			glog.V(2).Infof("delete: %+v", resp)
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
			if message.OldEntry.IsDirectory {
				glog.V(0).Infof("rmdir  %s", remote_storage.FormatLocation(dest))
				return client.RemoveDirectory(dest)
			}
			glog.V(0).Infof("delete %s", remote_storage.FormatLocation(dest))
			return client.DeleteFile(dest)
		}
		if message.OldEntry != nil && message.NewEntry != nil {
			oldDest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.NewParentPath, message.NewEntry.Name), remoteStorageMountLocation)
			if !shouldSendToRemote(message.NewEntry) {
				glog.V(2).Infof("skipping updating: %+v", resp)
				return nil
			}
			if message.NewEntry.IsDirectory {
				return client.WriteDirectory(dest, message.NewEntry)
			}
			if resp.Directory == message.NewParentPath && message.OldEntry.Name == message.NewEntry.Name {
				if filer.IsSameData(message.OldEntry, message.NewEntry) {
					glog.V(2).Infof("update meta: %+v", resp)
					return client.UpdateFileMetadata(dest, message.OldEntry, message.NewEntry)
				}
			}
			glog.V(2).Infof("update: %+v", resp)
			glog.V(0).Infof("delete %s", remote_storage.FormatLocation(oldDest))
			if err := client.DeleteFile(oldDest); err != nil {
				return err
			}
			remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewEntry, dest)
			if writeErr != nil {
				return writeErr
			}
			return updateLocalEntry(&remoteSyncOptions, message.NewParentPath, message.NewEntry, remoteEntry)
		}

		return nil
	}
	return eachEntryFunc, nil
}

func retriedWriteFile(client remote_storage.RemoteStorageClient, filerSource *source.FilerSource, newEntry *filer_pb.Entry, dest *rpc.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error) {
	var writeErr error
	err = util.Retry("writeFile", func() error {
		reader := filer.NewFileReader(filerSource, newEntry)
		glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
		remoteEntry, writeErr = client.WriteFile(dest, newEntry, reader)
		if writeErr != nil {
			return writeErr
		}
		return nil
	})
	if err != nil {
		glog.Errorf("write to %s: %v", dest, err)
	}
	return
}

func collectLastSyncOffset(filerClient filer_pb.FilerClient, grpcDialOption grpc.DialOption, filerAddress rpc.ServerAddress, mountedDir string, timeAgo time.Duration) time.Time {
	// 1. specified by timeAgo
	// 2. last offset timestamp for this directory
	// 3. directory creation time
	var lastOffsetTs time.Time
	if timeAgo == 0 {
		mountedDirEntry, err := filer_pb.GetEntry(filerClient, util.FullPath(mountedDir))
		if err != nil {
			glog.V(0).Infof("get mounted directory %s: %v", mountedDir, err)
			return time.Now()
		}

		lastOffsetTsNs, err := remote_storage.GetSyncOffset(grpcDialOption, filerAddress, mountedDir)
		if mountedDirEntry != nil {
			if err == nil && mountedDirEntry.Attributes.Crtime < lastOffsetTsNs/1000000 {
				lastOffsetTs = time.Unix(0, lastOffsetTsNs)
				glog.V(0).Infof("resume from %v", lastOffsetTs)
			} else {
				lastOffsetTs = time.Unix(mountedDirEntry.Attributes.Crtime, 0)
			}
		} else {
			lastOffsetTs = time.Now()
		}
	} else {
		lastOffsetTs = time.Now().Add(-timeAgo)
	}
	return lastOffsetTs
}

func toRemoteStorageLocation(mountDir, sourcePath util.FullPath, remoteMountLocation *rpc.RemoteStorageLocation) *rpc.RemoteStorageLocation {
	source := string(sourcePath[len(mountDir):])
	dest := util.FullPath(remoteMountLocation.Path).Child(source)
	return &rpc.RemoteStorageLocation{
		Name:   remoteMountLocation.Name,
		Bucket: remoteMountLocation.Bucket,
		Path:   string(dest),
	}
}

func shouldSendToRemote(entry *filer_pb.Entry) bool {
	if entry.RemoteEntry == nil {
		return true
	}
	if entry.RemoteEntry.RemoteMtime < entry.Attributes.Mtime {
		return true
	}
	return false
}

func updateLocalEntry(filerClient filer_pb.FilerClient, dir string, entry *filer_pb.Entry, remoteEntry *filer_pb.RemoteEntry) error {
	remoteEntry.LastLocalSyncTsNs = time.Now().UnixNano()
	entry.RemoteEntry = remoteEntry
	return filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
		return err
	})
}
