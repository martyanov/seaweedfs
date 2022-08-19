package filer

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/viant/ptrie"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const REMOTE_STORAGE_CONF_SUFFIX = ".conf"
const REMOTE_STORAGE_MOUNT_FILE = "mount.mapping"

type FilerRemoteStorage struct {
	rules             ptrie.Trie
	storageNameToConf map[string]*rpc.RemoteConfiguration
}

func NewFilerRemoteStorage() (rs *FilerRemoteStorage) {
	rs = &FilerRemoteStorage{
		rules:             ptrie.New(),
		storageNameToConf: make(map[string]*rpc.RemoteConfiguration),
	}
	return rs
}

func (rs *FilerRemoteStorage) LoadRemoteStorageConfigurationsAndMapping(filer *Filer) (err error) {
	// execute this on filer

	limit := int64(math.MaxInt32)

	entries, _, err := filer.ListDirectoryEntries(context.Background(), DirectoryEtcRemote, "", false, limit, "", "", "")
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil
		}
		glog.Errorf("read remote storage %s: %v", DirectoryEtcRemote, err)
		return
	}

	for _, entry := range entries {
		if entry.Name() == REMOTE_STORAGE_MOUNT_FILE {
			if err := rs.loadRemoteStorageMountMapping(entry.Content); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), REMOTE_STORAGE_CONF_SUFFIX) {
			return nil
		}
		conf := &rpc.RemoteConfiguration{}
		if err := proto.Unmarshal(entry.Content, conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, entry.Name(), err)
		}
		rs.storageNameToConf[conf.Name] = conf
	}
	return nil
}

func (rs *FilerRemoteStorage) loadRemoteStorageMountMapping(data []byte) (err error) {
	mappings := &rpc.RemoteStorageMapping{}
	if err := proto.Unmarshal(data, mappings); err != nil {
		return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE, err)
	}
	for dir, storageLocation := range mappings.Mappings {
		rs.mapDirectoryToRemoteStorage(util.FullPath(dir), storageLocation)
	}
	return nil
}

func (rs *FilerRemoteStorage) mapDirectoryToRemoteStorage(dir util.FullPath, loc *rpc.RemoteStorageLocation) {
	rs.rules.Put([]byte(dir+"/"), loc)
}

func (rs *FilerRemoteStorage) FindMountDirectory(p util.FullPath) (mountDir util.FullPath, remoteLocation *rpc.RemoteStorageLocation) {
	rs.rules.MatchPrefix([]byte(p), func(key []byte, value interface{}) bool {
		mountDir = util.FullPath(string(key[:len(key)-1]))
		remoteLocation = value.(*rpc.RemoteStorageLocation)
		return true
	})
	return
}

func (rs *FilerRemoteStorage) FindRemoteStorageClient(p util.FullPath) (client remote_storage.RemoteStorageClient, remoteConf *rpc.RemoteConfiguration, found bool) {
	var storageLocation *rpc.RemoteStorageLocation
	rs.rules.MatchPrefix([]byte(p), func(key []byte, value interface{}) bool {
		storageLocation = value.(*rpc.RemoteStorageLocation)
		return true
	})

	if storageLocation == nil {
		found = false
		return
	}

	return rs.GetRemoteStorageClient(storageLocation.Name)
}

func (rs *FilerRemoteStorage) GetRemoteStorageClient(storageName string) (client remote_storage.RemoteStorageClient, remoteConf *rpc.RemoteConfiguration, found bool) {
	remoteConf, found = rs.storageNameToConf[storageName]
	if !found {
		return
	}

	var err error
	if client, err = remote_storage.GetRemoteStorage(remoteConf); err == nil {
		found = true
		return
	}
	return
}

func UnmarshalRemoteStorageMappings(oldContent []byte) (mappings *rpc.RemoteStorageMapping, err error) {
	mappings = &rpc.RemoteStorageMapping{
		Mappings: make(map[string]*rpc.RemoteStorageLocation),
	}
	if len(oldContent) > 0 {
		if err = proto.Unmarshal(oldContent, mappings); err != nil {
			glog.Warningf("unmarshal existing mappings: %v", err)
		}
	}
	return
}

func ReadRemoteStorageConf(grpcDialOption grpc.DialOption, filerAddress rpc.ServerAddress, storageName string) (conf *rpc.RemoteConfiguration, readErr error) {
	var oldContent []byte
	if readErr = rpc.WithFilerClient(false, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		oldContent, readErr = ReadInsideFiler(client, DirectoryEtcRemote, storageName+REMOTE_STORAGE_CONF_SUFFIX)
		return readErr
	}); readErr != nil {
		return nil, readErr
	}

	// unmarshal storage configuration
	conf = &rpc.RemoteConfiguration{}
	if unMarshalErr := proto.Unmarshal(oldContent, conf); unMarshalErr != nil {
		readErr = fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, storageName+REMOTE_STORAGE_CONF_SUFFIX, unMarshalErr)
		return
	}

	return
}

func DetectMountInfo(grpcDialOption grpc.DialOption, filerAddress rpc.ServerAddress, dir string) (*rpc.RemoteStorageMapping, string, *rpc.RemoteStorageLocation, *rpc.RemoteConfiguration, error) {

	mappings, listErr := ReadMountMappings(grpcDialOption, filerAddress)
	if listErr != nil {
		return nil, "", nil, nil, listErr
	}
	if dir == "" {
		return mappings, "", nil, nil, fmt.Errorf("need to specify '-dir' option")
	}

	var localMountedDir string
	var remoteStorageMountedLocation *rpc.RemoteStorageLocation
	for k, loc := range mappings.Mappings {
		if strings.HasPrefix(dir, k) {
			localMountedDir, remoteStorageMountedLocation = k, loc
		}
	}
	if localMountedDir == "" {
		return mappings, localMountedDir, remoteStorageMountedLocation, nil, fmt.Errorf("%s is not mounted", dir)
	}

	// find remote storage configuration
	remoteStorageConf, err := ReadRemoteStorageConf(grpcDialOption, filerAddress, remoteStorageMountedLocation.Name)
	if err != nil {
		return mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, err
	}

	return mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, nil
}
