package remote_storage

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
)

const slash = "/"

func ParseLocationName(remote string) (locationName string) {
	remote = strings.TrimSuffix(remote, slash)
	parts := strings.SplitN(remote, slash, 2)
	if len(parts) >= 1 {
		return parts[0]
	}
	return
}

func parseBucketLocation(remote string) (loc *rpc.RemoteStorageLocation) {
	loc = &rpc.RemoteStorageLocation{}
	remote = strings.TrimSuffix(remote, slash)
	parts := strings.SplitN(remote, slash, 3)
	if len(parts) >= 1 {
		loc.Name = parts[0]
	}
	if len(parts) >= 2 {
		loc.Bucket = parts[1]
	}
	loc.Path = remote[len(loc.Name)+1+len(loc.Bucket):]
	if loc.Path == "" {
		loc.Path = slash
	}
	return
}

func parseNoBucketLocation(remote string) (loc *rpc.RemoteStorageLocation) {
	loc = &rpc.RemoteStorageLocation{}
	remote = strings.TrimSuffix(remote, slash)
	parts := strings.SplitN(remote, slash, 2)
	if len(parts) >= 1 {
		loc.Name = parts[0]
	}
	loc.Path = remote[len(loc.Name):]
	if loc.Path == "" {
		loc.Path = slash
	}
	return
}

func FormatLocation(loc *rpc.RemoteStorageLocation) string {
	if loc.Bucket == "" {
		return fmt.Sprintf("%s%s", loc.Name, loc.Path)
	}
	return fmt.Sprintf("%s/%s%s", loc.Name, loc.Bucket, loc.Path)
}

type VisitFunc func(dir string, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error

type Bucket struct {
	Name      string
	CreatedAt time.Time
}

type RemoteStorageClient interface {
	Traverse(loc *rpc.RemoteStorageLocation, visitFn VisitFunc) error
	ReadFile(loc *rpc.RemoteStorageLocation, offset int64, size int64) (data []byte, err error)
	WriteDirectory(loc *rpc.RemoteStorageLocation, entry *filer_pb.Entry) (err error)
	RemoveDirectory(loc *rpc.RemoteStorageLocation) (err error)
	WriteFile(loc *rpc.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error)
	UpdateFileMetadata(loc *rpc.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (err error)
	DeleteFile(loc *rpc.RemoteStorageLocation) (err error)
	ListBuckets() ([]*Bucket, error)
	CreateBucket(name string) (err error)
	DeleteBucket(name string) (err error)
}

type RemoteStorageClientMaker interface {
	Make(remoteConf *rpc.RemoteConf) (RemoteStorageClient, error)
	HasBucket() bool
}

type CachedRemoteStorageClient struct {
	*rpc.RemoteConfiguration
	RemoteStorageClient
}

var (
	RemoteStorageClientMakers = make(map[string]RemoteStorageClientMaker)
	remoteStorageClients      = make(map[string]CachedRemoteStorageClient)
	remoteStorageClientsLock  sync.Mutex
)

func GetAllRemoteStorageNames() string {
	var storageNames []string
	for k := range RemoteStorageClientMakers {
		storageNames = append(storageNames, k)
	}
	sort.Strings(storageNames)
	return strings.Join(storageNames, "|")
}

func GetRemoteStorageNamesHasBucket() string {
	var storageNames []string
	for k, m := range RemoteStorageClientMakers {
		if m.HasBucket() {
			storageNames = append(storageNames, k)
		}
	}
	sort.Strings(storageNames)
	return strings.Join(storageNames, "|")
}

func ParseRemoteLocation(remoteConfType string, remote string) (remoteStorageLocation *rpc.RemoteStorageLocation, err error) {
	maker, found := RemoteStorageClientMakers[remoteConfType]
	if !found {
		return nil, fmt.Errorf("remote storage type %s not found", remoteConfType)
	}

	if !maker.HasBucket() {
		return parseNoBucketLocation(remote), nil
	}
	return parseBucketLocation(remote), nil
}

func makeRemoteStorageClient(remoteConf *rpc.RemoteConf) (RemoteStorageClient, error) {
	maker, found := RemoteStorageClientMakers[remoteConf.Type]
	if !found {
		return nil, fmt.Errorf("remote storage type %s not found", remoteConf.Type)
	}
	return maker.Make(remoteConf)
}

func GetRemoteStorage(remoteConf *rpc.RemoteConfiguration) (RemoteStorageClient, error) {
	remoteStorageClientsLock.Lock()
	defer remoteStorageClientsLock.Unlock()

	existingRemoteStorageClient, found := remoteStorageClients[remoteConf.Name]
	if found && proto.Equal(existingRemoteStorageClient.RemoteConf, remoteConf) {
		return existingRemoteStorageClient.RemoteStorageClient, nil
	}

	newRemoteStorageClient, err := makeRemoteStorageClient(remoteConf)
	if err != nil {
		return nil, fmt.Errorf("make remote storage client %s: %v", remoteConf.Name, err)
	}

	remoteStorageClients[remoteConf.Name] = CachedRemoteStorageClient{
		RemoteConf:          remoteConf,
		RemoteStorageClient: newRemoteStorageClient,
	}

	return newRemoteStorageClient, nil
}
