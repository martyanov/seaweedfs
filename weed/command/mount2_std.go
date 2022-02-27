//go:build linux || darwin
// +build linux darwin

package command

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/mount"
	"github.com/chrislusf/seaweedfs/weed/mount/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/mount/unmount"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/hanwen/go-fuse/v2/fuse"
	"net/http"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/grace"
)

func runMount2(cmd *Command, args []string) bool {

	if *mount2Options.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *mount2Options.debugPort), nil)
	}

	grace.SetupProfiling(*mountCpuProfile, *mountMemProfile)
	if *mountReadRetryTime < time.Second {
		*mountReadRetryTime = time.Second
	}
	util.RetryWaitTime = *mountReadRetryTime

	umask, umaskErr := strconv.ParseUint(*mount2Options.umaskString, 8, 64)
	if umaskErr != nil {
		fmt.Printf("can not parse umask %s", *mount2Options.umaskString)
		return false
	}

	if len(args) > 0 {
		return false
	}

	return RunMount2(&mount2Options, os.FileMode(umask))
}

func RunMount2(option *Mount2Options, umask os.FileMode) bool {

	// basic checks
	chunkSizeLimitMB := *mount2Options.chunkSizeLimitMB
	if chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.")
		return false
	}

	// try to connect to filer
	filerAddresses := pb.ServerAddresses(*option.filer).ToAddresses()
	util.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	var cipher bool
	var err error
	for i := 0; i < 10; i++ {
		err = pb.WithOneOfGrpcFilerClients(false, filerAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer grpc address %v configuration: %v", filerAddresses, err)
			}
			cipher = resp.Cipher
			return nil
		})
		if err != nil {
			glog.V(0).Infof("failed to talk to filer %v: %v", filerAddresses, err)
			glog.V(0).Infof("wait for %d seconds ...", i+1)
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}
	if err != nil {
		glog.Errorf("failed to talk to filer %v: %v", filerAddresses, err)
		return true
	}

	filerMountRootPath := *option.filerMountRootPath

	// clean up mount point
	dir := util.ResolvePath(*option.dir)
	if dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}

	unmount.Unmount(dir)

	// detect mount folder mode
	if *option.dirAutoCreate {
		os.MkdirAll(dir, os.FileMode(0777)&^umask)
	}
	fileInfo, err := os.Stat(dir)

	// collect uid, gid
	uid, gid := uint32(0), uint32(0)
	mountMode := os.ModeDir | 0777
	if err == nil {
		mountMode = os.ModeDir | os.FileMode(0777)&^umask
		uid, gid = util.GetFileUidGid(fileInfo)
		fmt.Printf("mount point owner uid=%d gid=%d mode=%s\n", uid, gid, mountMode)
	} else {
		fmt.Printf("can not stat %s\n", dir)
		return false
	}

	// detect uid, gid
	if uid == 0 {
		if u, err := user.Current(); err == nil {
			if parsedId, pe := strconv.ParseUint(u.Uid, 10, 32); pe == nil {
				uid = uint32(parsedId)
			}
			if parsedId, pe := strconv.ParseUint(u.Gid, 10, 32); pe == nil {
				gid = uint32(parsedId)
			}
			fmt.Printf("current uid=%d gid=%d\n", uid, gid)
		}
	}

	// mapping uid, gid
	uidGidMapper, err := meta_cache.NewUidGidMapper(*option.uidMap, *option.gidMap)
	if err != nil {
		fmt.Printf("failed to parse %s %s: %v\n", *option.uidMap, *option.gidMap, err)
		return false
	}

	// Ensure target mount point availability
	if isValid := checkMountPointAvailable(dir); !isValid {
		glog.Fatalf("Expected mount to still be active, target mount point: %s, please check!", dir)
		return true
	}

	// mount fuse
	fuseMountOptions := &fuse.MountOptions{
		AllowOther:               *option.allowOthers,
		Options:                  nil,
		MaxBackground:            128,
		MaxWrite:                 1024 * 1024 * 2,
		MaxReadAhead:             1024 * 1024 * 2,
		IgnoreSecurityLabels:     false,
		RememberInodes:           false,
		FsName:                   *option.filer + ":" + filerMountRootPath,
		Name:                     "seaweedfs",
		SingleThreaded:           false,
		DisableXAttrs:            false,
		Debug:                    *option.debug,
		EnableLocks:              false,
		ExplicitDataCacheControl: false,
		DirectMount:              true,
		DirectMountFlags:         0,
		//SyncRead:                 false, // set to false to enable the FUSE_CAP_ASYNC_READ capability
		//EnableAcl:                true,
	}
	if *option.nonempty {
		fuseMountOptions.Options = append(fuseMountOptions.Options, "nonempty")
	}
	if *option.readOnly {
		if runtime.GOOS == "darwin" {
			fuseMountOptions.Options = append(fuseMountOptions.Options, "rdonly")
		} else {
			fuseMountOptions.Options = append(fuseMountOptions.Options, "ro")
		}
	}
	if runtime.GOOS == "darwin" {
		// https://github-wiki-see.page/m/macfuse/macfuse/wiki/Mount-Options
		ioSizeMB := 1
		for ioSizeMB*2 <= *option.chunkSizeLimitMB && ioSizeMB*2 <= 32 {
			ioSizeMB *= 2
		}
		fuseMountOptions.Options = append(fuseMountOptions.Options, "daemon_timeout=600")
		fuseMountOptions.Options = append(fuseMountOptions.Options, "noapplexattr")
		// fuseMountOptions.Options = append(fuseMountOptions.Options, "novncache") // need to test effectiveness
		fuseMountOptions.Options = append(fuseMountOptions.Options, "slow_statfs")
		fuseMountOptions.Options = append(fuseMountOptions.Options, "volname="+*option.filer)
		fuseMountOptions.Options = append(fuseMountOptions.Options, fmt.Sprintf("iosize=%d", ioSizeMB*1024*1024))
	}

	// find mount point
	mountRoot := filerMountRootPath
	if mountRoot != "/" && strings.HasSuffix(mountRoot, "/") {
		mountRoot = mountRoot[0 : len(mountRoot)-1]
	}

	seaweedFileSystem := mount.NewSeaweedFileSystem(&mount.Option{
		MountDirectory:     dir,
		FilerAddresses:     filerAddresses,
		GrpcDialOption:     grpcDialOption,
		FilerMountRootPath: mountRoot,
		Collection:         *option.collection,
		Replication:        *option.replication,
		TtlSec:             int32(*option.ttlSec),
		DiskType:           types.ToDiskType(*option.diskType),
		ChunkSizeLimit:     int64(chunkSizeLimitMB) * 1024 * 1024,
		ConcurrentWriters:  *option.concurrentWriters,
		CacheDir:           *option.cacheDir,
		CacheSizeMB:        *option.cacheSizeMB,
		DataCenter:         *option.dataCenter,
		MountUid:           uid,
		MountGid:           gid,
		MountMode:          mountMode,
		MountCtime:         fileInfo.ModTime(),
		MountMtime:         time.Now(),
		Umask:              umask,
		VolumeServerAccess: *mount2Options.volumeServerAccess,
		Cipher:             cipher,
		UidGidMapper:       uidGidMapper,
	})

	server, err := fuse.NewServer(seaweedFileSystem, dir, fuseMountOptions)
	if err != nil {
		glog.Fatalf("Mount fail: %v", err)
	}
	grace.OnInterrupt(func() {
		unmount.Unmount(dir)
	})

	seaweedFileSystem.StartBackgroundTasks()

	fmt.Printf("This is SeaweedFS version %s %s %s\n", util.Version(), runtime.GOOS, runtime.GOARCH)

	server.Serve()

	return true
}
