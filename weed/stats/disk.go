package stats

import (
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
)

func NewDiskStatus(path string) (disk *volume_server_pb.DiskStatus) {
	disk = &volume_server_pb.DiskStatus{Dir: path}
	fillInDiskStatus(disk)
	if disk.PercentUsed > 95 {
		glog.V(0).Infof("disk status: %v", disk)
	}
	return
}

func fillInDiskStatus(disk *volume_server_pb.DiskStatus) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(disk.Dir, &fs)
	if err != nil {
		return
	}
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	disk.PercentFree = float32((float64(disk.Free) / float64(disk.All)) * 100)
	disk.PercentUsed = float32((float64(disk.Used) / float64(disk.All)) * 100)
	return
}
