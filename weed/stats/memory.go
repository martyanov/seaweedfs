package stats

import (
	"runtime"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
)

func MemStat() *volume_server_pb.MemStatus {
	mem := &volume_server_pb.MemStatus{}
	mem.Goroutines = int32(runtime.NumGoroutine())
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	mem.Self = memStat.Alloc
	mem.Heap = memStat.HeapAlloc
	mem.Stack = memStat.StackInuse

	fillInMemStatus(mem)
	return mem
}

func fillInMemStatus(mem *volume_server_pb.MemStatus) {
	//system memory usage
	sysInfo := new(syscall.Sysinfo_t)
	err := syscall.Sysinfo(sysInfo)
	if err == nil {
		mem.All = uint64(sysInfo.Totalram) //* uint64(syscall.Getpagesize())
		mem.Free = uint64(sysInfo.Freeram) //* uint64(syscall.Getpagesize())
		mem.Used = mem.All - mem.Free
	}
}
