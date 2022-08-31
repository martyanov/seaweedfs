package command

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type MetadataProcessor struct {
	activeJobs           map[int64]*filer_pb.SubscribeMetadataResponse
	activeJobsLock       sync.Mutex
	activeJobsCond       *sync.Cond
	concurrencyLimit     int
	fn                   rpc.ProcessMetadataFunc
	processedTsWatermark int64
}

func NewMetadataProcessor(fn rpc.ProcessMetadataFunc, concurrency int) *MetadataProcessor {
	t := &MetadataProcessor{
		fn:               fn,
		activeJobs:       make(map[int64]*filer_pb.SubscribeMetadataResponse),
		concurrencyLimit: concurrency,
	}
	t.activeJobsCond = sync.NewCond(&t.activeJobsLock)
	return t
}

func (t *MetadataProcessor) AddSyncJob(resp *filer_pb.SubscribeMetadataResponse) {
	if filer_pb.IsEmpty(resp) {
		return
	}

	t.activeJobsLock.Lock()
	defer t.activeJobsLock.Unlock()

	for len(t.activeJobs) >= t.concurrencyLimit || t.conflictsWith(resp) {
		t.activeJobsCond.Wait()
	}
	t.activeJobs[resp.TsNs] = resp
	go func() {

		util.RetryForever("metadata processor", func() error {
			return t.fn(resp)
		}, func(err error) bool {
			glog.Errorf("process %v: %v", resp, err)
			return true
		})

		t.activeJobsLock.Lock()
		defer t.activeJobsLock.Unlock()

		delete(t.activeJobs, resp.TsNs)

		// if is the oldest job, write down the watermark
		isOldest := true
		for t, _ := range t.activeJobs {
			if resp.TsNs > t {
				isOldest = false
				break
			}
		}
		if isOldest {
			t.processedTsWatermark = resp.TsNs
		}
		t.activeJobsCond.Signal()
	}()
}

func (t *MetadataProcessor) conflictsWith(resp *filer_pb.SubscribeMetadataResponse) bool {
	for _, r := range t.activeJobs {
		if shouldWaitFor(resp, r) {
			return true
		}
	}
	return false
}

// a is one possible job to schedule
// b is one existing active job
func shouldWaitFor(a *filer_pb.SubscribeMetadataResponse, b *filer_pb.SubscribeMetadataResponse) bool {
	aPath, aNewPath, aIsDirectory := extractPathsFromMetadata(a)
	bPath, bNewPath, bIsDirectory := extractPathsFromMetadata(b)

	if pairShouldWaitFor(aPath, bPath, aIsDirectory, bIsDirectory) {
		return true
	}
	if aNewPath != "" {
		if pairShouldWaitFor(aNewPath, bPath, aIsDirectory, bIsDirectory) {
			return true
		}
	}
	if bNewPath != "" {
		if pairShouldWaitFor(aPath, bNewPath, aIsDirectory, bIsDirectory) {
			return true
		}
	}
	if aNewPath != "" && bNewPath != "" {
		if pairShouldWaitFor(aNewPath, bNewPath, aIsDirectory, bIsDirectory) {
			return true
		}
	}
	return false
}

func pairShouldWaitFor(aPath, bPath util.FullPath, aIsDirectory, bIsDirectory bool) bool {
	if bIsDirectory {
		if aIsDirectory {
			return aPath.IsUnder(bPath) || bPath.IsUnder(aPath)
		} else {
			return aPath.IsUnder(bPath)
		}
	} else {
		if aIsDirectory {
			return bPath.IsUnder(aPath)
		} else {
			return aPath == bPath
		}
	}
}

func extractPathsFromMetadata(resp *filer_pb.SubscribeMetadataResponse) (path, newPath util.FullPath, isDirectory bool) {
	oldEntry := resp.EventNotification.OldEntry
	newEntry := resp.EventNotification.NewEntry
	// create
	if filer_pb.IsCreate(resp) {
		path = util.FullPath(resp.Directory).Child(newEntry.Name)
		isDirectory = newEntry.IsDirectory
		return
	}
	if filer_pb.IsDelete(resp) {
		path = util.FullPath(resp.Directory).Child(oldEntry.Name)
		isDirectory = oldEntry.IsDirectory
		return
	}
	if filer_pb.IsUpdate(resp) {
		path = util.FullPath(resp.Directory).Child(newEntry.Name)
		isDirectory = newEntry.IsDirectory
		return
	}
	// renaming
	path = util.FullPath(resp.Directory).Child(oldEntry.Name)
	isDirectory = oldEntry.IsDirectory
	newPath = util.FullPath(resp.EventNotification.NewParentPath).Child(newEntry.Name)
	return
}
