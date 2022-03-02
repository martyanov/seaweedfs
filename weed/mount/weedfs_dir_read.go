package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/mount/meta_cache"
	"github.com/hanwen/go-fuse/v2/fuse"
	"math"
	"sync"
)

type DirectoryHandleId uint64

type DirectoryHandle struct {
	isFinished    bool
	lastEntryName string
}

type DirectoryHandleToInode struct {
	// shares the file handle id sequencer with FileHandleToInode{nextFh}
	sync.Mutex
	dir2inode map[DirectoryHandleId]*DirectoryHandle
}

func NewDirectoryHandleToInode() *DirectoryHandleToInode {
	return &DirectoryHandleToInode{
		dir2inode: make(map[DirectoryHandleId]*DirectoryHandle),
	}
}

func (wfs *WFS) AcquireDirectoryHandle() (DirectoryHandleId, *DirectoryHandle) {
	wfs.fhmap.Lock()
	fh := wfs.fhmap.nextFh
	wfs.fhmap.nextFh++
	wfs.fhmap.Unlock()

	wfs.dhmap.Lock()
	defer wfs.dhmap.Unlock()
	dh := &DirectoryHandle{
		isFinished:    false,
		lastEntryName: "",
	}
	wfs.dhmap.dir2inode[DirectoryHandleId(fh)] = dh
	return DirectoryHandleId(fh), dh
}

func (wfs *WFS) GetDirectoryHandle(dhid DirectoryHandleId) *DirectoryHandle {
	wfs.dhmap.Lock()
	defer wfs.dhmap.Unlock()
	if dh, found := wfs.dhmap.dir2inode[dhid]; found {
		return dh
	}
	dh := &DirectoryHandle{
		isFinished:    false,
		lastEntryName: "",
	}

	wfs.dhmap.dir2inode[dhid] = dh
	return dh
}

func (wfs *WFS) ReleaseDirectoryHandle(dhid DirectoryHandleId) {
	wfs.dhmap.Lock()
	defer wfs.dhmap.Unlock()
	delete(wfs.dhmap.dir2inode, dhid)
}

// Directory handling

/** Open directory
 *
 * Unless the 'default_permissions' mount option is given,
 * this method should check if opendir is permitted for this
 * directory. Optionally opendir may also return an arbitrary
 * filehandle in the fuse_file_info structure, which will be
 * passed to readdir, releasedir and fsyncdir.
 */
func (wfs *WFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	if !wfs.inodeToPath.HasInode(input.NodeId) {
		return fuse.ENOENT
	}
	dhid, _ := wfs.AcquireDirectoryHandle()
	out.Fh = uint64(dhid)
	return fuse.OK
}

/** Release directory
 *
 * If the directory has been removed after the call to opendir, the
 * path parameter will be NULL.
 */
func (wfs *WFS) ReleaseDir(input *fuse.ReleaseIn) {
	wfs.ReleaseDirectoryHandle(DirectoryHandleId(input.Fh))
}

/** Synchronize directory contents
 *
 * If the directory has been removed after the call to opendir, the
 * path parameter will be NULL.
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 */
func (wfs *WFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

/** Read directory
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 */
func (wfs *WFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	return wfs.doReadDirectory(input, out, false)
}

func (wfs *WFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	return wfs.doReadDirectory(input, out, true)
}

func (wfs *WFS) doReadDirectory(input *fuse.ReadIn, out *fuse.DirEntryList, isPlusMode bool) fuse.Status {

	dh := wfs.GetDirectoryHandle(DirectoryHandleId(input.Fh))
	if dh.isFinished {
		if input.Offset == 0 {
			dh.isFinished = false
			dh.lastEntryName = ""
		} else {
			return fuse.OK
		}
	}

	isEarlyTerminated := false
	dirPath, code := wfs.inodeToPath.GetPath(input.NodeId)
	if code != fuse.OK {
		return code
	}

	var dirEntry fuse.DirEntry
	if input.Offset == 0 {
		if !isPlusMode {
			out.AddDirEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "."})
			out.AddDirEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".."})
		} else {
			out.AddDirLookupEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "."})
			out.AddDirLookupEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".."})
		}
	}

	processEachEntryFn := func(entry *filer.Entry, isLast bool) bool {
		dirEntry.Name = entry.Name()
		dirEntry.Mode = toSyscallMode(entry.Mode)
		if !isPlusMode {
			inode := wfs.inodeToPath.Lookup(dirPath.Child(dirEntry.Name), entry.Mode, len(entry.HardLinkId) > 0, entry.Inode, false)
			dirEntry.Ino = inode
			if !out.AddDirEntry(dirEntry) {
				isEarlyTerminated = true
				return false
			}
		} else {
			inode := wfs.inodeToPath.Lookup(dirPath.Child(dirEntry.Name), entry.Mode, len(entry.HardLinkId) > 0, entry.Inode, true)
			dirEntry.Ino = inode
			entryOut := out.AddDirLookupEntry(dirEntry)
			if entryOut == nil {
				isEarlyTerminated = true
				return false
			}
			if fh, found := wfs.fhmap.FindFileHandle(inode); found {
				glog.V(4).Infof("readdir opened file %s", dirPath.Child(dirEntry.Name))
				entry = filer.FromPbEntry(string(dirPath), fh.entry)
			}
			wfs.outputFilerEntry(entryOut, inode, entry)
		}
		dh.lastEntryName = entry.Name()
		return true
	}

	entryChan := make(chan *filer.Entry, 128)
	var err error
	go func() {
		if err = meta_cache.EnsureVisited(wfs.metaCache, wfs, dirPath, entryChan); err != nil {
			glog.Errorf("dir ReadDirAll %s: %v", dirPath, err)
		}
		close(entryChan)
	}()
	hasData := false
	for entry := range entryChan {
		hasData = true
		processEachEntryFn(entry, false)
	}
	if err != nil {
		return fuse.EIO
	}

	if !hasData {
		listErr := wfs.metaCache.ListDirectoryEntries(context.Background(), dirPath, dh.lastEntryName, false, int64(math.MaxInt32), func(entry *filer.Entry) bool {
			return processEachEntryFn(entry, false)
		})
		if listErr != nil {
			glog.Errorf("list meta cache: %v", listErr)
			return fuse.EIO
		}
	}

	if !isEarlyTerminated {
		dh.isFinished = true
	}

	return fuse.OK
}
