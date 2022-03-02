package mount

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/hanwen/go-fuse/v2/fuse"
	"time"
)

/**
 * Flush method
 *
 * This is called on each close() of the opened file.
 *
 * Since file descriptors can be duplicated (dup, dup2, fork), for
 * one open call there may be many flush calls.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * NOTE: the name of the method is misleading, since (unlike
 * fsync) the filesystem is not forced to flush pending writes.
 * One reason to flush data is if the filesystem wants to return
 * write errors during close.  However, such use is non-portable
 * because POSIX does not require [close] to wait for delayed I/O to
 * complete.
 *
 * If the filesystem supports file locking operations (setlk,
 * getlk) it should remove all locks belonging to 'fi->owner'.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to flush() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 *
 * [close]: http://pubs.opengroup.org/onlinepubs/9699919799/functions/close.html
 */
func (wfs *WFS) Flush(cancel <-chan struct{}, in *fuse.FlushIn) fuse.Status {
	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return fuse.ENOENT
	}

	fh.Lock()
	defer fh.Unlock()

	return wfs.doFlush(fh, in.Uid, in.Gid)
}

/**
 * Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to fsync() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */
func (wfs *WFS) Fsync(cancel <-chan struct{}, in *fuse.FsyncIn) (code fuse.Status) {

	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return fuse.ENOENT
	}

	fh.Lock()
	defer fh.Unlock()

	return wfs.doFlush(fh, in.Uid, in.Gid)

}

func (wfs *WFS) doFlush(fh *FileHandle, uid, gid uint32) fuse.Status {
	// flush works at fh level
	fileFullPath := fh.FullPath()
	dir, name := fileFullPath.DirAndName()
	// send the data to the OS
	glog.V(4).Infof("doFlush %s fh %d", fileFullPath, fh.handle)

	if err := fh.dirtyPages.FlushData(); err != nil {
		glog.Errorf("%v doFlush: %v", fileFullPath, err)
		return fuse.EIO
	}

	if !fh.dirtyMetadata {
		return fuse.OK
	}

	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		entry := fh.entry
		if entry == nil {
			return nil
		}
		entry.Name = name // this flush may be just after a rename operation

		if entry.Attributes != nil {
			entry.Attributes.Mime = fh.contentType
			if entry.Attributes.Uid == 0 {
				entry.Attributes.Uid = uid
			}
			if entry.Attributes.Gid == 0 {
				entry.Attributes.Gid = gid
			}
			if entry.Attributes.Crtime == 0 {
				entry.Attributes.Crtime = time.Now().Unix()
			}
			entry.Attributes.Mtime = time.Now().Unix()
			entry.Attributes.Collection, entry.Attributes.Replication = fh.dirtyPages.GetStorageOptions()
		}

		request := &filer_pb.CreateEntryRequest{
			Directory:  string(dir),
			Entry:      entry,
			Signatures: []int32{wfs.signature},
		}

		glog.V(4).Infof("%s set chunks: %v", fileFullPath, len(entry.Chunks))
		for i, chunk := range entry.Chunks {
			glog.V(4).Infof("%s chunks %d: %v [%d,%d)", fileFullPath, i, chunk.GetFileIdString(), chunk.Offset, chunk.Offset+int64(chunk.Size))
		}

		manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(entry.Chunks)

		chunks, _ := filer.CompactFileChunks(wfs.LookupFn(), nonManifestChunks)
		chunks, manifestErr := filer.MaybeManifestize(wfs.saveDataAsChunk(fileFullPath), chunks)
		if manifestErr != nil {
			// not good, but should be ok
			glog.V(0).Infof("MaybeManifestize: %v", manifestErr)
		}
		entry.Chunks = append(chunks, manifestChunks...)

		wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.Errorf("fh flush create %s: %v", fileFullPath, err)
			return fmt.Errorf("fh flush create %s: %v", fileFullPath, err)
		}

		wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})

	if err == nil {
		fh.dirtyMetadata = false
	}

	if err != nil {
		glog.Errorf("%v fh %d flush: %v", fileFullPath, fh.handle, err)
		return fuse.EIO
	}

	return fuse.OK
}
