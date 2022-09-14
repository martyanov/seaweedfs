package storage

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const PagedReadLimit = 1024 * 1024

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedle(n *needle.Needle, readOption *ReadOption, onReadSizeFn func(size Size)) (count int, err error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset.IsZero() {
		return -1, ErrorNotFound
	}
	readSize := nv.Size
	if readSize.IsDeleted() {
		if readOption != nil && readOption.ReadDeleted && readSize != TombstoneFileSize {
			glog.V(3).Infof("reading deleted %s", n.String())
			readSize = -readSize
		} else {
			return -1, ErrorDeleted
		}
	}
	if readSize == 0 {
		return 0, nil
	}
	if onReadSizeFn != nil {
		onReadSizeFn(readSize)
	}
	if readOption != nil && readOption.AttemptMetaOnly && readSize > PagedReadLimit {
		readOption.VolumeRevision = v.SuperBlock.CompactionRevision
		err = n.ReadNeedleMeta(v.DataBackend, nv.Offset.ToActualOffset(), readSize, v.Version())
		if err == needle.ErrorSizeMismatch && OffsetSize == 4 {
			readOption.IsOutOfRange = true
			err = n.ReadNeedleMeta(v.DataBackend, nv.Offset.ToActualOffset()+int64(MaxPossibleVolumeSize), readSize, v.Version())
		}
		if err != nil {
			return 0, err
		}
		if !n.IsCompressed() && !n.IsChunkedManifest() {
			readOption.IsMetaOnly = true
		}
	}
	if readOption == nil || !readOption.IsMetaOnly {
		err = n.ReadData(v.DataBackend, nv.Offset.ToActualOffset(), readSize, v.Version())
		if err == needle.ErrorSizeMismatch && OffsetSize == 4 {
			err = n.ReadData(v.DataBackend, nv.Offset.ToActualOffset()+int64(MaxPossibleVolumeSize), readSize, v.Version())
		}
		v.checkReadWriteError(err)
		if err != nil {
			return 0, err
		}
	}
	count = int(n.DataSize)
	if !n.HasTtl() {
		return
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return
	}
	if !n.HasLastModifiedDate() {
		return
	}
	if time.Now().Before(time.Unix(0, int64(n.AppendAtNs)).Add(time.Duration(ttlMinutes) * time.Minute)) {
		return
	}
	return -1, ErrorNotFound
}

// read needle at a specific offset
func (v *Volume) readNeedleMetaAt(n *needle.Needle, offset int64, size int32) (err error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	// read deleted meta data
	if size < 0 {
		size = -size
	}
	err = n.ReadNeedleMeta(v.DataBackend, offset, Size(size), v.Version())
	if err == needle.ErrorSizeMismatch && OffsetSize == 4 {
		err = n.ReadNeedleMeta(v.DataBackend, offset+int64(MaxPossibleVolumeSize), Size(size), v.Version())
	}
	if err != nil {
		return err
	}
	return nil
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedleDataInto(n *needle.Needle, readOption *ReadOption, writer io.Writer, offset int64, size int64) (err error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset.IsZero() {
		return ErrorNotFound
	}
	readSize := nv.Size
	if readSize.IsDeleted() {
		if readOption != nil && readOption.ReadDeleted && readSize != TombstoneFileSize {
			glog.V(3).Infof("reading deleted %s", n.String())
			readSize = -readSize
		} else {
			return ErrorDeleted
		}
	}
	if readSize == 0 {
		return nil
	}

	if readOption.VolumeRevision != v.SuperBlock.CompactionRevision {
		// the volume is compacted
		readOption.IsOutOfRange = false
		err = n.ReadNeedleMeta(v.DataBackend, nv.Offset.ToActualOffset(), readSize, v.Version())
	}
	buf := mem.Allocate(min(1024*1024, int(size)))
	defer mem.Free(buf)
	actualOffset := nv.Offset.ToActualOffset()
	if readOption.IsOutOfRange {
		actualOffset += int64(MaxPossibleVolumeSize)
	}

	return n.ReadNeedleDataInto(v.DataBackend, actualOffset, buf, writer, offset, size)
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) ReadNeedleBlob(offset int64, size Size) ([]byte, error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	return needle.ReadNeedleBlob(v.DataBackend, offset, size, v.Version())
}

type VolumeFileScanner interface {
	VisitSuperBlock(super_block.SuperBlock) error
	ReadNeedleBody() bool
	VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error
}

func ScanVolumeFile(dirname string, collection string, id needle.VolumeId,
	needleMapKind NeedleMapKind,
	volumeFileScanner VolumeFileScanner) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection, id, needleMapKind); err != nil {
		return fmt.Errorf("failed to load volume %d: %v", id, err)
	}
	if err = volumeFileScanner.VisitSuperBlock(v.SuperBlock); err != nil {
		return fmt.Errorf("failed to process volume %d super block: %v", id, err)
	}
	defer v.Close()

	version := v.Version()

	offset := int64(v.SuperBlock.BlockSize())

	return ScanVolumeFileFrom(version, v.DataBackend, offset, volumeFileScanner)
}

func ScanVolumeFileFrom(version needle.Version, datBackend backend.BackendStorageFile, offset int64, volumeFileScanner VolumeFileScanner) (err error) {
	n, nh, rest, e := needle.ReadNeedleHeader(datBackend, version, offset)
	if e != nil {
		if e == io.EOF {
			return nil
		}
		return fmt.Errorf("cannot read %s at offset %d: %v", datBackend.Name(), offset, e)
	}
	for n != nil {
		var needleBody []byte
		if volumeFileScanner.ReadNeedleBody() {
			// println("needle", n.Id.String(), "offset", offset, "size", n.Size, "rest", rest)
			if needleBody, err = n.ReadNeedleBody(datBackend, version, offset+NeedleHeaderSize, rest); err != nil {
				glog.V(0).Infof("cannot read needle head [%d, %d) body [%d, %d) body length %d: %v", offset, offset+NeedleHeaderSize, offset+NeedleHeaderSize, offset+NeedleHeaderSize+rest, rest, err)
				// err = fmt.Errorf("cannot read needle body: %v", err)
				// return
			}
		}
		err := volumeFileScanner.VisitNeedle(n, offset, nh, needleBody)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			glog.V(0).Infof("visit needle error: %v", err)
			return fmt.Errorf("visit needle error: %v", err)
		}
		offset += NeedleHeaderSize + rest
		glog.V(4).Infof("==> new entry offset %d", offset)
		if n, nh, rest, err = needle.ReadNeedleHeader(datBackend, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header at offset %d: %v", offset, err)
		}
		glog.V(4).Infof("new entry needle size:%d rest:%d", n.Size, rest)
	}
	return nil
}
