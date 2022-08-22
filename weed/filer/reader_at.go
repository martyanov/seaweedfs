package filer

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type ChunkReadAt struct {
	masterClient  *wdclient.MasterClient
	chunkViews    []*ChunkView
	readerLock    sync.Mutex
	fileSize      int64
	readerCache   *ReaderCache
	readerPattern *ReaderPattern
	lastChunkFid  string
}

var _ = io.ReaderAt(&ChunkReadAt{})
var _ = io.Closer(&ChunkReadAt{})

func LookupFn(filerClient filer_pb.FilerClient) wdclient.LookupFileIdFunctionType {

	vidCache := make(map[string]*filer_pb.Locations)
	var vicCacheLock sync.RWMutex
	return func(fileId string) (targetUrls []string, err error) {
		vid := VolumeId(fileId)
		vicCacheLock.RLock()
		locations, found := vidCache[vid]
		vicCacheLock.RUnlock()

		if !found {
			util.Retry("lookup volume "+vid, func() error {
				err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
					resp, err := client.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
						VolumeIds: []string{vid},
					})
					if err != nil {
						return err
					}

					locations = resp.LocationsMap[vid]
					if locations == nil || len(locations.Locations) == 0 {
						glog.V(0).Infof("failed to locate %s", fileId)
						return fmt.Errorf("failed to locate %s", fileId)
					}
					vicCacheLock.Lock()
					vidCache[vid] = locations
					vicCacheLock.Unlock()

					return nil
				})
				return err
			})
		}

		if err != nil {
			return nil, err
		}

		fcDataCenter := filerClient.GetDataCenter()
		var sameDcTargetUrls, otherTargetUrls []string
		for _, loc := range locations.Locations {
			volumeServerAddress := filerClient.AdjustedUrl(loc)
			targetUrl := fmt.Sprintf("http://%s/%s", volumeServerAddress, fileId)
			if fcDataCenter == "" || fcDataCenter != loc.DataCenter {
				otherTargetUrls = append(otherTargetUrls, targetUrl)
			} else {
				sameDcTargetUrls = append(sameDcTargetUrls, targetUrl)
			}
		}
		rand.Shuffle(len(sameDcTargetUrls), func(i, j int) {
			sameDcTargetUrls[i], sameDcTargetUrls[j] = sameDcTargetUrls[j], sameDcTargetUrls[i]
		})
		rand.Shuffle(len(otherTargetUrls), func(i, j int) {
			otherTargetUrls[i], otherTargetUrls[j] = otherTargetUrls[j], otherTargetUrls[i]
		})
		// Prefer same data center
		targetUrls = append(sameDcTargetUrls, otherTargetUrls...)
		return
	}
}

func NewChunkReaderAtFromClient(lookupFn wdclient.LookupFileIdFunctionType, chunkViews []*ChunkView, chunkCache chunk_cache.ChunkCache, fileSize int64) *ChunkReadAt {

	return &ChunkReadAt{
		chunkViews:    chunkViews,
		fileSize:      fileSize,
		readerCache:   newReaderCache(32, chunkCache, lookupFn),
		readerPattern: NewReaderPattern(),
	}
}

func (c *ChunkReadAt) Close() error {
	c.readerCache.destroy()
	return nil
}

func (c *ChunkReadAt) ReadAt(p []byte, offset int64) (n int, err error) {

	c.readerPattern.MonitorReadAt(offset, len(p))

	c.readerLock.Lock()
	defer c.readerLock.Unlock()

	// glog.V(4).Infof("ReadAt [%d,%d) of total file size %d bytes %d chunk views", offset, offset+int64(len(p)), c.fileSize, len(c.chunkViews))
	return c.doReadAt(p, offset)
}

func (c *ChunkReadAt) doReadAt(p []byte, offset int64) (n int, err error) {

	startOffset, remaining := offset, int64(len(p))
	var nextChunks []*ChunkView
	for i, chunk := range c.chunkViews {
		if remaining <= 0 {
			break
		}
		if i+1 < len(c.chunkViews) {
			nextChunks = c.chunkViews[i+1:]
		}
		if startOffset < chunk.LogicOffset {
			gap := chunk.LogicOffset - startOffset
			glog.V(4).Infof("zero [%d,%d)", startOffset, chunk.LogicOffset)
			n += zero(p, startOffset-offset, gap)
			startOffset, remaining = chunk.LogicOffset, remaining-gap
			if remaining <= 0 {
				break
			}
		}
		// fmt.Printf(">>> doReadAt [%d,%d), chunk[%d,%d)\n", offset, offset+int64(len(p)), chunk.LogicOffset, chunk.LogicOffset+int64(chunk.Size))
		chunkStart, chunkStop := max(chunk.LogicOffset, startOffset), min(chunk.LogicOffset+int64(chunk.Size), startOffset+remaining)
		if chunkStart >= chunkStop {
			continue
		}
		// glog.V(4).Infof("read [%d,%d), %d/%d chunk %s [%d,%d)", chunkStart, chunkStop, i, len(c.chunkViews), chunk.FileId, chunk.LogicOffset-chunk.Offset, chunk.LogicOffset-chunk.Offset+int64(chunk.Size))
		bufferOffset := chunkStart - chunk.LogicOffset + chunk.Offset
		copied, err := c.readChunkSliceAt(p[startOffset-offset:chunkStop-chunkStart+startOffset-offset], chunk, nextChunks, uint64(bufferOffset))
		if err != nil {
			glog.Errorf("fetching chunk %+v: %v\n", chunk, err)
			return copied, err
		}

		n += copied
		startOffset, remaining = startOffset+int64(copied), remaining-int64(copied)
	}

	// glog.V(4).Infof("doReadAt [%d,%d), n:%v, err:%v", offset, offset+int64(len(p)), n, err)

	// zero the remaining bytes if a gap exists at the end of the last chunk (or a fully sparse file)
	if err == nil && remaining > 0 {
		var delta int64
		if c.fileSize > startOffset {
			delta = min(remaining, c.fileSize-startOffset)
			startOffset -= offset
		} else {
			delta = remaining
			startOffset = max(startOffset-offset, startOffset-remaining-offset)
		}
		glog.V(4).Infof("zero2 [%d,%d) of file size %d bytes", startOffset, startOffset+delta, c.fileSize)
		n += zero(p, startOffset, delta)
	}

	if err == nil && offset+int64(len(p)) >= c.fileSize {
		err = io.EOF
	}
	// fmt.Printf("~~~ filled %d, err: %v\n\n", n, err)

	return

}

func (c *ChunkReadAt) readChunkSliceAt(buffer []byte, chunkView *ChunkView, nextChunkViews []*ChunkView, offset uint64) (n int, err error) {

	if c.readerPattern.IsRandomMode() {
		n, err := c.readerCache.chunkCache.ReadChunkAt(buffer, chunkView.FileId, offset)
		if n > 0 {
			return n, err
		}
		return fetchChunkRange(buffer, c.readerCache.lookupFileIdFn, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int64(offset))
	}

	n, err = c.readerCache.ReadChunkAt(buffer, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int64(offset), int(chunkView.ChunkSize), chunkView.LogicOffset == 0)
	if c.lastChunkFid != chunkView.FileId {
		if chunkView.Offset == 0 { // start of a new chunk
			if c.lastChunkFid != "" {
				c.readerCache.UnCache(c.lastChunkFid)
				c.readerCache.MaybeCache(nextChunkViews)
			} else {
				if len(nextChunkViews) >= 1 {
					c.readerCache.MaybeCache(nextChunkViews[:1]) // just read the next chunk if at the very beginning
				}
			}
		}
	}
	c.lastChunkFid = chunkView.FileId
	return
}

func zero(buffer []byte, start, length int64) int {
	end := min(start+length, int64(len(buffer)))
	start = max(start, 0)

	// zero the bytes
	for o := start; o < end; o++ {
		buffer[o] = 0
	}
	return int(end - start)
}
