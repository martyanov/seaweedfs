package chunk_cache

import (
	"errors"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

var ErrorOutOfBounds = errors.New("attempt to read out of bounds")

type ChunkCache interface {
	ReadChunkAt(data []byte, fileId string, offset uint64) (n int, err error)
	SetChunk(fileId string, data []byte)
}

// a global cache for recently accessed file chunks
type TieredChunkCache struct {
	memCache   *ChunkCacheInMemory
	diskCaches []*OnDiskCacheLayer
	sync.RWMutex
	onDiskCacheSizeLimit0 uint64
	onDiskCacheSizeLimit1 uint64
	onDiskCacheSizeLimit2 uint64
}

var _ ChunkCache = &TieredChunkCache{}

func NewTieredChunkCache(maxEntries int64, dir string, diskSizeInUnit int64, unitSize int64) *TieredChunkCache {

	c := &TieredChunkCache{
		memCache: NewChunkCacheInMemory(maxEntries),
	}
	c.diskCaches = make([]*OnDiskCacheLayer, 3)
	c.onDiskCacheSizeLimit0 = uint64(unitSize)
	c.onDiskCacheSizeLimit1 = 4 * c.onDiskCacheSizeLimit0
	c.onDiskCacheSizeLimit2 = 2 * c.onDiskCacheSizeLimit1
	c.diskCaches[0] = NewOnDiskCacheLayer(dir, "c0_2", diskSizeInUnit*unitSize/8, 2)
	c.diskCaches[1] = NewOnDiskCacheLayer(dir, "c1_3", diskSizeInUnit*unitSize/4+diskSizeInUnit*unitSize/8, 3)
	c.diskCaches[2] = NewOnDiskCacheLayer(dir, "c2_2", diskSizeInUnit*unitSize/2, 2)

	return c
}

func (c *TieredChunkCache) ReadChunkAt(data []byte, fileId string, offset uint64) (n int, err error) {
	if c == nil {
		return 0, nil
	}

	c.RLock()
	defer c.RUnlock()

	minSize := offset + uint64(len(data))
	if minSize <= c.onDiskCacheSizeLimit0 {
		n, err = c.memCache.readChunkAt(data, fileId, offset)
		if err != nil {
			glog.Errorf("failed to read from memcache: %s", err)
		}
		if n >= int(minSize) {
			return n, nil
		}
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return n, nil
	}

	if minSize <= c.onDiskCacheSizeLimit0 {
		n, err = c.diskCaches[0].readChunkAt(data, fid.Key, offset)
		if n >= int(minSize) {
			return
		}
	}
	if minSize <= c.onDiskCacheSizeLimit1 {
		n, err = c.diskCaches[1].readChunkAt(data, fid.Key, offset)
		if n >= int(minSize) {
			return
		}
	}
	{
		n, err = c.diskCaches[2].readChunkAt(data, fid.Key, offset)
		if n >= int(minSize) {
			return
		}
	}

	return 0, nil

}

func (c *TieredChunkCache) SetChunk(fileId string, data []byte) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()

	glog.V(4).Infof("SetChunk %s size %d\n", fileId, len(data))

	c.doSetChunk(fileId, data)
}

func (c *TieredChunkCache) doSetChunk(fileId string, data []byte) {

	if len(data) <= int(c.onDiskCacheSizeLimit0) {
		c.memCache.SetChunk(fileId, data)
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return
	}

	if len(data) <= int(c.onDiskCacheSizeLimit0) {
		c.diskCaches[0].setChunk(fid.Key, data)
	} else if len(data) <= int(c.onDiskCacheSizeLimit1) {
		c.diskCaches[1].setChunk(fid.Key, data)
	} else {
		c.diskCaches[2].setChunk(fid.Key, data)
	}

}

func (c *TieredChunkCache) Shutdown() {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	for _, diskCache := range c.diskCaches {
		diskCache.shutdown()
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
