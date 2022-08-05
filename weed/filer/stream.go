package filer

import (
	"bytes"
	"fmt"
	"golang.org/x/exp/slices"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

var getLookupFileIdBackoffSchedule = []time.Duration{
	150 * time.Millisecond,
	600 * time.Millisecond,
	1800 * time.Millisecond,
}

func HasData(entry *filer_pb.Entry) bool {

	if len(entry.Content) > 0 {
		return true
	}

	return len(entry.Chunks) > 0
}

func IsSameData(a, b *filer_pb.Entry) bool {

	if len(a.Content) > 0 || len(b.Content) > 0 {
		return bytes.Equal(a.Content, b.Content)
	}

	return isSameChunks(a.Chunks, b.Chunks)
}

func isSameChunks(a, b []*filer_pb.FileChunk) bool {
	if len(a) != len(b) {
		return false
	}
	slices.SortFunc(a, func(i, j *filer_pb.FileChunk) bool {
		return strings.Compare(i.ETag, j.ETag) < 0
	})
	slices.SortFunc(b, func(i, j *filer_pb.FileChunk) bool {
		return strings.Compare(i.ETag, j.ETag) < 0
	})
	for i := 0; i < len(a); i++ {
		if a[i].ETag != b[i].ETag {
			return false
		}
	}
	return true
}

func NewFileReader(filerClient filer_pb.FilerClient, entry *filer_pb.Entry) io.Reader {
	if len(entry.Content) > 0 {
		return bytes.NewReader(entry.Content)
	}
	return NewChunkStreamReader(filerClient, entry.Chunks)
}

func StreamContent(masterClient wdclient.HasLookupFileIdFunction, writer io.Writer, chunks []*filer_pb.FileChunk, offset int64, size int64) error {
	return StreamContentWithThrottler(masterClient, writer, chunks, offset, size, 0)
}

func StreamContentWithThrottler(masterClient wdclient.HasLookupFileIdFunction, writer io.Writer, chunks []*filer_pb.FileChunk, offset int64, size int64, downloadMaxBytesPs int64) error {

	glog.V(4).Infof("start to stream content for chunks: %+v", chunks)
	chunkViews := ViewFromChunks(masterClient.GetLookupFileIdFunction(), chunks, offset, size)

	fileId2Url := make(map[string][]string)

	for _, chunkView := range chunkViews {
		var urlStrings []string
		var err error
		for _, backoff := range getLookupFileIdBackoffSchedule {
			urlStrings, err = masterClient.GetLookupFileIdFunction()(chunkView.FileId)
			if err == nil && len(urlStrings) > 0 {
				break
			}
			time.Sleep(backoff)
		}
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return err
		} else if len(urlStrings) == 0 {
			errUrlNotFound := fmt.Errorf("operation LookupFileId %s failed, err: urls not found", chunkView.FileId)
			glog.Error(errUrlNotFound)
			return errUrlNotFound
		}
		fileId2Url[chunkView.FileId] = urlStrings
	}

	downloadThrottler := util.NewWriteThrottler(downloadMaxBytesPs)
	remaining := size
	for _, chunkView := range chunkViews {
		if offset < chunkView.LogicOffset {
			gap := chunkView.LogicOffset - offset
			remaining -= gap
			glog.V(4).Infof("zero [%d,%d)", offset, chunkView.LogicOffset)
			err := writeZero(writer, gap)
			if err != nil {
				return fmt.Errorf("write zero [%d,%d)", offset, chunkView.LogicOffset)
			}
			offset = chunkView.LogicOffset
		}
		urlStrings := fileId2Url[chunkView.FileId]
		start := time.Now()
		err := retriedStreamFetchChunkData(writer, urlStrings, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.Offset, int(chunkView.Size))
		offset += int64(chunkView.Size)
		remaining -= int64(chunkView.Size)
		stats.FilerRequestHistogram.WithLabelValues("chunkDownload").Observe(time.Since(start).Seconds())
		if err != nil {
			stats.FilerRequestCounter.WithLabelValues("chunkDownloadError").Inc()
			return fmt.Errorf("read chunk: %v", err)
		}
		stats.FilerRequestCounter.WithLabelValues("chunkDownload").Inc()
		downloadThrottler.MaybeSlowdown(int64(chunkView.Size))
	}
	if remaining > 0 {
		glog.V(4).Infof("zero [%d,%d)", offset, offset+remaining)
		err := writeZero(writer, remaining)
		if err != nil {
			return fmt.Errorf("write zero [%d,%d)", offset, offset+remaining)
		}
	}

	return nil

}

// ----------------  ReadAllReader ----------------------------------

func writeZero(w io.Writer, size int64) (err error) {
	zeroPadding := make([]byte, 1024)
	var written int
	for size > 0 {
		if size > 1024 {
			written, err = w.Write(zeroPadding)
		} else {
			written, err = w.Write(zeroPadding[:size])
		}
		size -= int64(written)
		if err != nil {
			return
		}
	}
	return
}

func ReadAll(buffer []byte, masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) error {

	lookupFileIdFn := func(fileId string) (targetUrls []string, err error) {
		return masterClient.LookupFileId(fileId)
	}

	chunkViews := ViewFromChunks(lookupFileIdFn, chunks, 0, int64(len(buffer)))

	idx := 0

	for _, chunkView := range chunkViews {
		urlStrings, err := lookupFileIdFn(chunkView.FileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return err
		}

		n, err := retriedFetchChunkData(buffer[idx:idx+int(chunkView.Size)], urlStrings, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.Offset)
		if err != nil {
			return err
		}
		idx += n
	}
	return nil
}

// ----------------  ChunkStreamReader ----------------------------------
type ChunkStreamReader struct {
	chunkViews   []*ChunkView
	totalSize    int64
	logicOffset  int64
	buffer       []byte
	bufferOffset int64
	bufferLock   sync.Mutex
	chunk        string
	lookupFileId wdclient.LookupFileIdFunctionType
}

var _ = io.ReadSeeker(&ChunkStreamReader{})
var _ = io.ReaderAt(&ChunkStreamReader{})

func doNewChunkStreamReader(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	chunkViews := ViewFromChunks(lookupFileIdFn, chunks, 0, math.MaxInt64)
	slices.SortFunc(chunkViews, func(a, b *ChunkView) bool {
		return a.LogicOffset < b.LogicOffset
	})

	var totalSize int64
	for _, chunk := range chunkViews {
		totalSize += int64(chunk.Size)
	}

	return &ChunkStreamReader{
		chunkViews:   chunkViews,
		lookupFileId: lookupFileIdFn,
		totalSize:    totalSize,
	}
}

func NewChunkStreamReaderFromFiler(masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	lookupFileIdFn := func(fileId string) (targetUrl []string, err error) {
		return masterClient.LookupFileId(fileId)
	}

	return doNewChunkStreamReader(lookupFileIdFn, chunks)
}

func NewChunkStreamReader(filerClient filer_pb.FilerClient, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	lookupFileIdFn := LookupFn(filerClient)

	return doNewChunkStreamReader(lookupFileIdFn, chunks)
}

func (c *ChunkStreamReader) ReadAt(p []byte, off int64) (n int, err error) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	if err = c.prepareBufferFor(off); err != nil {
		return
	}
	c.logicOffset = off
	return c.doRead(p)
}

func (c *ChunkStreamReader) Read(p []byte) (n int, err error) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	return c.doRead(p)
}

func (c *ChunkStreamReader) doRead(p []byte) (n int, err error) {
	// fmt.Printf("do read [%d,%d) at %s[%d,%d)\n", c.logicOffset, c.logicOffset+int64(len(p)), c.chunk, c.bufferOffset, c.bufferOffset+int64(len(c.buffer)))
	for n < len(p) {
		// println("read", c.logicOffset)
		if err = c.prepareBufferFor(c.logicOffset); err != nil {
			return
		}
		t := copy(p[n:], c.buffer[c.logicOffset-c.bufferOffset:])
		n += t
		c.logicOffset += int64(t)
	}
	return
}

func (c *ChunkStreamReader) isBufferEmpty() bool {
	return len(c.buffer) <= int(c.logicOffset-c.bufferOffset)
}

func (c *ChunkStreamReader) Seek(offset int64, whence int) (int64, error) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()

	var err error
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += c.logicOffset
	case io.SeekEnd:
		offset = c.totalSize + offset
	}
	if offset > c.totalSize {
		err = io.ErrUnexpectedEOF
	} else {
		c.logicOffset = offset
	}

	return offset, err

}

func insideChunk(offset int64, chunk *ChunkView) bool {
	return chunk.LogicOffset <= offset && offset < chunk.LogicOffset+int64(chunk.Size)
}

func (c *ChunkStreamReader) prepareBufferFor(offset int64) (err error) {
	// stay in the same chunk
	if c.bufferOffset <= offset && offset < c.bufferOffset+int64(len(c.buffer)) {
		return nil
	}

	// fmt.Printf("fetch for offset %d\n", offset)

	// need to seek to a different chunk
	currentChunkIndex := sort.Search(len(c.chunkViews), func(i int) bool {
		return offset < c.chunkViews[i].LogicOffset
	})
	if currentChunkIndex == len(c.chunkViews) {
		// not found
		if insideChunk(offset, c.chunkViews[0]) {
			// fmt.Printf("select0 chunk %d %s\n", currentChunkIndex, c.chunkViews[currentChunkIndex].FileId)
			currentChunkIndex = 0
		} else if insideChunk(offset, c.chunkViews[len(c.chunkViews)-1]) {
			currentChunkIndex = len(c.chunkViews) - 1
			// fmt.Printf("select last chunk %d %s\n", currentChunkIndex, c.chunkViews[currentChunkIndex].FileId)
		} else {
			return io.EOF
		}
	} else if currentChunkIndex > 0 {
		if insideChunk(offset, c.chunkViews[currentChunkIndex]) {
			// good hit
		} else if insideChunk(offset, c.chunkViews[currentChunkIndex-1]) {
			currentChunkIndex -= 1
			// fmt.Printf("select -1 chunk %d %s\n", currentChunkIndex, c.chunkViews[currentChunkIndex].FileId)
		} else {
			// glog.Fatalf("unexpected1 offset %d", offset)
			return fmt.Errorf("unexpected1 offset %d", offset)
		}
	} else {
		// glog.Fatalf("unexpected2 offset %d", offset)
		return fmt.Errorf("unexpected2 offset %d", offset)
	}

	// positioning within the new chunk
	chunk := c.chunkViews[currentChunkIndex]
	if insideChunk(offset, chunk) {
		if c.isBufferEmpty() || c.bufferOffset != chunk.LogicOffset {
			if err = c.fetchChunkToBuffer(chunk); err != nil {
				return
			}
		}
	} else {
		// glog.Fatalf("unexpected3 offset %d in %s [%d,%d)", offset, chunk.FileId, chunk.LogicOffset, chunk.LogicOffset+int64(chunk.Size))
		return fmt.Errorf("unexpected3 offset %d in %s [%d,%d)", offset, chunk.FileId, chunk.LogicOffset, chunk.LogicOffset+int64(chunk.Size))
	}
	return
}

func (c *ChunkStreamReader) fetchChunkToBuffer(chunkView *ChunkView) error {
	urlStrings, err := c.lookupFileId(chunkView.FileId)
	if err != nil {
		glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
		return err
	}
	var buffer bytes.Buffer
	var shouldRetry bool
	for _, urlString := range urlStrings {
		shouldRetry, err = util.ReadUrlAsStream(urlString+"?readDeleted=true", chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.Offset, int(chunkView.Size), func(data []byte) {
			buffer.Write(data)
		})
		if !shouldRetry {
			break
		}
		if err != nil {
			glog.V(1).Infof("read %s failed, err: %v", chunkView.FileId, err)
			buffer.Reset()
		} else {
			break
		}
	}
	if err != nil {
		return err
	}
	c.buffer = buffer.Bytes()
	c.bufferOffset = chunkView.LogicOffset
	c.chunk = chunkView.FileId

	// glog.V(0).Infof("fetched %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	return nil
}

func (c *ChunkStreamReader) Close() {
	// TODO try to release and reuse buffer
}

func VolumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}
