package filersink

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (fs *FilerSink) replicateChunks(sourceChunks []*filer_pb.FileChunk, path string) (replicatedChunks []*filer_pb.FileChunk, err error) {
	if len(sourceChunks) == 0 {
		return
	}

	replicatedChunks = make([]*filer_pb.FileChunk, len(sourceChunks))

	var wg sync.WaitGroup
	for chunkIndex, sourceChunk := range sourceChunks {
		wg.Add(1)
		go func(chunk *filer_pb.FileChunk, index int) {
			defer wg.Done()
			replicatedChunk, e := fs.replicateOneChunk(chunk, path)
			if e != nil {
				err = e
				return
			}
			replicatedChunks[index] = replicatedChunk
		}(sourceChunk, chunkIndex)
	}
	wg.Wait()

	return
}

func (fs *FilerSink) replicateOneChunk(sourceChunk *filer_pb.FileChunk, path string) (*filer_pb.FileChunk, error) {

	fileId, err := fs.fetchAndWrite(sourceChunk, path)
	if err != nil {
		return nil, fmt.Errorf("copy %s: %v", sourceChunk.GetFileIdString(), err)
	}

	return &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       sourceChunk.Offset,
		Size:         sourceChunk.Size,
		Mtime:        sourceChunk.Mtime,
		ETag:         sourceChunk.ETag,
		SourceFileId: sourceChunk.GetFileIdString(),
		CipherKey:    sourceChunk.CipherKey,
		IsCompressed: sourceChunk.IsCompressed,
	}, nil
}

func (fs *FilerSink) fetchAndWrite(sourceChunk *filer_pb.FileChunk, path string) (fileId string, err error) {

	filename, header, resp, err := fs.filerSource.ReadPart(sourceChunk.GetFileIdString())
	if err != nil {
		return "", fmt.Errorf("read part %s: %v", sourceChunk.GetFileIdString(), err)
	}
	defer util.CloseResponse(resp)

	fileId, uploadResult, err, _ := operation.UploadWithRetry(
		fs,
		&filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: fs.replication,
			Collection:  fs.collection,
			TtlSec:      fs.ttlSec,
			DataCenter:  fs.dataCenter,
			DiskType:    fs.diskType,
			Path:        path,
		},
		&operation.UploadOption{
			Filename:          filename,
			Cipher:            false,
			IsInputCompressed: "gzip" == header.Get("Content-Encoding"),
			MimeType:          header.Get("Content-Type"),
			PairMap:           nil,
		},
		func(host, fileId string) string {
			fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
			if fs.writeChunkByFiler {
				fileUrl = fmt.Sprintf("http://%s/?proxyChunkId=%s", fs.address, fileId)
			}
			glog.V(4).Infof("replicating %s to %s header:%+v", filename, fileUrl, header)
			return fileUrl
		},
		resp.Body,
	)

	if err != nil {
		glog.V(0).Infof("upload source data %v: %v", sourceChunk.GetFileIdString(), err)
		return "", fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v: %v", filename, err)
		return "", fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	return
}

var _ = filer_pb.FilerClient(&FilerSink{})

func (fs *FilerSink) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return rpc.WithGrpcClient(streamingMode, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.grpcAddress, fs.grpcDialOption)

}

func (fs *FilerSink) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (fs *FilerSink) GetDataCenter() string {
	return fs.dataCenter
}
