package s3api

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"golang.org/x/exp/slices"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
)

type InitiateMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	s3.CreateMultipartUploadOutput
}

func (s3a *S3ApiServer) createMultipartUpload(input *s3.CreateMultipartUploadInput) (output *InitiateMultipartUploadResult, code s3err.ErrorCode) {

	glog.V(2).Infof("createMultipartUpload input %v", input)

	uploadIdString := s3a.generateUploadID(*input.Key)

	if err := s3a.mkdir(s3a.genUploadsFolder(*input.Bucket), uploadIdString, func(entry *filer_pb.Entry) {
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended["key"] = []byte(*input.Key)
		for k, v := range input.Metadata {
			entry.Extended[k] = []byte(*v)
		}
		if input.ContentType != nil {
			entry.Attributes.Mime = *input.ContentType
		}
	}); err != nil {
		glog.Errorf("NewMultipartUpload error: %v", err)
		return nil, s3err.ErrInternalError
	}

	output = &InitiateMultipartUploadResult{
		CreateMultipartUploadOutput: s3.CreateMultipartUploadOutput{
			Bucket:   input.Bucket,
			Key:      objectKey(input.Key),
			UploadId: aws.String(uploadIdString),
		},
	}

	return
}

type CompleteMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	s3.CompleteMultipartUploadOutput
}

func (s3a *S3ApiServer) completeMultipartUpload(input *s3.CompleteMultipartUploadInput, parts *CompleteMultipartUpload) (output *CompleteMultipartUploadResult, code s3err.ErrorCode) {

	glog.V(2).Infof("completeMultipartUpload input %v", input)

	completedParts := parts.Parts
	slices.SortFunc(completedParts, func(a, b CompletedPart) bool {
		return a.PartNumber < b.PartNumber
	})

	uploadDirectory := s3a.genUploadsFolder(*input.Bucket) + "/" + *input.UploadId

	entries, _, err := s3a.list(uploadDirectory, "", "", false, maxPartsList)
	if err != nil || len(entries) == 0 {
		glog.Errorf("completeMultipartUpload %s %s error: %v, entries:%d", *input.Bucket, *input.UploadId, err, len(entries))
		return nil, s3err.ErrNoSuchUpload
	}

	pentry, err := s3a.getEntry(s3a.genUploadsFolder(*input.Bucket), *input.UploadId)
	if err != nil {
		glog.Errorf("completeMultipartUpload %s %s error: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrNoSuchUpload
	}

	mime := pentry.Attributes.Mime

	var finalParts []*filer_pb.FileChunk
	var offset int64

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name, ".part") && !entry.IsDirectory {
			partETag, found := findByPartNumber(entry.Name, completedParts)
			if !found {
				continue
			}
			entryETag := hex.EncodeToString(entry.Attributes.GetMd5())
			if partETag != "" && len(partETag) == 32 && entryETag != "" && entryETag != partETag {
				glog.Errorf("completeMultipartUpload %s ETag mismatch chunk: %s part: %s", entry.Name, entryETag, partETag)
				return nil, s3err.ErrInvalidPart
			}
			for _, chunk := range entry.Chunks {
				p := &filer_pb.FileChunk{
					FileId:    chunk.GetFileIdString(),
					Offset:    offset,
					Size:      chunk.Size,
					Mtime:     chunk.Mtime,
					CipherKey: chunk.CipherKey,
					ETag:      chunk.ETag,
				}
				finalParts = append(finalParts, p)
				offset += int64(chunk.Size)
			}
		}
	}

	entryName := filepath.Base(*input.Key)
	dirName := filepath.Dir(*input.Key)
	if dirName == "." {
		dirName = ""
	}
	if strings.HasPrefix(dirName, "/") {
		dirName = dirName[1:]
	}
	dirName = fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, *input.Bucket, dirName)

	// remove suffix '/'
	if strings.HasSuffix(dirName, "/") {
		dirName = dirName[:len(dirName)-1]
	}

	err = s3a.mkFile(dirName, entryName, finalParts, func(entry *filer_pb.Entry) {
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		for k, v := range pentry.Extended {
			if k != "key" {
				entry.Extended[k] = v
			}
		}
		if pentry.Attributes.Mime != "" {
			entry.Attributes.Mime = pentry.Attributes.Mime
		} else if mime != "" {
			entry.Attributes.Mime = mime
		}
	})

	if err != nil {
		glog.Errorf("completeMultipartUpload %s/%s error: %v", dirName, entryName, err)
		return nil, s3err.ErrInternalError
	}

	output = &CompleteMultipartUploadResult{
		CompleteMultipartUploadOutput: s3.CompleteMultipartUploadOutput{
			Location: aws.String(fmt.Sprintf("http://%s%s/%s", s3a.option.Filer.ToHttpAddress(), urlPathEscape(dirName), urlPathEscape(entryName))),
			Bucket:   input.Bucket,
			ETag:     aws.String("\"" + filer.ETagChunks(finalParts) + "\""),
			Key:      objectKey(input.Key),
		},
	}

	if err = s3a.rm(s3a.genUploadsFolder(*input.Bucket), *input.UploadId, false, true); err != nil {
		glog.V(1).Infof("completeMultipartUpload cleanup %s upload %s: %v", *input.Bucket, *input.UploadId, err)
	}

	return
}

func findByPartNumber(fileName string, parts []CompletedPart) (etag string, found bool) {
	partNumber, formatErr := strconv.Atoi(fileName[:4])
	if formatErr != nil {
		return
	}
	x := sort.Search(len(parts), func(i int) bool {
		return parts[i].PartNumber >= partNumber
	})
	if x >= len(parts) {
		return
	}
	if parts[x].PartNumber != partNumber {
		return
	}
	y := 0
	for i, part := range parts[x:] {
		if part.PartNumber == partNumber {
			y = i
		} else {
			break
		}
	}
	return parts[x+y].ETag, true
}

func (s3a *S3ApiServer) abortMultipartUpload(input *s3.AbortMultipartUploadInput) (output *s3.AbortMultipartUploadOutput, code s3err.ErrorCode) {

	glog.V(2).Infof("abortMultipartUpload input %v", input)

	exists, err := s3a.exists(s3a.genUploadsFolder(*input.Bucket), *input.UploadId, true)
	if err != nil {
		glog.V(1).Infof("bucket %s abort upload %s: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrNoSuchUpload
	}
	if exists {
		err = s3a.rm(s3a.genUploadsFolder(*input.Bucket), *input.UploadId, true, true)
	}
	if err != nil {
		glog.V(1).Infof("bucket %s remove upload %s: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrInternalError
	}

	return &s3.AbortMultipartUploadOutput{}, s3err.ErrNone
}

type ListMultipartUploadsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult"`

	// copied from s3.ListMultipartUploadsOutput, the Uploads is not converting to <Upload></Upload>
	Bucket             *string               `type:"string"`
	Delimiter          *string               `type:"string"`
	EncodingType       *string               `type:"string" enum:"EncodingType"`
	IsTruncated        *bool                 `type:"boolean"`
	KeyMarker          *string               `type:"string"`
	MaxUploads         *int64                `type:"integer"`
	NextKeyMarker      *string               `type:"string"`
	NextUploadIdMarker *string               `type:"string"`
	Prefix             *string               `type:"string"`
	UploadIdMarker     *string               `type:"string"`
	Upload             []*s3.MultipartUpload `locationName:"Upload" type:"list" flattened:"true"`
}

func (s3a *S3ApiServer) listMultipartUploads(input *s3.ListMultipartUploadsInput) (output *ListMultipartUploadsResult, code s3err.ErrorCode) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html

	glog.V(2).Infof("listMultipartUploads input %v", input)

	output = &ListMultipartUploadsResult{
		Bucket:       input.Bucket,
		Delimiter:    input.Delimiter,
		EncodingType: input.EncodingType,
		KeyMarker:    input.KeyMarker,
		MaxUploads:   input.MaxUploads,
		Prefix:       input.Prefix,
		IsTruncated:  aws.Bool(false),
	}

	entries, _, err := s3a.list(s3a.genUploadsFolder(*input.Bucket), "", *input.UploadIdMarker, false, math.MaxInt32)
	if err != nil {
		glog.Errorf("listMultipartUploads %s error: %v", *input.Bucket, err)
		return
	}

	uploadsCount := int64(0)
	for _, entry := range entries {
		if entry.Extended != nil {
			key := string(entry.Extended["key"])
			if *input.KeyMarker != "" && *input.KeyMarker != key {
				continue
			}
			if *input.Prefix != "" && !strings.HasPrefix(key, *input.Prefix) {
				continue
			}
			output.Upload = append(output.Upload, &s3.MultipartUpload{
				Key:      objectKey(aws.String(key)),
				UploadId: aws.String(entry.Name),
			})
			uploadsCount += 1
		}
		if uploadsCount >= *input.MaxUploads {
			output.IsTruncated = aws.Bool(true)
			output.NextUploadIdMarker = aws.String(entry.Name)
			break
		}
	}

	return
}

type ListPartsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult"`

	// copied from s3.ListPartsOutput, the Parts is not converting to <Part></Part>
	Bucket               *string    `type:"string"`
	IsTruncated          *bool      `type:"boolean"`
	Key                  *string    `min:"1" type:"string"`
	MaxParts             *int64     `type:"integer"`
	NextPartNumberMarker *int64     `type:"integer"`
	PartNumberMarker     *int64     `type:"integer"`
	Part                 []*s3.Part `locationName:"Part" type:"list" flattened:"true"`
	StorageClass         *string    `type:"string" enum:"StorageClass"`
	UploadId             *string    `type:"string"`
}

func (s3a *S3ApiServer) listObjectParts(input *s3.ListPartsInput) (output *ListPartsResult, code s3err.ErrorCode) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html

	glog.V(2).Infof("listObjectParts input %v", input)

	output = &ListPartsResult{
		Bucket:           input.Bucket,
		Key:              objectKey(input.Key),
		UploadId:         input.UploadId,
		MaxParts:         input.MaxParts,         // the maximum number of parts to return.
		PartNumberMarker: input.PartNumberMarker, // the part number starts after this, exclusive
		StorageClass:     aws.String("STANDARD"),
	}

	entries, isLast, err := s3a.list(s3a.genUploadsFolder(*input.Bucket)+"/"+*input.UploadId, "", fmt.Sprintf("%04d.part", *input.PartNumberMarker), false, uint32(*input.MaxParts))
	if err != nil {
		glog.Errorf("listObjectParts %s %s error: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrNoSuchUpload
	}

	// Note: The upload directory is sort of a marker of the existence of an multipart upload request.
	// So can not just delete empty upload folders.

	output.IsTruncated = aws.Bool(!isLast)

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name, ".part") && !entry.IsDirectory {
			partNumberString := entry.Name[:len(entry.Name)-len(".part")]
			partNumber, err := strconv.Atoi(partNumberString)
			if err != nil {
				glog.Errorf("listObjectParts %s %s parse %s: %v", *input.Bucket, *input.UploadId, entry.Name, err)
				continue
			}
			output.Part = append(output.Part, &s3.Part{
				PartNumber:   aws.Int64(int64(partNumber)),
				LastModified: aws.Time(time.Unix(entry.Attributes.Mtime, 0).UTC()),
				Size:         aws.Int64(int64(filer.FileSize(entry))),
				ETag:         aws.String("\"" + filer.ETag(entry) + "\""),
			})
			if !isLast {
				output.NextPartNumberMarker = aws.Int64(int64(partNumber))
			}
		}
	}

	return
}
