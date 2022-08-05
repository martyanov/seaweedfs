package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type ListBucketResultV2 struct {
	XMLName               xml.Name      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string        `xml:"Name"`
	Prefix                string        `xml:"Prefix"`
	MaxKeys               int           `xml:"MaxKeys"`
	Delimiter             string        `xml:"Delimiter,omitempty"`
	IsTruncated           bool          `xml:"IsTruncated"`
	Contents              []ListEntry   `xml:"Contents,omitempty"`
	CommonPrefixes        []PrefixEntry `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     string        `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string        `xml:"NextContinuationToken,omitempty"`
	KeyCount              int           `xml:"KeyCount"`
	StartAfter            string        `xml:"StartAfter,omitempty"`
}

func (s3a *S3ApiServer) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html

	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("ListObjectsV2Handler %s", bucket)

	originalPrefix, continuationToken, startAfter, delimiter, _, maxKeys := getListObjectsV2Args(r.URL.Query())

	if maxKeys < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxKeys)
		return
	}
	if delimiter != "" && delimiter != "/" {
		s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
		return
	}

	marker := continuationToken
	if continuationToken == "" {
		marker = startAfter
	}

	response, err := s3a.listFilerEntries(bucket, originalPrefix, maxKeys, marker, delimiter)

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if len(response.Contents) == 0 {
		if exists, existErr := s3a.exists(s3a.option.BucketsPath, bucket, true); existErr == nil && !exists {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
	}

	responseV2 := &ListBucketResultV2{
		XMLName:               response.XMLName,
		Name:                  response.Name,
		CommonPrefixes:        response.CommonPrefixes,
		Contents:              response.Contents,
		ContinuationToken:     continuationToken,
		Delimiter:             response.Delimiter,
		IsTruncated:           response.IsTruncated,
		KeyCount:              len(response.Contents) + len(response.CommonPrefixes),
		MaxKeys:               response.MaxKeys,
		NextContinuationToken: response.NextMarker,
		Prefix:                response.Prefix,
		StartAfter:            startAfter,
	}

	writeSuccessResponseXML(w, r, responseV2)
}

func (s3a *S3ApiServer) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html

	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("ListObjectsV1Handler %s", bucket)

	originalPrefix, marker, delimiter, maxKeys := getListObjectsV1Args(r.URL.Query())

	if maxKeys < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxKeys)
		return
	}
	if delimiter != "" && delimiter != "/" {
		s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
		return
	}

	response, err := s3a.listFilerEntries(bucket, originalPrefix, maxKeys, marker, delimiter)

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if len(response.Contents) == 0 {
		if exists, existErr := s3a.exists(s3a.option.BucketsPath, bucket, true); existErr == nil && !exists {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
	}

	writeSuccessResponseXML(w, r, response)
}

func (s3a *S3ApiServer) listFilerEntries(bucket string, originalPrefix string, maxKeys int, marker string, delimiter string) (response ListBucketResult, err error) {
	// convert full path prefix into directory name and prefix for entry name
	reqDir, prefix := filepath.Split(originalPrefix)
	if strings.HasPrefix(reqDir, "/") {
		reqDir = reqDir[1:]
	}
	bucketPrefix := fmt.Sprintf("%s/%s/", s3a.option.BucketsPath, bucket)
	bucketPrefixLen := len(bucketPrefix)
	reqDir = fmt.Sprintf("%s%s", bucketPrefix, reqDir)
	if strings.HasSuffix(reqDir, "/") {
		reqDir = strings.TrimSuffix(reqDir, "/")
	}

	var contents []ListEntry
	var commonPrefixes []PrefixEntry
	var isTruncated bool
	var doErr error
	var nextMarker string

	// check filer
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		_, isTruncated, nextMarker, doErr = s3a.doListFilerEntries(client, reqDir, prefix, maxKeys, marker, delimiter, false, false, bucketPrefixLen, func(dir string, entry *filer_pb.Entry) {
			if entry.IsDirectory {
				if delimiter == "/" {
					commonPrefixes = append(commonPrefixes, PrefixEntry{
						Prefix: fmt.Sprintf("%s/%s/", dir, entry.Name)[bucketPrefixLen:],
					})
				}
				if !(entry.IsDirectoryKeyObject() && strings.HasSuffix(entry.Name, "/")) {
					return
				}
			}
			storageClass := "STANDARD"
			if v, ok := entry.Extended[s3_constants.AmzStorageClass]; ok {
				storageClass = string(v)
			}
			contents = append(contents, ListEntry{
				Key:          fmt.Sprintf("%s/%s", dir, entry.Name)[bucketPrefixLen:],
				LastModified: time.Unix(entry.Attributes.Mtime, 0).UTC(),
				ETag:         "\"" + filer.ETag(entry) + "\"",
				Size:         int64(filer.FileSize(entry)),
				Owner: CanonicalUser{
					ID:          fmt.Sprintf("%x", entry.Attributes.Uid),
					DisplayName: entry.Attributes.UserName,
				},
				StorageClass: StorageClass(storageClass),
			})
		})
		glog.V(4).Infof("end doListFilerEntries isTruncated:%v nextMarker:%v reqDir: %v prefix: %v", isTruncated, nextMarker, reqDir, prefix)
		if doErr != nil {
			return doErr
		}

		if !isTruncated {
			nextMarker = ""
		}

		if len(contents) == 0 && len(commonPrefixes) == 0 && maxKeys > 0 {
			if strings.HasSuffix(originalPrefix, "/") && prefix == "" {
				reqDir, prefix = filepath.Split(strings.TrimSuffix(reqDir, "/"))
				reqDir = strings.TrimSuffix(reqDir, "/")
			}
			_, _, _, doErr = s3a.doListFilerEntries(client, reqDir, prefix, 1, prefix, delimiter, true, false, bucketPrefixLen, func(dir string, entry *filer_pb.Entry) {
				if entry.IsDirectoryKeyObject() && entry.Name == prefix {
					storageClass := "STANDARD"
					if v, ok := entry.Extended[s3_constants.AmzStorageClass]; ok {
						storageClass = string(v)
					}
					contents = append(contents, ListEntry{
						Key:          fmt.Sprintf("%s/%s/", dir, entry.Name)[bucketPrefixLen:],
						LastModified: time.Unix(entry.Attributes.Mtime, 0).UTC(),
						ETag:         "\"" + fmt.Sprintf("%x", entry.Attributes.Md5) + "\"",
						Size:         int64(filer.FileSize(entry)),
						Owner: CanonicalUser{
							ID:          fmt.Sprintf("%x", entry.Attributes.Uid),
							DisplayName: entry.Attributes.UserName,
						},
						StorageClass: StorageClass(storageClass),
					})
				}
			})
			if doErr != nil {
				return doErr
			}
		}

		if len(nextMarker) > 0 {
			nextMarker = nextMarker[bucketPrefixLen:]
		}

		response = ListBucketResult{
			Name:           bucket,
			Prefix:         originalPrefix,
			Marker:         marker,
			NextMarker:     nextMarker,
			MaxKeys:        maxKeys,
			Delimiter:      delimiter,
			IsTruncated:    isTruncated,
			Contents:       contents,
			CommonPrefixes: commonPrefixes,
		}

		return nil
	})

	return
}

func (s3a *S3ApiServer) doListFilerEntries(client filer_pb.SeaweedFilerClient, dir, prefix string, maxKeys int, marker, delimiter string, inclusiveStartFrom bool, subEntries bool, bucketPrefixLen int, eachEntryFn func(dir string, entry *filer_pb.Entry)) (counter int, isTruncated bool, nextMarker string, err error) {
	// invariants
	//   prefix and marker should be under dir, marker may contain "/"
	//   maxKeys should be updated for each recursion

	if prefix == "/" && delimiter == "/" {
		return
	}
	if maxKeys <= 0 {
		return
	}

	if strings.Contains(marker, "/") {
		if strings.HasSuffix(marker, "/") {
			marker = strings.TrimSuffix(marker, "/")
		}
		sepIndex := strings.Index(marker, "/")
		if sepIndex != -1 {
			subPrefix, subMarker := marker[0:sepIndex], marker[sepIndex+1:]
			var subDir string
			if len(dir) > bucketPrefixLen && dir[bucketPrefixLen:] == subPrefix {
				subDir = dir
			} else {
				subDir = fmt.Sprintf("%s/%s", dir, subPrefix)
			}
			subCounter, subIsTruncated, subNextMarker, subErr := s3a.doListFilerEntries(client, subDir, "", maxKeys, subMarker, delimiter, false, false, bucketPrefixLen, eachEntryFn)
			if subErr != nil {
				err = subErr
				return
			}
			counter += subCounter
			isTruncated = isTruncated || subIsTruncated
			maxKeys -= subCounter
			nextMarker = subNextMarker
			// finished processing this sub directory
			marker = subPrefix
		}
	}
	if maxKeys <= 0 {
		return
	}

	// now marker is also a direct child of dir
	request := &filer_pb.ListEntriesRequest{
		Directory:          dir,
		Prefix:             prefix,
		Limit:              uint32(maxKeys + 2), // bucket root directory needs to skip additional s3_constants.MultipartUploadsFolder folder
		StartFromFileName:  marker,
		InclusiveStartFrom: inclusiveStartFrom,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, listErr := client.ListEntries(ctx, request)
	if listErr != nil {
		err = fmt.Errorf("list entires %+v: %v", request, listErr)
		return
	}

	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF {
				break
			} else {
				err = fmt.Errorf("iterating entires %+v: %v", request, recvErr)
				return
			}
		}
		if counter >= maxKeys {
			isTruncated = true
			return
		}
		entry := resp.Entry
		nextMarker = dir + "/" + entry.Name
		if entry.IsDirectory {
			// println("ListEntries", dir, "dir:", entry.Name)
			if entry.Name == s3_constants.MultipartUploadsFolder { // FIXME no need to apply to all directories. this extra also affects maxKeys
				continue
			}
			if delimiter == "" {
				eachEntryFn(dir, entry)
				// println("doListFilerEntries2 dir", dir+"/"+entry.Name, "maxKeys", maxKeys-counter)
				subCounter, subIsTruncated, subNextMarker, subErr := s3a.doListFilerEntries(client, dir+"/"+entry.Name, "", maxKeys-counter, "", delimiter, false, true, bucketPrefixLen, eachEntryFn)
				if subErr != nil {
					err = fmt.Errorf("doListFilerEntries2: %v", subErr)
					return
				}
				// println("doListFilerEntries2 dir", dir+"/"+entry.Name, "maxKeys", maxKeys-counter, "subCounter", subCounter, "subNextMarker", subNextMarker, "subIsTruncated", subIsTruncated)
				if subCounter == 0 && entry.IsDirectoryKeyObject() {
					entry.Name += "/"
					eachEntryFn(dir, entry)
					counter++
				}
				counter += subCounter
				nextMarker = subNextMarker
				if subIsTruncated {
					isTruncated = true
					return
				}
			} else if delimiter == "/" {
				var isEmpty bool
				if !s3a.option.AllowEmptyFolder && !entry.IsDirectoryKeyObject() {
					if isEmpty, err = s3a.ensureDirectoryAllEmpty(client, dir, entry.Name); err != nil {
						glog.Errorf("check empty folder %s: %v", dir, err)
					}
				}
				if !isEmpty {
					nextMarker += "/"
					eachEntryFn(dir, entry)
					counter++
				}
			}
		} else if !(delimiter == "/" && subEntries) {
			// println("ListEntries", dir, "file:", entry.Name)
			eachEntryFn(dir, entry)
			counter++
		}
	}
	return
}

func getListObjectsV2Args(values url.Values) (prefix, token, startAfter, delimiter string, fetchOwner bool, maxkeys int) {
	prefix = values.Get("prefix")
	token = values.Get("continuation-token")
	startAfter = values.Get("start-after")
	delimiter = values.Get("delimiter")
	if values.Get("max-keys") != "" {
		maxkeys, _ = strconv.Atoi(values.Get("max-keys"))
	} else {
		maxkeys = maxObjectListSizeLimit
	}
	fetchOwner = values.Get("fetch-owner") == "true"
	return
}

func getListObjectsV1Args(values url.Values) (prefix, marker, delimiter string, maxkeys int) {
	prefix = values.Get("prefix")
	marker = values.Get("marker")
	delimiter = values.Get("delimiter")
	if values.Get("max-keys") != "" {
		maxkeys, _ = strconv.Atoi(values.Get("max-keys"))
	} else {
		maxkeys = maxObjectListSizeLimit
	}
	return
}

func (s3a *S3ApiServer) ensureDirectoryAllEmpty(filerClient filer_pb.SeaweedFilerClient, parentDir, name string) (isEmpty bool, err error) {
	// println("+ ensureDirectoryAllEmpty", dir, name)
	glog.V(4).Infof("+ isEmpty %s/%s", parentDir, name)
	defer glog.V(4).Infof("- isEmpty %s/%s %v", parentDir, name, isEmpty)
	var fileCounter int
	var subDirs []string
	currentDir := parentDir + "/" + name
	var startFrom string
	var isExhausted bool
	var foundEntry bool
	for fileCounter == 0 && !isExhausted && err == nil {
		err = filer_pb.SeaweedList(filerClient, currentDir, "", func(entry *filer_pb.Entry, isLast bool) error {
			foundEntry = true
			if entry.IsDirectory {
				subDirs = append(subDirs, entry.Name)
			} else {
				fileCounter++
			}
			startFrom = entry.Name
			isExhausted = isExhausted || isLast
			glog.V(4).Infof("    * %s/%s isLast: %t", currentDir, startFrom, isLast)
			return nil
		}, startFrom, false, 8)
		if !foundEntry {
			break
		}
	}

	if err != nil {
		return false, err
	}

	if fileCounter > 0 {
		return false, nil
	}

	for _, subDir := range subDirs {
		isSubEmpty, subErr := s3a.ensureDirectoryAllEmpty(filerClient, currentDir, subDir)
		if subErr != nil {
			return false, subErr
		}
		if !isSubEmpty {
			return false, nil
		}
	}

	glog.V(1).Infof("deleting empty folder %s", currentDir)
	if err = doDeleteEntry(filerClient, parentDir, name, true, true); err != nil {
		return
	}

	return true, nil
}
