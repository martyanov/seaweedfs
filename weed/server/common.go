package weed_server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/gorilla/mux"
)

var serverStats *stats.ServerStats
var startTime = time.Now()

func init() {
	serverStats = stats.NewServerStats()
	go serverStats.Start()
}

// bodyAllowedForStatus is a copy of http.bodyAllowedForStatus non-exported function.
func bodyAllowedForStatus(status int) bool {
	switch {
	case status >= 100 && status <= 199:
		return false
	case status == http.StatusNoContent:
		return false
	case status == http.StatusNotModified:
		return false
	}
	return true
}

func writeJson(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	if !bodyAllowedForStatus(httpStatus) {
		return
	}

	var bytes []byte
	if obj != nil {
		if r.FormValue("pretty") != "" {
			bytes, err = json.MarshalIndent(obj, "", "  ")
		} else {
			bytes, err = json.Marshal(obj)
		}
	}
	if err != nil {
		return
	}

	if httpStatus >= 400 {
		glog.V(0).Infof("response method:%s URL:%s with httpStatus:%d and JSON:%s",
			r.Method, r.URL.String(), httpStatus, string(bytes))
	}

	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		if httpStatus == http.StatusNotModified {
			return
		}
		_, err = w.Write(bytes)
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if httpStatus == http.StatusNotModified {
			return
		}
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}

	return
}

// wrapper for writeJson - just logs errors
func writeJsonQuiet(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) {
	if err := writeJson(w, r, httpStatus, obj); err != nil {
		glog.V(0).Infof("error writing JSON status %d: %v", httpStatus, err)
		glog.V(1).Infof("JSON content: %+v", obj)
	}
}
func writeJsonError(w http.ResponseWriter, r *http.Request, httpStatus int, err error) {
	m := make(map[string]interface{})
	m["error"] = err.Error()
	glog.V(1).Infof("error JSON response status %d: %s", httpStatus, m["error"])
	writeJsonQuiet(w, r, httpStatus, m)
}

func debug(params ...interface{}) {
	glog.V(4).Infoln(params...)
}

func submitForClientHandler(w http.ResponseWriter, r *http.Request, masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption) {
	m := make(map[string]interface{})
	if r.Method != "POST" {
		writeJsonError(w, r, http.StatusMethodNotAllowed, errors.New("Only submit via POST!"))
		return
	}

	debug("parsing upload file...")
	bytesBuffer := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(bytesBuffer)
	pu, pe := needle.ParseUpload(r, 256*1024*1024, bytesBuffer)
	if pe != nil {
		writeJsonError(w, r, http.StatusBadRequest, pe)
		return
	}

	debug("assigning file id for", pu.FileName)
	r.ParseForm()
	count := uint64(1)
	if r.FormValue("count") != "" {
		count, pe = strconv.ParseUint(r.FormValue("count"), 10, 32)
		if pe != nil {
			writeJsonError(w, r, http.StatusBadRequest, pe)
			return
		}
	}
	ar := &operation.VolumeAssignRequest{
		Count:       count,
		DataCenter:  r.FormValue("dataCenter"),
		Rack:        r.FormValue("rack"),
		Replication: r.FormValue("replication"),
		Collection:  r.FormValue("collection"),
		Ttl:         r.FormValue("ttl"),
		DiskType:    r.FormValue("disk"),
	}
	assignResult, ae := operation.Assign(masterFn, grpcDialOption, ar)
	if ae != nil {
		writeJsonError(w, r, http.StatusInternalServerError, ae)
		return
	}

	url := "http://" + assignResult.Url + "/" + assignResult.Fid
	if pu.ModifiedTime != 0 {
		url = url + "?ts=" + strconv.FormatUint(pu.ModifiedTime, 10)
	}

	debug("upload file to store", url)
	uploadOption := &operation.UploadOption{
		UploadUrl:         url,
		Filename:          pu.FileName,
		Cipher:            false,
		IsInputCompressed: pu.IsGzipped,
		MimeType:          pu.MimeType,
		PairMap:           pu.PairMap,
		Jwt:               assignResult.Auth,
	}
	uploadResult, err := operation.UploadData(pu.Data, uploadOption)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	m["fileName"] = pu.FileName
	m["fid"] = assignResult.Fid
	m["fileUrl"] = assignResult.PublicUrl + "/" + assignResult.Fid
	m["size"] = pu.OriginalDataSize
	m["eTag"] = uploadResult.ETag
	writeJsonQuiet(w, r, http.StatusCreated, m)
	return
}

func parseURLPath(path string) (vid, fid, filename, ext string, isVolumeIdOnly bool) {
	switch strings.Count(path, "/") {
	case 3:
		parts := strings.Split(path, "/")
		vid, fid, filename = parts[1], parts[2], parts[3]
		ext = filepath.Ext(filename)
	case 2:
		parts := strings.Split(path, "/")
		vid, fid = parts[1], parts[2]
		dotIndex := strings.LastIndex(fid, ".")
		if dotIndex > 0 {
			ext = fid[dotIndex:]
			fid = fid[0:dotIndex]
		}
	default:
		sepIndex := strings.LastIndex(path, "/")
		commaIndex := strings.LastIndex(path[sepIndex:], ",")
		if commaIndex <= 0 {
			vid, isVolumeIdOnly = path[sepIndex+1:], true
			return
		}
		dotIndex := strings.LastIndex(path[sepIndex:], ".")
		vid = path[sepIndex+1 : commaIndex]
		fid = path[commaIndex+1:]
		ext = ""
		if dotIndex > 0 {
			fid = path[commaIndex+1 : dotIndex]
			ext = path[dotIndex:]
		}
	}
	return
}

var StaticFS fs.FS

func handleStaticResources(defaultMux *http.ServeMux) {
	defaultMux.Handle("/favicon.ico", http.FileServer(http.FS(StaticFS)))
	defaultMux.Handle("/seaweedfsstatic/", http.StripPrefix("/seaweedfsstatic", http.FileServer(http.FS(StaticFS))))
}

func handleStaticResources2(r *mux.Router) {
	r.Handle("/favicon.ico", http.FileServer(http.FS(StaticFS)))
	r.PathPrefix("/seaweedfsstatic/").Handler(http.StripPrefix("/seaweedfsstatic", http.FileServer(http.FS(StaticFS))))
}

func adjustPassthroughHeaders(w http.ResponseWriter, r *http.Request, filename string) {
	for header, values := range r.Header {
		if normalizedHeader, ok := s3_constants.PassThroughHeaders[strings.ToLower(header)]; ok {
			w.Header()[normalizedHeader] = values
		}
	}
	adjustHeaderContentDisposition(w, r, filename)
}
func adjustHeaderContentDisposition(w http.ResponseWriter, r *http.Request, filename string) {
	if contentDisposition := w.Header().Get("Content-Disposition"); contentDisposition != "" {
		return
	}
	if filename != "" {
		filename = url.QueryEscape(filename)
		contentDisposition := "inline"
		if r.FormValue("dl") != "" {
			if dl, _ := strconv.ParseBool(r.FormValue("dl")); dl {
				contentDisposition = "attachment"
			}
		}
		w.Header().Set("Content-Disposition", contentDisposition+`; filename="`+fileNameEscaper.Replace(filename)+`"`)
	}
}

func processRangeRequest(r *http.Request, w http.ResponseWriter, totalSize int64, mimeType string, writeFn func(writer io.Writer, offset int64, size int64) error) error {
	rangeReq := r.Header.Get("Range")
	bufferedWriter := bufio.NewWriterSize(w, 128*1024)
	defer bufferedWriter.Flush()

	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		if err := writeFn(bufferedWriter, 0, totalSize); err != nil {
			glog.Errorf("processRangeRequest: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("processRangeRequest: %v", err)
		}
		return nil
	}

	//the rest is dealing with partial content request
	//mostly copy from src/pkg/net/http/fs.go
	ranges, err := parseRange(rangeReq, totalSize)
	if err != nil {
		glog.Errorf("processRangeRequest headers: %+v err: %v", w.Header(), err)
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return fmt.Errorf("processRangeRequest header: %v", err)
	}
	if sumRangesSize(ranges) > totalSize {
		// The total number of bytes in all the ranges
		// is larger than the size of the file by
		// itself, so this is probably an attack, or a
		// dumb client.  Ignore the range request.
		return nil
	}
	if len(ranges) == 0 {
		return nil
	}
	if len(ranges) == 1 {
		// RFC 2616, Section 14.16:
		// "When an HTTP message includes the content of a single
		// range (for example, a response to a request for a
		// single range, or to a request for a set of ranges
		// that overlap without any holes), this content is
		// transmitted with a Content-Range header, and a
		// Content-Length header showing the number of bytes
		// actually transferred.
		// ...
		// A response to a request for a single range MUST NOT
		// be sent using the multipart/byteranges media type."
		ra := ranges[0]
		w.Header().Set("Content-Length", strconv.FormatInt(ra.length, 10))
		w.Header().Set("Content-Range", ra.contentRange(totalSize))

		w.WriteHeader(http.StatusPartialContent)
		err = writeFn(bufferedWriter, ra.start, ra.length)
		if err != nil {
			glog.Errorf("processRangeRequest range[0]: %+v err: %v", w.Header(), err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("processRangeRequest range[0]: %v", err)
		}
		return nil
	}

	// process multiple ranges
	for _, ra := range ranges {
		if ra.start > totalSize {
			http.Error(w, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return fmt.Errorf("out of range: %v", err)
		}
	}
	sendSize := rangesMIMESize(ranges, mimeType, totalSize)
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	w.Header().Set("Content-Type", "multipart/byteranges; boundary="+mw.Boundary())
	sendContent := pr
	defer pr.Close() // cause writing goroutine to fail and exit if CopyN doesn't finish.
	go func() {
		for _, ra := range ranges {
			part, e := mw.CreatePart(ra.mimeHeader(mimeType, totalSize))
			if e != nil {
				pw.CloseWithError(e)
				return
			}
			if e = writeFn(part, ra.start, ra.length); e != nil {
				pw.CloseWithError(e)
				return
			}
		}
		mw.Close()
		pw.Close()
	}()
	if w.Header().Get("Content-Encoding") == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(sendSize, 10))
	}
	w.WriteHeader(http.StatusPartialContent)
	if _, err := io.CopyN(bufferedWriter, sendContent, sendSize); err != nil {
		glog.Errorf("processRangeRequest err: %v", err)
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return fmt.Errorf("processRangeRequest err: %v", err)
	}
	return nil
}
