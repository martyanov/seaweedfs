package weed_server

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"net/http"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func (ms *MasterServer) lookupVolumeId(vids []string, collection string) (volumeLocations map[string]operation.LookupResult) {
	volumeLocations = make(map[string]operation.LookupResult)
	for _, vid := range vids {
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
		if _, ok := volumeLocations[vid]; ok {
			continue
		}
		volumeLocations[vid] = ms.findVolumeLocation(collection, vid)
	}
	return
}

// If "fileId" is provided, this returns the fileId location and a JWT to update or delete the file.
// If "volumeId" is provided, this only returns the volumeId location
func (ms *MasterServer) dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	if vid != "" {
		// backward compatible
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
	}
	fileId := r.FormValue("fileId")
	if fileId != "" {
		commaSep := strings.Index(fileId, ",")
		if commaSep > 0 {
			vid = fileId[0:commaSep]
		}
	}
	collection := r.FormValue("collection") // optional, but can be faster if too many collections
	location := ms.findVolumeLocation(collection, vid)
	httpStatus := http.StatusOK
	if location.Error != "" || location.Locations == nil {
		httpStatus = http.StatusNotFound
	} else {
		forRead := r.FormValue("read")
		isRead := forRead == "yes"
		ms.maybeAddJwtAuthorization(w, fileId, !isRead)
	}
	writeJsonQuiet(w, r, httpStatus, location)
}

// findVolumeLocation finds the volume location from master topo if it is leader,
// or from master client if not leader
func (ms *MasterServer) findVolumeLocation(collection, vid string) operation.LookupResult {
	var locations []operation.Location
	var err error
	if ms.Topo.IsLeader() {
		volumeId, newVolumeIdErr := needle.NewVolumeId(vid)
		if newVolumeIdErr != nil {
			err = fmt.Errorf("Unknown volume id %s", vid)
		} else {
			machines := ms.Topo.Lookup(collection, volumeId)
			for _, loc := range machines {
				locations = append(locations, operation.Location{
					Url: loc.Url(), PublicUrl: loc.PublicUrl, DataCenter: loc.GetDataCenterId(),
				})
			}
		}
	} else {
		machines, getVidLocationsErr := ms.MasterClient.GetVidLocations(vid)
		for _, loc := range machines {
			locations = append(locations, operation.Location{
				Url: loc.Url, PublicUrl: loc.PublicUrl, DataCenter: loc.DataCenter,
			})
		}
		err = getVidLocationsErr
	}
	if len(locations) == 0 && err == nil {
		err = fmt.Errorf("volume id %s not found", vid)
	}
	ret := operation.LookupResult{
		VolumeOrFileId: vid,
		Locations:      locations,
	}
	if err != nil {
		ret.Error = err.Error()
	}
	return ret
}

func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	stats.AssignRequest()
	requestedCount, e := strconv.ParseUint(r.FormValue("count"), 10, 64)
	if e != nil || requestedCount == 0 {
		requestedCount = 1
	}

	writableVolumeCount, e := strconv.Atoi(r.FormValue("writableVolumeCount"))
	if e != nil {
		writableVolumeCount = 0
	}

	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
		return
	}

	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

	if !vl.HasGrowRequest() && vl.ShouldGrowVolumes(option) {
		glog.V(0).Infof("dirAssign volume growth %v from %v", option.String(), r.RemoteAddr)
		if ms.Topo.AvailableSpaceFor(option) <= 0 {
			writeJsonQuiet(w, r, http.StatusNotFound, operation.AssignResult{Error: "No free volumes left for " + option.String()})
			return
		}
		errCh := make(chan error, 1)
		vl.AddGrowRequest()
		ms.vgCh <- &topology.VolumeGrowRequest{
			Option: option,
			Count:  writableVolumeCount,
			ErrCh:  errCh,
		}
		if err := <-errCh; err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("cannot grow volume group! %v", err))
			return
		}
	}
	fid, count, dnList, err := ms.Topo.PickForWrite(requestedCount, option)
	if err == nil {
		ms.maybeAddJwtAuthorization(w, fid, true)
		dn := dnList.Head()
		writeJsonQuiet(w, r, http.StatusOK, operation.AssignResult{Fid: fid, Url: dn.Url(), PublicUrl: dn.PublicUrl, Count: count})
	} else {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
	}
}

func (ms *MasterServer) maybeAddJwtAuthorization(w http.ResponseWriter, fileId string, isWrite bool) {
	if fileId == "" {
		return
	}
	var encodedJwt security.EncodedJwt
	if isWrite {
		encodedJwt = security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fileId)
	} else {
		encodedJwt = security.GenJwtForVolumeServer(ms.guard.ReadSigningKey, ms.guard.ReadExpiresAfterSec, fileId)
	}
	if encodedJwt == "" {
		return
	}

	w.Header().Set("Authorization", "BEARER "+string(encodedJwt))
}
