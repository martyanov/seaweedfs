package weed_server

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

type ClusterStatusResult struct {
	IsLeader    bool             `json:"IsLeader,omitempty"`
	Leader      pb.ServerAddress `json:"Leader,omitempty"`
	Peers       []string         `json:"Peers,omitempty"`
	MaxVolumeId needle.VolumeId  `json:"MaxVolumeId,omitempty"`
}

func (s *RaftServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	ret := ClusterStatusResult{
		IsLeader:    s.topo.IsLeader(),
		Peers:       s.Peers(),
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}

	if leader, e := s.topo.Leader(); e == nil {
		ret.Leader = leader
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (s *RaftServer) HealthzHandler(w http.ResponseWriter, r *http.Request) {
	_, err := s.topo.Leader()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (s *RaftServer) StatsRaftHandler(w http.ResponseWriter, r *http.Request) {
	if s.Raft == nil {
		writeJsonQuiet(w, r, http.StatusNotFound, nil)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, s.Raft.Stats())
}
