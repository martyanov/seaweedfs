package weed_server

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

const (
	ldbFile            = "logs.dat"
	sdbFile            = "stable.dat"
	updatePeersTimeout = 15 * time.Minute
)

type RaftServerOption struct {
	GrpcDialOption    grpc.DialOption
	Peers             map[string]pb.ServerAddress
	ServerAddr        pb.ServerAddress
	DataDir           string
	Topo              *topology.Topology
	RaftResumeState   bool
	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	RaftBootstrap     bool
}

type RaftServer struct {
	peers            map[string]pb.ServerAddress // initial peers to join with
	Raft             *raft.Raft
	TransportManager *transport.Manager
	dataDir          string
	serverAddr       pb.ServerAddress
	topo             *topology.Topology
}

type StateMachine struct {
	topo *topology.Topology
}

var _ raft.FSM = &StateMachine{}

func (s StateMachine) Save() ([]byte, error) {
	state := topology.MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}
	glog.V(1).Infof("Save raft state %+v", state)
	return json.Marshal(state)
}

func (s StateMachine) Recovery(data []byte) error {
	state := topology.MaxVolumeIdCommand{}
	err := json.Unmarshal(data, &state)
	if err != nil {
		return err
	}
	glog.V(1).Infof("Recovery raft state %+v", state)
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)
	return nil
}

func (s *StateMachine) Apply(l *raft.Log) interface{} {
	before := s.topo.GetMaxVolumeId()
	state := topology.MaxVolumeIdCommand{}
	err := json.Unmarshal(l.Data, &state)
	if err != nil {
		return err
	}
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)

	glog.V(1).Infoln("max volume id", before, "==>", s.topo.GetMaxVolumeId())
	return nil
}

func (s *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return &topology.MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}, nil
}

func (s *StateMachine) Restore(r io.ReadCloser) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	if err := s.Recovery(b); err != nil {
		return err
	}
	return nil
}

func (s *RaftServer) Peers() (members []string) {
	if s.Raft != nil {
		cfg := s.Raft.GetConfiguration()
		for _, p := range cfg.Configuration().Servers {
			members = append(members, string(p.ID))
		}
	}
	return
}

func getPeerIdx(self pb.ServerAddress, mapPeers map[string]pb.ServerAddress) int {
	peers := make([]pb.ServerAddress, 0, len(mapPeers))
	for _, peer := range mapPeers {
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool {
		return strings.Compare(string(peers[i]), string(peers[j])) < 0
	})
	for i, peer := range peers {
		if string(peer) == string(self) {
			return i
		}
	}
	return -1
}

func (s *RaftServer) AddPeersConfiguration() (cfg raft.Configuration) {
	for _, peer := range s.peers {
		cfg.Servers = append(cfg.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(peer),
			Address:  raft.ServerAddress(peer.ToGrpcAddress()),
		})
	}
	return cfg
}

func (s *RaftServer) UpdatePeers() {
	for {
		select {
		case isLeader := <-s.Raft.LeaderCh():
			if isLeader {
				peerLeader := string(s.serverAddr)
				existsPeerName := make(map[string]bool)
				for _, server := range s.Raft.GetConfiguration().Configuration().Servers {
					if string(server.ID) == peerLeader {
						continue
					}
					existsPeerName[string(server.ID)] = true
				}
				for _, peer := range s.peers {
					peerName := string(peer)
					if peerName == peerLeader || existsPeerName[peerName] {
						continue
					}
					glog.V(0).Infof("adding new peer: %s", peerName)
					s.Raft.AddVoter(
						raft.ServerID(peerName), raft.ServerAddress(peer.ToGrpcAddress()), 0, 0)
				}
				for peer, _ := range existsPeerName {
					if _, found := s.peers[peer]; !found {
						glog.V(0).Infof("removing old peer: %s", peer)
						s.Raft.RemoveServer(raft.ServerID(peer), 0, 0)
					}
				}
				if _, found := s.peers[peerLeader]; !found {
					glog.V(0).Infof("removing old leader peer: %s", peerLeader)
					s.Raft.RemoveServer(raft.ServerID(peerLeader), 0, 0)
				}
			}
			return
		case <-time.After(updatePeersTimeout):
			return
		}
	}
}

func NewRaftServer(option *RaftServerOption) (*RaftServer, error) {
	s := &RaftServer{
		peers:      option.Peers,
		serverAddr: option.ServerAddr,
		dataDir:    option.DataDir,
		topo:       option.Topo,
	}

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(s.serverAddr) // TODO maybee the IP:port address will change
	c.HeartbeatTimeout = time.Duration(float64(option.HeartbeatInterval) * (rand.Float64()*0.25 + 1))
	c.ElectionTimeout = option.ElectionTimeout
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		c.LeaderLeaseTimeout = c.HeartbeatTimeout
	}
	if glog.V(4) {
		c.LogLevel = "Debug"
	} else if glog.V(2) {
		c.LogLevel = "Info"
	} else if glog.V(1) {
		c.LogLevel = "Warn"
	} else if glog.V(0) {
		c.LogLevel = "Error"
	}

	if option.RaftBootstrap {
		os.RemoveAll(path.Join(s.dataDir, ldbFile))
		os.RemoveAll(path.Join(s.dataDir, sdbFile))
		os.RemoveAll(path.Join(s.dataDir, "snapshots"))
	}
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshots"), os.ModePerm); err != nil {
		return nil, err
	}
	baseDir := s.dataDir

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, ldbFile))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, sdbFile))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	s.TransportManager = transport.New(raft.ServerAddress(s.serverAddr), []grpc.DialOption{option.GrpcDialOption})

	stateMachine := StateMachine{topo: option.Topo}
	s.Raft, err = raft.NewRaft(c, &stateMachine, ldb, sdb, fss, s.TransportManager.Transport())
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}
	if option.RaftBootstrap || len(s.Raft.GetConfiguration().Configuration().Servers) == 0 {
		cfg := s.AddPeersConfiguration()
		// Need to get lock, in case all servers do this at the same time.
		peerIdx := getPeerIdx(s.serverAddr, s.peers)
		timeSpeep := time.Duration(float64(c.LeaderLeaseTimeout) * (rand.Float64()*0.25 + 1) * float64(peerIdx))
		glog.V(0).Infof("Bootstrapping idx: %d sleep: %v new cluster: %+v", peerIdx, timeSpeep, cfg)
		time.Sleep(timeSpeep)
		f := s.Raft.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	} else {
		go s.UpdatePeers()
	}

	ticker := time.NewTicker(c.HeartbeatTimeout * 10)
	if glog.V(4) {
		go func() {
			for {
				select {
				case <-ticker.C:
					cfuture := s.Raft.GetConfiguration()
					if err = cfuture.Error(); err != nil {
						glog.Fatalf("error getting config: %s", err)
					}
					configuration := cfuture.Configuration()
					glog.V(4).Infof("Showing peers known by %s:\n%+v", s.Raft.String(), configuration.Servers)
				}
			}
		}()
	}

	return s, nil
}
