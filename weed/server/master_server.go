package weed_server

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	SequencerType        = "master.sequencer.type"
	SequencerSnowflakeId = "master.sequencer.sequencer_snowflake_id"
)

type MasterOption struct {
	Master            pb.ServerAddress
	MetaFolder        string
	VolumeSizeLimitMB uint32
	VolumePreallocate bool
	// PulseSeconds            int
	DefaultReplicaPlacement string
	GarbageThreshold        float64
	WhiteList               []string
	DisableHttp             bool
	MetricsAddress          string
	MetricsIntervalSec      int
	IsFollower              bool
}

type MasterServer struct {
	master_pb.UnimplementedSeaweedServer
	option *MasterOption
	guard  *security.Guard

	preallocateSize int64

	Topo *topology.Topology
	vg   *topology.VolumeGrowth
	vgCh chan *topology.VolumeGrowRequest

	boundedLeaderChan chan int

	// notifying clients
	clientChansLock sync.RWMutex
	clientChans     map[string]chan *master_pb.KeepConnectedResponse

	grpcDialOption grpc.DialOption

	MasterClient *wdclient.MasterClient

	adminLocks *AdminLocks

	Cluster *cluster.Cluster
}

func NewMasterServer(r *mux.Router, option *MasterOption, peers map[string]pb.ServerAddress) *MasterServer {
	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	v.SetDefault("master.replication.treat_replication_as_minimums", false)
	replicationAsMin := v.GetBool("master.replication.treat_replication_as_minimums")

	v.SetDefault("master.volume_growth.copy_1", 7)
	v.SetDefault("master.volume_growth.copy_2", 6)
	v.SetDefault("master.volume_growth.copy_3", 3)
	v.SetDefault("master.volume_growth.copy_other", 1)
	v.SetDefault("master.volume_growth.threshold", 0.9)

	var preallocateSize int64
	if option.VolumePreallocate {
		preallocateSize = int64(option.VolumeSizeLimitMB) * (1 << 20)
	}

	grpcDialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	ms := &MasterServer{
		option:          option,
		preallocateSize: preallocateSize,
		vgCh:            make(chan *topology.VolumeGrowRequest, 1<<6),
		clientChans:     make(map[string]chan *master_pb.KeepConnectedResponse),
		grpcDialOption:  grpcDialOption,
		MasterClient:    wdclient.NewMasterClient(grpcDialOption, "", cluster.MasterType, option.Master, "", "", peers),
		adminLocks:      NewAdminLocks(),
		Cluster:         cluster.NewCluster(),
	}
	ms.boundedLeaderChan = make(chan int, 16)

	ms.MasterClient.OnPeerUpdate = ms.OnPeerUpdate

	seq := ms.createSequencer(option)
	if nil == seq {
		glog.Fatalf("create sequencer failed.")
	}
	ms.Topo = topology.NewTopology("topo", seq, uint64(ms.option.VolumeSizeLimitMB)*1024*1024, 5, replicationAsMin)
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", ms.option.VolumeSizeLimitMB, "MB")

	ms.guard = security.NewGuard(ms.option.WhiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	handleStaticResources2(r)
	r.HandleFunc("/", ms.proxyToLeader(ms.uiStatusHandler))
	r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
	if !ms.option.DisableHttp {
		r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.guard.WhiteList(ms.dirAssignHandler)))
		r.HandleFunc("/dir/lookup", ms.guard.WhiteList(ms.dirLookupHandler))
		r.HandleFunc("/dir/status", ms.proxyToLeader(ms.guard.WhiteList(ms.dirStatusHandler)))
		r.HandleFunc("/col/delete", ms.proxyToLeader(ms.guard.WhiteList(ms.collectionDeleteHandler)))
		r.HandleFunc("/vol/grow", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeGrowHandler)))
		r.HandleFunc("/vol/status", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeStatusHandler)))
		r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeVacuumHandler)))
		r.HandleFunc("/submit", ms.guard.WhiteList(ms.submitFromMasterServerHandler))
		/*
			r.HandleFunc("/stats/health", ms.guard.WhiteList(statsHealthHandler))
			r.HandleFunc("/stats/counter", ms.guard.WhiteList(statsCounterHandler))
			r.HandleFunc("/stats/memory", ms.guard.WhiteList(statsMemoryHandler))
		*/
		r.HandleFunc("/{fileId}", ms.redirectHandler)
	}

	ms.Topo.StartRefreshWritableVolumes(
		ms.grpcDialOption,
		ms.option.GarbageThreshold,
		v.GetFloat64("master.volume_growth.threshold"),
		ms.preallocateSize,
	)

	ms.ProcessGrowRequest()

	if !option.IsFollower {
		ms.startAdminScripts()
	}

	return ms
}

func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	var raftServerName string
	if raftServer.Raft != nil {
		ms.Topo.Raft = raftServer.Raft
		leaderCh := raftServer.Raft.LeaderCh()
		prevLeader := ms.Topo.Raft.Leader()
		go func() {
			for {
				select {
				case isLeader := <-leaderCh:
					leader := ms.Topo.Raft.Leader()
					glog.V(0).Infof("is leader %+v change event: %+v => %+v", isLeader, prevLeader, leader)
					stats.MasterLeaderChangeCounter.WithLabelValues(fmt.Sprintf("%+v", leader)).Inc()
					prevLeader = leader
				}
			}
		}()
		raftServerName = ms.Topo.Raft.String()
	}
	if ms.Topo.IsLeader() {
		glog.V(0).Infoln("[", raftServerName, "]", "I am the leader!")
	} else {
		if ms.Topo.Raft != nil && ms.Topo.Raft.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.Raft.String(), "]", ms.Topo.Raft.Leader(), "is the leader.")
		}
	}
}

func (ms *MasterServer) proxyToLeader(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.Topo.IsLeader() {
			f(w, r)
			return
		}
		var raftServerLeader string
		if ms.Topo.Raft != nil && ms.Topo.Raft.Leader() != "" {
			raftServerLeader = string(ms.Topo.Raft.Leader())
		}
		if raftServerLeader == "" {
			f(w, r)
			return
		}
		ms.boundedLeaderChan <- 1
		defer func() { <-ms.boundedLeaderChan }()
		targetUrl, err := url.Parse("http://" + raftServerLeader)
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError,
				fmt.Errorf("Leader URL http://%s Parse Error: %v", raftServerLeader, err))
			return
		}
		glog.V(4).Infoln("proxying to leader", raftServerLeader)
		proxy := httputil.NewSingleHostReverseProxy(targetUrl)
		director := proxy.Director
		proxy.Director = func(req *http.Request) {
			actualHost, err := security.GetActualRemoteHost(req)
			if err == nil {
				req.Header.Set("HTTP_X_FORWARDED_FOR", actualHost)
			}
			director(req)
		}
		proxy.Transport = util.Transport
		proxy.ServeHTTP(w, r)
	}
}

func (ms *MasterServer) startAdminScripts() {

	v := util.GetViper()
	adminScripts := v.GetString("master.maintenance.scripts")
	if adminScripts == "" {
		return
	}
	glog.V(0).Infof("adminScripts: %v", adminScripts)

	v.SetDefault("master.maintenance.sleep_minutes", 17)
	sleepMinutes := v.GetInt("master.maintenance.sleep_minutes")

	scriptLines := strings.Split(adminScripts, "\n")
	if !strings.Contains(adminScripts, "lock") {
		scriptLines = append(append([]string{}, "lock"), scriptLines...)
		scriptLines = append(scriptLines, "unlock")
	}

	masterAddress := string(ms.option.Master)

	var shellOptions shell.ShellOptions
	shellOptions.GrpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	shellOptions.Masters = &masterAddress

	shellOptions.Directory = "/"
	emptyFilerGroup := ""
	shellOptions.FilerGroup = &emptyFilerGroup

	commandEnv := shell.NewCommandEnv(&shellOptions)

	reg, _ := regexp.Compile(`'.*?'|".*?"|\S+`)

	go commandEnv.MasterClient.KeepConnectedToMaster()

	go func() {
		for {
			time.Sleep(time.Duration(sleepMinutes) * time.Minute)
			if ms.Topo.IsLeader() && ms.MasterClient.GetMaster() != "" {
				shellOptions.FilerAddress = ms.GetOneFiler(cluster.FilerGroupName(*shellOptions.FilerGroup))
				if shellOptions.FilerAddress == "" {
					continue
				}
				for _, line := range scriptLines {
					for _, c := range strings.Split(line, ";") {
						processEachCmd(reg, c, commandEnv)
					}
				}
			}
		}
	}()
}

func processEachCmd(reg *regexp.Regexp, line string, commandEnv *shell.CommandEnv) {
	cmds := reg.FindAllString(line, -1)
	if len(cmds) == 0 {
		return
	}
	args := make([]string, len(cmds[1:]))
	for i := range args {
		args[i] = strings.Trim(string(cmds[1+i]), "\"'")
	}
	cmd := strings.ToLower(cmds[0])

	for _, c := range shell.Commands {
		if c.Name() == cmd {
			glog.V(0).Infof("executing: %s %v", cmd, args)
			if err := c.Do(args, commandEnv, os.Stdout); err != nil {
				glog.V(0).Infof("error: %v", err)
			}
		}
	}
}

func (ms *MasterServer) createSequencer(option *MasterOption) sequence.Sequencer {
	var seq sequence.Sequencer
	v := util.GetViper()
	seqType := strings.ToLower(v.GetString(SequencerType))
	glog.V(1).Infof("[%s] : [%s]", SequencerType, seqType)
	switch strings.ToLower(seqType) {
	case "snowflake":
		var err error
		snowflakeId := v.GetInt(SequencerSnowflakeId)
		seq, err = sequence.NewSnowflakeSequencer(string(option.Master), snowflakeId)
		if err != nil {
			glog.Error(err)
			seq = nil
		}
	default:
		seq = sequence.NewMemorySequencer()
	}
	return seq
}

func (ms *MasterServer) OnPeerUpdate(update *master_pb.ClusterNodeUpdate, startFrom time.Time) {
	if update.NodeType != cluster.MasterType || ms.Topo.Raft == nil {
		return
	}
	glog.V(4).Infof("OnPeerUpdate: %+v", update)

	peerAddress := pb.ServerAddress(update.Address)
	peerName := string(peerAddress)
	isLeader := ms.Topo.Raft.State() == raft.Leader
	if update.IsAdd && isLeader {
		raftServerFound := false
		for _, server := range ms.Topo.Raft.GetConfiguration().Configuration().Servers {
			if string(server.ID) == peerName {
				raftServerFound = true
			}
		}
		if !raftServerFound {
			glog.V(0).Infof("adding new raft server: %s", peerName)
			ms.Topo.Raft.AddVoter(
				raft.ServerID(peerName),
				raft.ServerAddress(peerAddress.ToGrpcAddress()), 0, 0)
		}
	}
}
