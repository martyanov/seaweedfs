package command

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

type ServerOptions struct {
	cpuprofile *string
	memprofile *string
	debug      *bool
	debugPort  *int
	v          VolumeServerOptions
}

var (
	serverOptions ServerOptions
	masterOptions MasterOptions
	filerOptions  FilerOptions
	s3Options     S3Options
	iamOptions    IamOptions
)

func init() {
	cmdServer.Run = runServer // break init cycle
}

var cmdServer = &Command{
	UsageLine: "server -dir=/tmp -volume.max=5 -ip=server_name",
	Short:     "start a master server, a volume server, and optionally a filer and a S3 gateway",
	Long: `start both a volume server to provide storage spaces
  and a master server to provide volume=>location mapping service and sequence number of file ids

  This is provided as a convenient way to start both volume server and master server.
  The servers acts exactly the same as starting them separately.
  So other volume servers can connect to this master server also.

  Optionally, a filer server can be started.
  Also optionally, a S3 gateway can be started.

  `,
}

var (
	serverIp                  = cmdServer.Flag.String("ip", util.DetectedHostAddress(), "ip or server name, also used as identifier")
	serverBindIp              = cmdServer.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
	serverTimeout             = cmdServer.Flag.Int("idleTimeout", 30, "connection idle seconds")
	serverDataCenter          = cmdServer.Flag.String("dataCenter", "", "current volume server's data center name")
	serverRack                = cmdServer.Flag.String("rack", "", "current volume server's rack name")
	serverWhiteListOption     = cmdServer.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	serverDisableHttp         = cmdServer.Flag.Bool("disableHttp", false, "disable http requests, only gRPC operations are allowed.")
	volumeDataFolders         = cmdServer.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	volumeMaxDataVolumeCounts = cmdServer.Flag.String("volume.max", "8", "maximum numbers of volumes, count[,count]... If set to zero, the limit will be auto configured as free disk space divided by volume size.")
	volumeMinFreeSpacePercent = cmdServer.Flag.String("volume.minFreeSpacePercent", "1", "minimum free disk space (default to 1%). Low disk space will mark all volumes as ReadOnly (deprecated, use minFreeSpace instead).")
	volumeMinFreeSpace        = cmdServer.Flag.String("volume.minFreeSpace", "", "min free disk space (value<=100 as percentage like 1, other as human readable bytes, like 10GiB). Low disk space will mark all volumes as ReadOnly.")
	serverMetricsHttpPort     = cmdServer.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")

	// pulseSeconds              = cmdServer.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	isStartingMasterServer = cmdServer.Flag.Bool("master", true, "whether to start master server")
	isStartingVolumeServer = cmdServer.Flag.Bool("volume", true, "whether to start volume server")
	isStartingFiler        = cmdServer.Flag.Bool("filer", false, "whether to start filer")
	isStartingS3           = cmdServer.Flag.Bool("s3", false, "whether to start S3 gateway")
	isStartingIam          = cmdServer.Flag.Bool("iam", false, "whether to start IAM service")

	False = false
)

func init() {
	serverOptions.cpuprofile = cmdServer.Flag.String("cpuprofile", "", "cpu profile output file")
	serverOptions.memprofile = cmdServer.Flag.String("memprofile", "", "memory profile output file")
	serverOptions.debug = cmdServer.Flag.Bool("debug", false, "serves runtime profiling data, e.g., http://localhost:6060/debug/pprof/goroutine?debug=2")
	serverOptions.debugPort = cmdServer.Flag.Int("debug.port", 6060, "http port for debugging")

	masterOptions.port = cmdServer.Flag.Int("master.port", 9333, "master server http listen port")
	masterOptions.portGrpc = cmdServer.Flag.Int("master.port.grpc", 0, "master server grpc listen port")
	masterOptions.metaFolder = cmdServer.Flag.String("master.dir", "", "data directory to store meta data, default to same as -dir specified")
	masterOptions.peers = cmdServer.Flag.String("master.peers", "", "all master nodes in comma separated ip:masterPort list")
	masterOptions.volumeSizeLimitMB = cmdServer.Flag.Uint("master.volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	masterOptions.volumePreallocate = cmdServer.Flag.Bool("master.volumePreallocate", false, "Preallocate disk space for volumes.")
	masterOptions.defaultReplication = cmdServer.Flag.String("master.defaultReplication", "", "Default replication type if not specified.")
	masterOptions.garbageThreshold = cmdServer.Flag.Float64("garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	masterOptions.metricsAddress = cmdServer.Flag.String("metrics.address", "", "Prometheus gateway address")
	masterOptions.metricsIntervalSec = cmdServer.Flag.Int("metrics.intervalSeconds", 15, "Prometheus push interval in seconds")
	masterOptions.raftResumeState = cmdServer.Flag.Bool("resumeState", false, "resume previous state on start master server")
	masterOptions.heartbeatInterval = cmdServer.Flag.Duration("master.heartbeatInterval", 300*time.Millisecond, "heartbeat interval of master servers, and will be randomly multiplied by [1, 1.25)")
	masterOptions.electionTimeout = cmdServer.Flag.Duration("master.electionTimeout", 10*time.Second, "election timeout of master servers")

	filerOptions.filerGroup = cmdServer.Flag.String("filer.filerGroup", "", "share metadata with other filers in the same filerGroup")
	filerOptions.collection = cmdServer.Flag.String("filer.collection", "", "all data will be stored in this collection")
	filerOptions.port = cmdServer.Flag.Int("filer.port", 8888, "filer server http listen port")
	filerOptions.portGrpc = cmdServer.Flag.Int("filer.port.grpc", 0, "filer server grpc listen port")
	filerOptions.publicPort = cmdServer.Flag.Int("filer.port.public", 0, "filer server public http listen port")
	filerOptions.defaultReplicaPlacement = cmdServer.Flag.String("filer.defaultReplicaPlacement", "", "default replication type. If not specified, use master setting.")
	filerOptions.disableDirListing = cmdServer.Flag.Bool("filer.disableDirListing", false, "turn off directory listing")
	filerOptions.maxMB = cmdServer.Flag.Int("filer.maxMB", 4, "split files larger than the limit")
	filerOptions.dirListingLimit = cmdServer.Flag.Int("filer.dirListLimit", 1000, "limit sub dir listing size")
	filerOptions.cipher = cmdServer.Flag.Bool("filer.encryptVolumeData", false, "encrypt data on volume servers")
	filerOptions.saveToFilerLimit = cmdServer.Flag.Int("filer.saveToFilerLimit", 0, "Small files smaller than this limit can be cached in filer store.")
	filerOptions.concurrentUploadLimitMB = cmdServer.Flag.Int("filer.concurrentUploadLimitMB", 64, "limit total concurrent upload size")
	filerOptions.localSocket = cmdServer.Flag.String("filer.localSocket", "", "default to /tmp/seaweedfs-filer-<port>.sock")
	filerOptions.showUIDirectoryDelete = cmdServer.Flag.Bool("filer.ui.deleteDir", true, "enable filer UI show delete directory button")
	filerOptions.downloadMaxMBps = cmdServer.Flag.Int("filer.downloadMaxMBps", 0, "download max speed for each download request, in MB per second")

	serverOptions.v.port = cmdServer.Flag.Int("volume.port", 8080, "volume server http listen port")
	serverOptions.v.portGrpc = cmdServer.Flag.Int("volume.port.grpc", 0, "volume server grpc listen port")
	serverOptions.v.publicPort = cmdServer.Flag.Int("volume.port.public", 0, "volume server public port")
	serverOptions.v.indexType = cmdServer.Flag.String("volume.index", "memory", "Choose [memory|leveldb|leveldbMedium|leveldbLarge] mode for memory~performance balance.")
	serverOptions.v.diskType = cmdServer.Flag.String("volume.disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	serverOptions.v.readMode = cmdServer.Flag.String("volume.readMode", "proxy", "[local|proxy|redirect] how to deal with non-local volume: 'not found|read in remote node|redirect volume location'.")
	serverOptions.v.compactionMBPerSecond = cmdServer.Flag.Int("volume.compactionMBps", 0, "limit compaction speed in mega bytes per second")
	serverOptions.v.fileSizeLimitMB = cmdServer.Flag.Int("volume.fileSizeLimitMB", 256, "limit file size to avoid out of memory")
	serverOptions.v.concurrentUploadLimitMB = cmdServer.Flag.Int("volume.concurrentUploadLimitMB", 64, "limit total concurrent upload size")
	serverOptions.v.concurrentDownloadLimitMB = cmdServer.Flag.Int("volume.concurrentDownloadLimitMB", 64, "limit total concurrent download size")
	serverOptions.v.publicUrl = cmdServer.Flag.String("volume.publicUrl", "", "publicly accessible address")
	serverOptions.v.preStopSeconds = cmdServer.Flag.Int("volume.preStopSeconds", 10, "number of seconds between stop send heartbeats and stop volume server")
	serverOptions.v.pprof = cmdServer.Flag.Bool("volume.pprof", false, "enable pprof http handlers. precludes --memprofile and --cpuprofile")
	serverOptions.v.idxFolder = cmdServer.Flag.String("volume.dir.idx", "", "directory to store .idx files")
	serverOptions.v.inflightUploadDataTimeout = cmdServer.Flag.Duration("volume.inflightUploadDataTimeout", 60*time.Second, "inflight upload data wait timeout of volume servers")
	serverOptions.v.hasSlowRead = cmdServer.Flag.Bool("volume.hasSlowRead", false, "<experimental> if true, this prevents slow reads from blocking other requests, but large file read P99 latency will increase.")
	serverOptions.v.readBufferSizeMB = cmdServer.Flag.Int("volume.readBufferSizeMB", 4, "<experimental> larger values can optimize query performance but will increase some memory usage,Use with hasSlowRead normally")

	s3Options.port = cmdServer.Flag.Int("s3.port", 8333, "s3 server http listen port")
	s3Options.portGrpc = cmdServer.Flag.Int("s3.port.grpc", 0, "s3 server grpc listen port")
	s3Options.domainName = cmdServer.Flag.String("s3.domainName", "", "suffix of the host name in comma separated list, {bucket}.{domainName}")
	s3Options.config = cmdServer.Flag.String("s3.config", "", "path to the config file")
	s3Options.allowEmptyFolder = cmdServer.Flag.Bool("s3.allowEmptyFolder", true, "allow empty folders")
	s3Options.allowDeleteBucketNotEmpty = cmdServer.Flag.Bool("s3.allowDeleteBucketNotEmpty", true, "allow recursive deleting all entries along with bucket")

	iamOptions.port = cmdServer.Flag.Int("iam.port", 8111, "iam server http listen port")
}

func runServer(cmd *Command, args []string) bool {

	if *serverOptions.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *serverOptions.debugPort), nil)
	}

	util.LoadConfiguration("security", false)
	util.LoadConfiguration("master", false)

	grace.SetupProfiling(*serverOptions.cpuprofile, *serverOptions.memprofile)

	if *isStartingS3 {
		*isStartingFiler = true
	}
	if *isStartingIam {
		*isStartingFiler = true
	}

	if *isStartingMasterServer {
		_, peerList := checkPeers(*serverIp, *masterOptions.port, *masterOptions.portGrpc, *masterOptions.peers)
		peers := strings.Join(rpc.ToAddressStrings(peerList), ",")
		masterOptions.peers = &peers
	}

	if *serverBindIp == "" {
		serverBindIp = serverIp
	}

	// ip address
	masterOptions.ip = serverIp
	masterOptions.ipBind = serverBindIp
	filerOptions.masters = rpc.ServerAddresses(*masterOptions.peers).ToAddressMap()
	filerOptions.ip = serverIp
	filerOptions.bindIp = serverBindIp
	s3Options.bindIp = serverBindIp
	iamOptions.ip = serverBindIp
	iamOptions.masters = masterOptions.peers
	serverOptions.v.ip = serverIp
	serverOptions.v.bindIp = serverBindIp
	serverOptions.v.masters = rpc.ServerAddresses(*masterOptions.peers).ToAddresses()
	serverOptions.v.idleConnectionTimeout = serverTimeout
	serverOptions.v.dataCenter = serverDataCenter
	serverOptions.v.rack = serverRack

	// serverOptions.v.pulseSeconds = pulseSeconds
	// masterOptions.pulseSeconds = pulseSeconds

	masterOptions.whiteList = serverWhiteListOption

	filerOptions.dataCenter = serverDataCenter
	filerOptions.rack = serverRack
	s3Options.dataCenter = serverDataCenter
	filerOptions.disableHttp = serverDisableHttp
	masterOptions.disableHttp = serverDisableHttp

	filerAddress := string(rpc.NewServerAddress(*serverIp, *filerOptions.port, *filerOptions.portGrpc))
	s3Options.filer = &filerAddress
	iamOptions.filer = &filerAddress

	go stats_collect.StartMetricsServer(*serverMetricsHttpPort)

	folders := strings.Split(*volumeDataFolders, ",")

	if *masterOptions.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("masterVolumeSizeLimitMB should be less than 30000")
	}

	if *masterOptions.metaFolder == "" {
		*masterOptions.metaFolder = folders[0]
	}
	if err := util.TestFolderWritable(util.ResolvePath(*masterOptions.metaFolder)); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir=\"%s\") Writable: %s", *masterOptions.metaFolder, err)
	}
	filerOptions.defaultLevelDbDirectory = masterOptions.metaFolder

	serverWhiteList := util.StringSplit(*serverWhiteListOption, ",")

	if *isStartingFiler {
		go func() {
			time.Sleep(1 * time.Second)
			filerOptions.startFiler()
		}()
	}

	if *isStartingS3 {
		go func() {
			time.Sleep(2 * time.Second)
			s3Options.localFilerSocket = filerOptions.localSocket
			s3Options.startS3Server()
		}()
	}

	if *isStartingIam {
		go func() {
			time.Sleep(2 * time.Second)
			iamOptions.startIamServer()
		}()
	}

	// start volume server
	if *isStartingVolumeServer {
		minFreeSpaces := util.MustParseMinFreeSpace(*volumeMinFreeSpace, *volumeMinFreeSpacePercent)
		go serverOptions.v.startVolumeServer(*volumeDataFolders, *volumeMaxDataVolumeCounts, *serverWhiteListOption, minFreeSpaces)
	}

	if *isStartingMasterServer {
		go startMaster(masterOptions, serverWhiteList)
	}

	select {}
}
