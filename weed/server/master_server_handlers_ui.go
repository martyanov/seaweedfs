package weed_server

import (
	"net/http"
	"time"

	"github.com/hashicorp/raft"

	ui "github.com/seaweedfs/seaweedfs/weed/server/master_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	infos["Max Volume Id"] = ms.Topo.GetMaxVolumeId()

	ms.Topo.RaftAccessLock.RLock()
	defer ms.Topo.RaftAccessLock.RUnlock()

	if ms.Topo.Raft != nil {
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        *raft.Raft
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			util.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.Raft,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusNewRaftTpl.Execute(w, args)
	}
}
