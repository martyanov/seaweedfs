package wdclient

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/master_pb"
)

type MasterClient struct {
	FilerGroup        string
	clientType        string
	clientHost        rpc.ServerAddress
	rack              string
	currentMaster     rpc.ServerAddress
	currentMasterLock sync.RWMutex
	masters           map[string]rpc.ServerAddress
	grpcDialOption    grpc.DialOption

	*vidMap
	vidMapCacheSize  int
	OnPeerUpdate     func(update *master_pb.ClusterNodeUpdate, startFrom time.Time)
	OnPeerUpdateLock sync.RWMutex
}

func NewMasterClient(grpcDialOption grpc.DialOption, filerGroup string, clientType string, clientHost rpc.ServerAddress, clientDataCenter string, rack string, masters map[string]rpc.ServerAddress) *MasterClient {
	return &MasterClient{
		FilerGroup:      filerGroup,
		clientType:      clientType,
		clientHost:      clientHost,
		rack:            rack,
		masters:         masters,
		grpcDialOption:  grpcDialOption,
		vidMap:          newVidMap(clientDataCenter),
		vidMapCacheSize: 5,
	}
}

func (mc *MasterClient) SetOnPeerUpdateFn(onPeerUpdate func(update *master_pb.ClusterNodeUpdate, startFrom time.Time)) {
	mc.OnPeerUpdateLock.Lock()
	mc.OnPeerUpdate = onPeerUpdate
	mc.OnPeerUpdateLock.Unlock()
}

func (mc *MasterClient) GetLookupFileIdFunction() LookupFileIdFunctionType {
	return mc.LookupFileIdWithFallback
}

func (mc *MasterClient) LookupFileIdWithFallback(fileId string) (fullUrls []string, err error) {
	fullUrls, err = mc.vidMap.LookupFileId(fileId)
	if err == nil && len(fullUrls) > 0 {
		return
	}
	err = rpc.WithMasterClient(false, mc.GetMaster(), mc.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: []string{fileId},
		})
		if err != nil {
			return fmt.Errorf("LookupVolume failed: %v", err)
		}
		for vid, vidLocation := range resp.VolumeIdLocations {
			for _, vidLoc := range vidLocation.Locations {
				loc := Location{
					Url:        vidLoc.Url,
					PublicUrl:  vidLoc.PublicUrl,
					GrpcPort:   int(vidLoc.GrpcPort),
					DataCenter: vidLoc.DataCenter,
				}
				mc.vidMap.addLocation(uint32(vid), loc)
				httpUrl := "http://" + loc.Url + "/" + fileId
				// Prefer same data center
				if mc.DataCenter != "" && mc.DataCenter == loc.DataCenter {
					fullUrls = append([]string{httpUrl}, fullUrls...)
				} else {
					fullUrls = append(fullUrls, httpUrl)
				}
			}
		}
		return nil
	})
	return
}

func (mc *MasterClient) getCurrentMaster() rpc.ServerAddress {
	mc.currentMasterLock.RLock()
	defer mc.currentMasterLock.RUnlock()
	return mc.currentMaster
}

func (mc *MasterClient) setCurrentMaster(master rpc.ServerAddress) {
	mc.currentMasterLock.Lock()
	mc.currentMaster = master
	mc.currentMasterLock.Unlock()
}

func (mc *MasterClient) GetMaster() rpc.ServerAddress {
	mc.WaitUntilConnected()
	return mc.getCurrentMaster()
}

func (mc *MasterClient) GetMasters() map[string]rpc.ServerAddress {
	mc.WaitUntilConnected()
	return mc.masters
}

func (mc *MasterClient) WaitUntilConnected() {
	for {
		if mc.getCurrentMaster() != "" {
			return
		}
		time.Sleep(time.Duration(rand.Int31n(200)) * time.Millisecond)
	}
}

func (mc *MasterClient) KeepConnectedToMaster() {
	glog.V(1).Infof("%s.%s masterClient bootstraps with masters %v", mc.FilerGroup, mc.clientType, mc.masters)
	for {
		mc.tryAllMasters()
		time.Sleep(time.Second)
	}
}

func (mc *MasterClient) FindLeaderFromOtherPeers(myMasterAddress rpc.ServerAddress) (leader string) {
	for _, master := range mc.masters {
		if master == myMasterAddress {
			continue
		}
		if grpcErr := rpc.WithMasterClient(false, master, mc.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
			defer cancel()
			resp, err := client.GetMasterConfiguration(ctx, &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return err
			}
			leader = resp.Leader
			return nil
		}); grpcErr != nil {
			glog.V(0).Infof("connect to %s: %v", master, grpcErr)
		}
		if leader != "" {
			glog.V(0).Infof("existing leader is %s", leader)
			return
		}
	}
	glog.V(0).Infof("No existing leader found!")
	return
}

func (mc *MasterClient) tryAllMasters() {
	var nextHintedLeader rpc.ServerAddress
	for _, master := range mc.masters {
		nextHintedLeader = mc.tryConnectToMaster(master)
		for nextHintedLeader != "" {
			nextHintedLeader = mc.tryConnectToMaster(nextHintedLeader)
		}
		mc.setCurrentMaster("")
	}
}

func (mc *MasterClient) tryConnectToMaster(master rpc.ServerAddress) (nextHintedLeader rpc.ServerAddress) {
	glog.V(1).Infof("%s.%s masterClient Connecting to master %v", mc.FilerGroup, mc.clientType, master)
	stats.MasterClientConnectCounter.WithLabelValues("total").Inc()
	gprcErr := rpc.WithMasterClient(true, master, mc.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.KeepConnected(ctx)
		if err != nil {
			glog.V(1).Infof("%s.%s masterClient failed to keep connected to %s: %v", mc.FilerGroup, mc.clientType, master, err)
			stats.MasterClientConnectCounter.WithLabelValues(stats.FailedToKeepConnected).Inc()
			return err
		}

		if err = stream.Send(&master_pb.KeepConnectedRequest{
			FilerGroup:    mc.FilerGroup,
			DataCenter:    mc.DataCenter,
			Rack:          mc.rack,
			ClientType:    mc.clientType,
			ClientAddress: string(mc.clientHost),
			Version:       util.Version(),
		}); err != nil {
			glog.V(0).Infof("%s.%s masterClient failed to send to %s: %v", mc.FilerGroup, mc.clientType, master, err)
			stats.MasterClientConnectCounter.WithLabelValues(stats.FailedToSend).Inc()
			return err
		}
		glog.V(1).Infof("%s.%s masterClient Connected to %v", mc.FilerGroup, mc.clientType, master)

		resp, err := stream.Recv()
		if err != nil {
			glog.V(0).Infof("%s.%s masterClient failed to receive from %s: %v", mc.FilerGroup, mc.clientType, master, err)
			stats.MasterClientConnectCounter.WithLabelValues(stats.FailedToReceive).Inc()
			return err
		}

		// check if it is the leader to determine whether to reset the vidMap
		if resp.VolumeLocation != nil {
			if resp.VolumeLocation.Leader != "" && string(master) != resp.VolumeLocation.Leader {
				glog.V(0).Infof("master %v redirected to leader %v", master, resp.VolumeLocation.Leader)
				nextHintedLeader = rpc.ServerAddress(resp.VolumeLocation.Leader)
				stats.MasterClientConnectCounter.WithLabelValues(stats.RedirectedToLeader).Inc()
				return nil
			}
			mc.resetVidMap()
			mc.updateVidMap(resp)
		} else {
			mc.resetVidMap()
		}
		mc.setCurrentMaster(master)

		for {
			resp, err := stream.Recv()
			if err != nil {
				glog.V(0).Infof("%s.%s masterClient failed to receive from %s: %v", mc.FilerGroup, mc.clientType, master, err)
				stats.MasterClientConnectCounter.WithLabelValues(stats.FailedToReceive).Inc()
				return err
			}

			if resp.VolumeLocation != nil {
				// maybe the leader is changed
				if resp.VolumeLocation.Leader != "" && string(mc.GetMaster()) != resp.VolumeLocation.Leader {
					glog.V(0).Infof("currentMaster %v redirected to leader %v", mc.GetMaster(), resp.VolumeLocation.Leader)
					nextHintedLeader = rpc.ServerAddress(resp.VolumeLocation.Leader)
					stats.MasterClientConnectCounter.WithLabelValues(stats.RedirectedToLeader).Inc()
					return nil
				}
				mc.updateVidMap(resp)
			}

			if resp.ClusterNodeUpdate != nil {
				update := resp.ClusterNodeUpdate
				mc.OnPeerUpdateLock.RLock()
				if mc.OnPeerUpdate != nil {
					if update.FilerGroup == mc.FilerGroup {
						if update.IsAdd {
							glog.V(0).Infof("+ %s.%s %s leader:%v\n", update.FilerGroup, update.NodeType, update.Address, update.IsLeader)
						} else {
							glog.V(0).Infof("- %s.%s %s leader:%v\n", update.FilerGroup, update.NodeType, update.Address, update.IsLeader)
						}
						stats.MasterClientConnectCounter.WithLabelValues(stats.OnPeerUpdate).Inc()
						mc.OnPeerUpdate(update, time.Now())
					}
				}
				mc.OnPeerUpdateLock.RUnlock()
			}
		}
	})
	if gprcErr != nil {
		stats.MasterClientConnectCounter.WithLabelValues(stats.Failed).Inc()
		glog.V(1).Infof("%s.%s masterClient failed to connect with master %v: %v", mc.FilerGroup, mc.clientType, master, gprcErr)
	}
	return
}

func (mc *MasterClient) updateVidMap(resp *master_pb.KeepConnectedResponse) {
	// process new volume location
	glog.V(1).Infof("updateVidMap() resp.VolumeLocation.DataCenter %v", resp.VolumeLocation.DataCenter)
	loc := Location{
		Url:        resp.VolumeLocation.Url,
		PublicUrl:  resp.VolumeLocation.PublicUrl,
		DataCenter: resp.VolumeLocation.DataCenter,
		GrpcPort:   int(resp.VolumeLocation.GrpcPort),
	}
	for _, newVid := range resp.VolumeLocation.NewVids {
		glog.V(1).Infof("%s.%s: %s masterClient adds volume %d", mc.FilerGroup, mc.clientType, loc.Url, newVid)
		mc.addLocation(newVid, loc)
	}
	for _, deletedVid := range resp.VolumeLocation.DeletedVids {
		glog.V(1).Infof("%s.%s: %s masterClient removes volume %d", mc.FilerGroup, mc.clientType, loc.Url, deletedVid)
		mc.deleteLocation(deletedVid, loc)
	}
	for _, newEcVid := range resp.VolumeLocation.NewEcVids {
		glog.V(1).Infof("%s.%s: %s masterClient adds ec volume %d", mc.FilerGroup, mc.clientType, loc.Url, newEcVid)
		mc.addEcLocation(newEcVid, loc)
	}
	for _, deletedEcVid := range resp.VolumeLocation.DeletedEcVids {
		glog.V(1).Infof("%s.%s: %s masterClient removes ec volume %d", mc.FilerGroup, mc.clientType, loc.Url, deletedEcVid)
		mc.deleteEcLocation(deletedEcVid, loc)
	}
}

func (mc *MasterClient) WithClient(streamingMode bool, fn func(client master_pb.SeaweedClient) error) error {
	return util.Retry("master grpc", func() error {
		return rpc.WithMasterClient(streamingMode, mc.GetMaster(), mc.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			return fn(client)
		})
	})
}

func (mc *MasterClient) resetVidMap() {
	tail := &vidMap{
		vid2Locations:   mc.vid2Locations,
		ecVid2Locations: mc.ecVid2Locations,
		DataCenter:      mc.DataCenter,
		cache:           mc.cache,
	}

	nvm := newVidMap(mc.DataCenter)
	nvm.cache = tail
	mc.vidMap = nvm

	//trim
	for i := 0; i < mc.vidMapCacheSize && tail.cache != nil; i++ {
		if i == mc.vidMapCacheSize-1 {
			tail.cache = nil
		} else {
			tail = tail.cache
		}
	}
}
