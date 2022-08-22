package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/exp/slices"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func init() {
	Commands = append(Commands, &commandVolumeFixReplication{})
}

type commandVolumeFixReplication struct {
	collectionPattern *string
}

func (c *commandVolumeFixReplication) Name() string {
	return "volume.fix.replication"
}

func (c *commandVolumeFixReplication) Help() string {
	return `add or remove replicas to volumes that are missing replicas or over-replicated

	This command finds all over-replicated volumes. If found, it will purge the oldest copies and stop.

	This command also finds all under-replicated volumes, and finds volume servers with free slots.
	If the free slots satisfy the replication requirement, the volume content is copied over and mounted.

	volume.fix.replication -n                             # do not take action
	volume.fix.replication                                # actually deleting or copying the volume files and mount the volume
	volume.fix.replication -collectionPattern=important*  # fix any collections with prefix "important"

	Note:
		* each time this will only add back one replica for each volume id that is under replicated.
		  If there are multiple replicas are missing, e.g. replica count is > 2, you may need to run this multiple times.
		* do not run this too quickly within seconds, since the new volume replica may take a few seconds 
		  to register itself to the master.

`
}

func (c *commandVolumeFixReplication) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volFixReplicationCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.collectionPattern = volFixReplicationCommand.String("collectionPattern", "", "match with wildcard characters '*' and '?'")
	skipChange := volFixReplicationCommand.Bool("n", false, "skip the changes")
	retryCount := volFixReplicationCommand.Int("retry", 0, "how many times to retry")
	volumesPerStep := volFixReplicationCommand.Int("volumesPerStep", 0, "how many volumes to fix in one cycle")

	if err = volFixReplicationCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	takeAction := !*skipChange

	underReplicatedVolumeIdsCount := 1
	for underReplicatedVolumeIdsCount > 0 {
		fixedVolumeReplicas := map[string]int{}

		// collect topology information
		topologyInfo, _, err := collectTopologyInfo(commandEnv, 15*time.Second)
		if err != nil {
			return err
		}

		// find all volumes that needs replication
		// collect all data nodes
		volumeReplicas, allLocations := collectVolumeReplicaLocations(topologyInfo)

		if len(allLocations) == 0 {
			return fmt.Errorf("no data nodes at all")
		}

		// find all under replicated volumes
		var underReplicatedVolumeIds, overReplicatedVolumeIds, misplacedVolumeIds []uint32
		for vid, replicas := range volumeReplicas {
			replica := replicas[0]
			replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replica.info.ReplicaPlacement))
			if replicaPlacement.GetCopyCount() > len(replicas) {
				underReplicatedVolumeIds = append(underReplicatedVolumeIds, vid)
			} else if replicaPlacement.GetCopyCount() < len(replicas) {
				overReplicatedVolumeIds = append(overReplicatedVolumeIds, vid)
				fmt.Fprintf(writer, "volume %d replication %s, but over replicated %+d\n", replica.info.Id, replicaPlacement, len(replicas))
			} else if isMisplaced(replicas, replicaPlacement) {
				misplacedVolumeIds = append(misplacedVolumeIds, vid)
				fmt.Fprintf(writer, "volume %d replication %s is not well placed %+v\n", replica.info.Id, replicaPlacement, replicas)
			}
		}

		if !commandEnv.isLocked() {
			return fmt.Errorf("lock is lost")
		}

		if len(overReplicatedVolumeIds) > 0 {
			if err := c.deleteOneVolume(commandEnv, writer, takeAction, overReplicatedVolumeIds, volumeReplicas, allLocations, pickOneReplicaToDelete); err != nil {
				return err
			}
		}

		if len(misplacedVolumeIds) > 0 {
			if err := c.deleteOneVolume(commandEnv, writer, takeAction, misplacedVolumeIds, volumeReplicas, allLocations, pickOneMisplacedVolume); err != nil {
				return err
			}
		}

		underReplicatedVolumeIdsCount = len(underReplicatedVolumeIds)
		if underReplicatedVolumeIdsCount > 0 {
			// find the most under populated data nodes
			fixedVolumeReplicas, err = c.fixUnderReplicatedVolumes(commandEnv, writer, takeAction, underReplicatedVolumeIds, volumeReplicas, allLocations, *retryCount, *volumesPerStep)
			if err != nil {
				return err
			}
		}

		if *skipChange {
			break
		}

		// check that the topology has been updated
		if len(fixedVolumeReplicas) > 0 {
			fixedVolumes := make([]string, 0, len(fixedVolumeReplicas))
			for k, _ := range fixedVolumeReplicas {
				fixedVolumes = append(fixedVolumes, k)
			}
			volumeIdLocations, err := lookupVolumeIds(commandEnv, fixedVolumes)
			if err != nil {
				return err
			}
			for _, volumeIdLocation := range volumeIdLocations {
				volumeId := volumeIdLocation.VolumeOrFileId
				volumeIdLocationCount := len(volumeIdLocation.Locations)
				i := 0
				for fixedVolumeReplicas[volumeId] >= volumeIdLocationCount {
					fmt.Fprintf(writer, "the number of locations for volume %s has not increased yet, let's wait\n", volumeId)
					time.Sleep(time.Duration(i+1) * time.Second * 7)
					volumeLocIds, err := lookupVolumeIds(commandEnv, []string{volumeId})
					if err != nil {
						return err
					}
					volumeIdLocationCount = len(volumeLocIds[0].Locations)
					if *retryCount <= i {
						return fmt.Errorf("replicas volume %s mismatch in topology", volumeId)
					}
					i += 1
				}
			}
		}
	}
	return nil
}

func collectVolumeReplicaLocations(topologyInfo *master_pb.TopologyInfo) (map[uint32][]*VolumeReplica, []location) {
	volumeReplicas := make(map[uint32][]*VolumeReplica)
	var allLocations []location
	eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		loc := newLocation(dc, string(rack), dn)
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				volumeReplicas[v.Id] = append(volumeReplicas[v.Id], &VolumeReplica{
					location: &loc,
					info:     v,
				})
			}
		}
		allLocations = append(allLocations, loc)
	})
	return volumeReplicas, allLocations
}

type SelectOneVolumeFunc func(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) *VolumeReplica

func (c *commandVolumeFixReplication) deleteOneVolume(commandEnv *CommandEnv, writer io.Writer, takeAction bool, overReplicatedVolumeIds []uint32, volumeReplicas map[uint32][]*VolumeReplica, allLocations []location, selectOneVolumeFn SelectOneVolumeFunc) error {
	for _, vid := range overReplicatedVolumeIds {
		replicas := volumeReplicas[vid]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replicas[0].info.ReplicaPlacement))

		replica := selectOneVolumeFn(replicas, replicaPlacement)

		// check collection name pattern
		if *c.collectionPattern != "" {
			matched, err := filepath.Match(*c.collectionPattern, replica.info.Collection)
			if err != nil {
				return fmt.Errorf("match pattern %s with collection %s: %v", *c.collectionPattern, replica.info.Collection, err)
			}
			if !matched {
				break
			}
		}

		collectionIsMismatch := false
		for _, volumeReplica := range replicas {
			if volumeReplica.info.Collection != replica.info.Collection {
				fmt.Fprintf(writer, "skip delete volume %d as collection %s is mismatch: %s\n", replica.info.Id, replica.info.Collection, volumeReplica.info.Collection)
				collectionIsMismatch = true
			}
		}
		if collectionIsMismatch {
			continue
		}

		fmt.Fprintf(writer, "deleting volume %d from %s ...\n", replica.info.Id, replica.location.dataNode.Id)

		if !takeAction {
			break
		}

		if err := deleteVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(replica.info.Id), rpc.NewServerAddressFromDataNode(replica.location.dataNode)); err != nil {
			return fmt.Errorf("deleting volume %d from %s : %v", replica.info.Id, replica.location.dataNode.Id, err)
		}

	}
	return nil
}

func (c *commandVolumeFixReplication) fixUnderReplicatedVolumes(commandEnv *CommandEnv, writer io.Writer, takeAction bool, underReplicatedVolumeIds []uint32, volumeReplicas map[uint32][]*VolumeReplica, allLocations []location, retryCount int, volumesPerStep int) (fixedVolumes map[string]int, err error) {
	fixedVolumes = map[string]int{}
	if len(underReplicatedVolumeIds) > volumesPerStep && volumesPerStep > 0 {
		underReplicatedVolumeIds = underReplicatedVolumeIds[0:volumesPerStep]
	}
	for _, vid := range underReplicatedVolumeIds {
		for i := 0; i < retryCount+1; i++ {
			if err = c.fixOneUnderReplicatedVolume(commandEnv, writer, takeAction, volumeReplicas, vid, allLocations); err == nil {
				if takeAction {
					fixedVolumes[strconv.FormatUint(uint64(vid), 10)] = len(volumeReplicas[vid])
				}
				break
			}
		}
	}
	return fixedVolumes, nil
}

func (c *commandVolumeFixReplication) fixOneUnderReplicatedVolume(commandEnv *CommandEnv, writer io.Writer, takeAction bool, volumeReplicas map[uint32][]*VolumeReplica, vid uint32, allLocations []location) error {
	replicas := volumeReplicas[vid]
	replica := pickOneReplicaToCopyFrom(replicas)
	replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replica.info.ReplicaPlacement))
	foundNewLocation := false
	hasSkippedCollection := false
	keepDataNodesSorted(allLocations, types.ToDiskType(replica.info.DiskType))
	fn := capacityByFreeVolumeCount(types.ToDiskType(replica.info.DiskType))
	for _, dst := range allLocations {
		// check whether data nodes satisfy the constraints
		if fn(dst.dataNode) > 0 && satisfyReplicaPlacement(replicaPlacement, replicas, dst) {
			// check collection name pattern
			if *c.collectionPattern != "" {
				matched, err := filepath.Match(*c.collectionPattern, replica.info.Collection)
				if err != nil {
					return fmt.Errorf("match pattern %s with collection %s: %v", *c.collectionPattern, replica.info.Collection, err)
				}
				if !matched {
					hasSkippedCollection = true
					break
				}
			}

			// ask the volume server to replicate the volume
			foundNewLocation = true
			fmt.Fprintf(writer, "replicating volume %d %s from %s to dataNode %s ...\n", replica.info.Id, replicaPlacement, replica.location.dataNode.Id, dst.dataNode.Id)

			if !takeAction {
				// adjust free volume count
				dst.dataNode.DiskInfos[replica.info.DiskType].FreeVolumeCount--
				break
			}

			err := operation.WithVolumeServerClient(false, rpc.NewServerAddressFromDataNode(dst.dataNode), commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				stream, replicateErr := volumeServerClient.VolumeCopy(context.Background(), &volume_server_pb.VolumeCopyRequest{
					VolumeId:       replica.info.Id,
					SourceDataNode: string(rpc.NewServerAddressFromDataNode(replica.location.dataNode)),
				})
				if replicateErr != nil {
					return fmt.Errorf("copying from %s => %s : %v", replica.location.dataNode.Id, dst.dataNode.Id, replicateErr)
				}
				for {
					resp, recvErr := stream.Recv()
					if recvErr != nil {
						if recvErr == io.EOF {
							break
						} else {
							return recvErr
						}
					}
					if resp.ProcessedBytes > 0 {
						fmt.Fprintf(writer, "volume %d processed %d bytes\n", replica.info.Id, resp.ProcessedBytes)
					}
				}

				return nil
			})

			if err != nil {
				return err
			}

			// adjust free volume count
			dst.dataNode.DiskInfos[replica.info.DiskType].FreeVolumeCount--
			break
		}
	}

	if !foundNewLocation && !hasSkippedCollection {
		fmt.Fprintf(writer, "failed to place volume %d replica as %s, existing:%+v\n", replica.info.Id, replicaPlacement, len(replicas))
	}
	return nil
}

func keepDataNodesSorted(dataNodes []location, diskType types.DiskType) {
	fn := capacityByFreeVolumeCount(diskType)
	slices.SortFunc(dataNodes, func(a, b location) bool {
		return fn(a.dataNode) > fn(b.dataNode)
	})
}

/*
  if on an existing data node {
    return false
  }
  if different from existing dcs {
    if lack on different dcs {
      return true
    }else{
      return false
    }
  }
  if not on primary dc {
    return false
  }
  if different from existing racks {
    if lack on different racks {
      return true
    }else{
      return false
    }
  }
  if not on primary rack {
    return false
  }
  if lacks on same rack {
    return true
  } else {
    return false
  }
*/
func satisfyReplicaPlacement(replicaPlacement *super_block.ReplicaPlacement, replicas []*VolumeReplica, possibleLocation location) bool {

	existingDataCenters, _, existingDataNodes := countReplicas(replicas)

	if _, found := existingDataNodes[possibleLocation.String()]; found {
		// avoid duplicated volume on the same data node
		return false
	}

	primaryDataCenters, _ := findTopKeys(existingDataCenters)

	// ensure data center count is within limit
	if _, found := existingDataCenters[possibleLocation.DataCenter()]; !found {
		// different from existing dcs
		if len(existingDataCenters) < replicaPlacement.DiffDataCenterCount+1 {
			// lack on different dcs
			return true
		} else {
			// adding this would go over the different dcs limit
			return false
		}
	}
	// now this is same as one of the existing data center
	if !isAmong(possibleLocation.DataCenter(), primaryDataCenters) {
		// not on one of the primary dcs
		return false
	}

	// now this is one of the primary dcs
	primaryDcRacks := make(map[string]int)
	for _, replica := range replicas {
		if replica.location.DataCenter() != possibleLocation.DataCenter() {
			continue
		}
		primaryDcRacks[replica.location.Rack()] += 1
	}
	primaryRacks, _ := findTopKeys(primaryDcRacks)
	sameRackCount := primaryDcRacks[possibleLocation.Rack()]

	// ensure rack count is within limit
	if _, found := primaryDcRacks[possibleLocation.Rack()]; !found {
		// different from existing racks
		if len(primaryDcRacks) < replicaPlacement.DiffRackCount+1 {
			// lack on different racks
			return true
		} else {
			// adding this would go over the different racks limit
			return false
		}
	}
	// now this is same as one of the existing racks
	if !isAmong(possibleLocation.Rack(), primaryRacks) {
		// not on the primary rack
		return false
	}

	// now this is on the primary rack

	// different from existing data nodes
	if sameRackCount < replicaPlacement.SameRackCount+1 {
		// lack on same rack
		return true
	} else {
		// adding this would go over the same data node limit
		return false
	}

}

func findTopKeys(m map[string]int) (topKeys []string, max int) {
	for k, c := range m {
		if max < c {
			topKeys = topKeys[:0]
			topKeys = append(topKeys, k)
			max = c
		} else if max == c {
			topKeys = append(topKeys, k)
		}
	}
	return
}

func isAmong(key string, keys []string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

type VolumeReplica struct {
	location *location
	info     *master_pb.VolumeInformationMessage
}

type location struct {
	dc       string
	rack     string
	dataNode *master_pb.DataNodeInfo
}

func newLocation(dc, rack string, dataNode *master_pb.DataNodeInfo) location {
	return location{
		dc:       dc,
		rack:     rack,
		dataNode: dataNode,
	}
}

func (l location) String() string {
	return fmt.Sprintf("%s %s %s", l.dc, l.rack, l.dataNode.Id)
}

func (l location) Rack() string {
	return fmt.Sprintf("%s %s", l.dc, l.rack)
}

func (l location) DataCenter() string {
	return l.dc
}

func pickOneReplicaToCopyFrom(replicas []*VolumeReplica) *VolumeReplica {
	mostRecent := replicas[0]
	for _, replica := range replicas {
		if replica.info.ModifiedAtSecond > mostRecent.info.ModifiedAtSecond {
			mostRecent = replica
		}
	}
	return mostRecent
}

func countReplicas(replicas []*VolumeReplica) (diffDc, diffRack, diffNode map[string]int) {
	diffDc = make(map[string]int)
	diffRack = make(map[string]int)
	diffNode = make(map[string]int)
	for _, replica := range replicas {
		diffDc[replica.location.DataCenter()] += 1
		diffRack[replica.location.Rack()] += 1
		diffNode[replica.location.String()] += 1
	}
	return
}

func pickOneReplicaToDelete(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) *VolumeReplica {
	slices.SortFunc(replicas, func(a, b *VolumeReplica) bool {
		if a.info.Size != b.info.Size {
			return a.info.Size < b.info.Size
		}
		if a.info.ModifiedAtSecond != b.info.ModifiedAtSecond {
			return a.info.ModifiedAtSecond < b.info.ModifiedAtSecond
		}
		if a.info.CompactRevision != b.info.CompactRevision {
			return a.info.CompactRevision < b.info.CompactRevision
		}
		return false
	})

	return replicas[0]

}

// check and fix misplaced volumes

func isMisplaced(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) bool {

	for i := 0; i < len(replicas); i++ {
		others := otherThan(replicas, i)
		if satisfyReplicaPlacement(replicaPlacement, others, *replicas[i].location) {
			return false
		}
	}

	return true

}

func otherThan(replicas []*VolumeReplica, index int) (others []*VolumeReplica) {
	for i := 0; i < len(replicas); i++ {
		if index != i {
			others = append(others, replicas[i])
		}
	}
	return
}

func pickOneMisplacedVolume(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) (toDelete *VolumeReplica) {

	var deletionCandidates []*VolumeReplica
	for i := 0; i < len(replicas); i++ {
		others := otherThan(replicas, i)
		if !isMisplaced(others, replicaPlacement) {
			deletionCandidates = append(deletionCandidates, replicas[i])
		}
	}
	if len(deletionCandidates) > 0 {
		return pickOneReplicaToDelete(deletionCandidates, replicaPlacement)
	}

	return pickOneReplicaToDelete(replicas, replicaPlacement)

}
