package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"

	"golang.org/x/exp/slices"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
)

func init() {
	Commands = append(Commands, &commandVolumeCheckDisk{})
}

type commandVolumeCheckDisk struct {
	env *CommandEnv
}

func (c *commandVolumeCheckDisk) Name() string {
	return "volume.check.disk"
}

func (c *commandVolumeCheckDisk) Help() string {
	return `check all replicated volumes to find and fix inconsistencies. It is optional and resource intensive.

	How it works:
	
	find all volumes that are replicated
	  for each volume id, if there are more than 2 replicas, find one pair with the largest 2 in file count.
      for the pair volume A and B
        append entries in A and not in B to B
        append entries in B and not in A to A

`
}

func (c *commandVolumeCheckDisk) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsckCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	slowMode := fsckCommand.Bool("slow", false, "slow mode checks all replicas even file counts are the same")
	verbose := fsckCommand.Bool("v", false, "verbose mode")
	volumeId := fsckCommand.Uint("volumeId", 0, "the volume id")
	applyChanges := fsckCommand.Bool("force", false, "apply the fix")
	nonRepairThreshold := fsckCommand.Float64("nonRepairThreshold", 0.3, "repair when missing keys is not more than this limit")
	if err = fsckCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyChanges, "-force")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	c.env = commandEnv

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)

	// pick 1 pairs of volume replica
	fileCount := func(replica *VolumeReplica) uint64 {
		return replica.info.FileCount - replica.info.DeleteCount
	}

	for _, replicas := range volumeReplicas {
		if *volumeId > 0 && replicas[0].info.Id != uint32(*volumeId) {
			continue
		}
		slices.SortFunc(replicas, func(a, b *VolumeReplica) bool {
			return fileCount(a) > fileCount(b)
		})
		for len(replicas) >= 2 {
			a, b := replicas[0], replicas[1]
			if !*slowMode {
				if fileCount(a) == fileCount(b) {
					replicas = replicas[1:]
					continue
				}
			}
			if a.info.ReadOnly || b.info.ReadOnly {
				fmt.Fprintf(writer, "skipping readonly volume %d on %s and %s\n", a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id)
				replicas = replicas[1:]
				continue
			}

			if err := c.syncTwoReplicas(a, b, *applyChanges, *nonRepairThreshold, *verbose, writer); err != nil {
				fmt.Fprintf(writer, "sync volume %d on %s and %s: %v\n", a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id, err)
			}
			replicas = replicas[1:]
		}
	}

	return nil
}

func (c *commandVolumeCheckDisk) syncTwoReplicas(a *VolumeReplica, b *VolumeReplica, applyChanges bool, nonRepairThreshold float64, verbose bool, writer io.Writer) (err error) {
	aHasChanges, bHasChanges := true, true
	for aHasChanges || bHasChanges {
		if aHasChanges, bHasChanges, err = c.checkBoth(a, b, applyChanges, nonRepairThreshold, verbose, writer); err != nil {
			return err
		}
	}
	return nil
}

func (c *commandVolumeCheckDisk) checkBoth(a *VolumeReplica, b *VolumeReplica, applyChanges bool, nonRepairThreshold float64, verbose bool, writer io.Writer) (aHasChanges bool, bHasChanges bool, err error) {
	aDB, bDB := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer func() {
		aDB.Close()
		bDB.Close()
	}()

	// read index db
	if err = c.readIndexDatabase(aDB, a.info.Collection, a.info.Id, rpc.NewServerAddressFromDataNode(a.location.dataNode), verbose, writer); err != nil {
		return true, true, fmt.Errorf("readIndexDatabase %s volume %d: %v", a.location.dataNode, a.info.Id, err)
	}
	if err := c.readIndexDatabase(bDB, b.info.Collection, b.info.Id, rpc.NewServerAddressFromDataNode(b.location.dataNode), verbose, writer); err != nil {
		return true, true, fmt.Errorf("readIndexDatabase %s volume %d: %v", b.location.dataNode, b.info.Id, err)
	}

	// find and make up the differences
	if aHasChanges, err = c.doVolumeCheckDisk(bDB, aDB, b, a, verbose, writer, applyChanges, nonRepairThreshold); err != nil {
		return true, true, fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %v", b.location.dataNode, a.location.dataNode, b.info.Id, err)
	}
	if bHasChanges, err = c.doVolumeCheckDisk(aDB, bDB, a, b, verbose, writer, applyChanges, nonRepairThreshold); err != nil {
		return true, true, fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %v", a.location.dataNode, b.location.dataNode, a.info.Id, err)
	}
	return
}

func (c *commandVolumeCheckDisk) doVolumeCheckDisk(minuend, subtrahend *needle_map.MemDb, source, target *VolumeReplica, verbose bool, writer io.Writer, applyChanges bool, nonRepairThreshold float64) (hasChanges bool, err error) {

	// find missing keys
	// hash join, can be more efficient
	var missingNeedles []needle_map.NeedleValue
	var counter int
	minuend.AscendingVisit(func(value needle_map.NeedleValue) error {
		counter++
		if _, found := subtrahend.Get(value.Key); !found {
			missingNeedles = append(missingNeedles, value)
		}
		return nil
	})

	fmt.Fprintf(writer, "volume %d %s has %d entries, %s missed %d entries\n", source.info.Id, source.location.dataNode.Id, counter, target.location.dataNode.Id, len(missingNeedles))

	if counter == 0 || len(missingNeedles) == 0 {
		return false, nil
	}

	missingNeedlesFraction := float64(len(missingNeedles)) / float64(counter)
	if missingNeedlesFraction > nonRepairThreshold {
		return false, fmt.Errorf(
			"failed to start repair volume %d, percentage of missing keys is greater than the threshold: %.2f > %.2f",
			source.info.Id, missingNeedlesFraction, nonRepairThreshold)
	}

	for _, needleValue := range missingNeedles {

		needleBlob, err := c.readSourceNeedleBlob(rpc.NewServerAddressFromDataNode(source.location.dataNode), source.info.Id, needleValue)
		if err != nil {
			return hasChanges, err
		}

		if !applyChanges {
			continue
		}

		if verbose {
			fmt.Fprintf(writer, "read %d,%x %s => %s \n", source.info.Id, needleValue.Key, source.location.dataNode.Id, target.location.dataNode.Id)
		}

		hasChanges = true

		if err = c.writeNeedleBlobToTarget(rpc.NewServerAddressFromDataNode(target.location.dataNode), source.info.Id, needleValue, needleBlob); err != nil {
			return hasChanges, err
		}

	}

	return
}

func (c *commandVolumeCheckDisk) readSourceNeedleBlob(sourceVolumeServer rpc.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue) (needleBlob []byte, err error) {

	err = operation.WithVolumeServerClient(false, sourceVolumeServer, c.env.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		resp, err := client.ReadNeedleBlob(context.Background(), &volume_server_pb.ReadNeedleBlobRequest{
			VolumeId: volumeId,
			NeedleId: uint64(needleValue.Key),
			Offset:   needleValue.Offset.ToActualOffset(),
			Size:     int32(needleValue.Size),
		})
		if err != nil {
			return err
		}
		needleBlob = resp.NeedleBlob
		return nil
	})
	return
}

func (c *commandVolumeCheckDisk) writeNeedleBlobToTarget(targetVolumeServer rpc.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue, needleBlob []byte) error {

	return operation.WithVolumeServerClient(false, targetVolumeServer, c.env.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.WriteNeedleBlob(context.Background(), &volume_server_pb.WriteNeedleBlobRequest{
			VolumeId:   volumeId,
			NeedleId:   uint64(needleValue.Key),
			Size:       int32(needleValue.Size),
			NeedleBlob: needleBlob,
		})
		return err
	})

}

func (c *commandVolumeCheckDisk) readIndexDatabase(db *needle_map.MemDb, collection string, volumeId uint32, volumeServer rpc.ServerAddress, verbose bool, writer io.Writer) error {

	var buf bytes.Buffer
	if err := c.copyVolumeIndexFile(collection, volumeId, volumeServer, &buf, verbose, writer); err != nil {
		return err
	}

	if verbose {
		fmt.Fprintf(writer, "load collection %s volume %d index size %d from %s ...\n", collection, volumeId, buf.Len(), volumeServer)
	}

	return db.LoadFromReaderAt(bytes.NewReader(buf.Bytes()))

}

func (c *commandVolumeCheckDisk) copyVolumeIndexFile(collection string, volumeId uint32, volumeServer rpc.ServerAddress, buf *bytes.Buffer, verbose bool, writer io.Writer) error {

	return operation.WithVolumeServerClient(true, volumeServer, c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		ext := ".idx"

		copyFileClient, err := volumeServerClient.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
			VolumeId:                 volumeId,
			Ext:                      ".idx",
			CompactionRevision:       math.MaxUint32,
			StopOffset:               math.MaxInt64,
			Collection:               collection,
			IsEcVolume:               false,
			IgnoreSourceFileNotFound: false,
		})
		if err != nil {
			return fmt.Errorf("failed to start copying volume %d%s: %v", volumeId, ext, err)
		}

		err = writeToBuffer(copyFileClient, buf)
		if err != nil {
			return fmt.Errorf("failed to copy %d%s from %s: %v", volumeId, ext, volumeServer, err)
		}

		return nil

	})
}

func writeToBuffer(client volume_server_pb.VolumeServer_CopyFileClient, buf *bytes.Buffer) error {
	for {
		resp, receiveErr := client.Recv()
		if receiveErr == io.EOF {
			break
		}
		if receiveErr != nil {
			return fmt.Errorf("receiving: %v", receiveErr)
		}
		buf.Write(resp.FileContent)
	}
	return nil
}
