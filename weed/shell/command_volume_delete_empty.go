package shell

import (
	"flag"
	"io"
	"log"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeDeleteEmpty{})
}

type commandVolumeDeleteEmpty struct {
}

func (c *commandVolumeDeleteEmpty) Name() string {
	return "volume.deleteEmpty"
}

func (c *commandVolumeDeleteEmpty) Help() string {
	return `delete empty volumes from all volume servers

	volume.deleteEmpty -quietFor=24h -force

	This command deletes all empty volumes from one volume server.

`
}

func (c *commandVolumeDeleteEmpty) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	quietPeriod := volDeleteCommand.Duration("quietFor", 24*time.Hour, "select empty volumes with no recent writes, avoid newly created ones")
	applyBalancing := volDeleteCommand.Bool("force", false, "apply to delete empty volumes")
	if err = volDeleteCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyBalancing, "-force")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	quietSeconds := int64(*quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.Size <= 8 && v.ModifiedAtSecond+quietSeconds < nowUnixSeconds {
					if *applyBalancing {
						log.Printf("deleting empty volume %d from %s", v.Id, dn.Id)
						if deleteErr := deleteVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(v.Id), rpc.NewServerAddressFromDataNode(dn)); deleteErr != nil {
							err = deleteErr
						}
						continue
					} else {
						log.Printf("empty volume %d from %s", v.Id, dn.Id)
					}
				}
			}
		}
	})

	return
}
