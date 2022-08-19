package shell

import (
	"context"
	"flag"
	"io"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeMount{})
}

type commandVolumeMount struct {
}

func (c *commandVolumeMount) Name() string {
	return "volume.mount"
}

func (c *commandVolumeMount) Help() string {
	return `mount a volume from one volume server

	volume.mount -node <volume server host:port> -volumeId <volume id>

	This command mounts a volume from one volume server.

`
}

func (c *commandVolumeMount) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volMountCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volMountCommand.Int("volumeId", 0, "the volume id")
	nodeStr := volMountCommand.String("node", "", "the volume server <host>:<port>")
	if err = volMountCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	sourceVolumeServer := rpc.ServerAddress(*nodeStr)

	volumeId := needle.VolumeId(*volumeIdInt)

	return mountVolume(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer)

}

func mountVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer rpc.ServerAddress) (err error) {
	return operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, mountErr := volumeServerClient.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{
			VolumeId: uint32(volumeId),
		})
		return mountErr
	})
}
