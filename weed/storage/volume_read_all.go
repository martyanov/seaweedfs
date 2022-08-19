package storage

import (
	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

type VolumeFileScanner4ReadAll struct {
	Stream volume_server_pb.VolumeServer_ReadAllNeedlesServer
	V      *Volume
}

func (scanner *VolumeFileScanner4ReadAll) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	return nil

}
func (scanner *VolumeFileScanner4ReadAll) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4ReadAll) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {

	nv, ok := scanner.V.nm.Get(n.Id)
	if !ok {
		return nil
	}
	if nv.Offset.ToActualOffset() != offset {
		return nil
	}

	sendErr := scanner.Stream.Send(&volume_server_pb.ReadAllNeedlesResponse{
		VolumeId:   uint32(scanner.V.Id),
		NeedleId:   uint64(n.Id),
		Cookie:     uint32(n.Cookie),
		NeedleBlob: n.Data,
	})
	if sendErr != nil {
		return sendErr
	}
	return nil
}
