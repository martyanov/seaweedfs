package topology

import (
	"encoding/json"
	"fmt"

	"github.com/hashicorp/raft"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

type MaxVolumeIdCommand struct {
	MaxVolumeId needle.VolumeId `json:"maxVolumeId"`
}

func NewMaxVolumeIdCommand(value needle.VolumeId) *MaxVolumeIdCommand {
	return &MaxVolumeIdCommand{
		MaxVolumeId: value,
	}
}

func (c *MaxVolumeIdCommand) CommandName() string {
	return "MaxVolumeId"
}

func (c *MaxVolumeIdCommand) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal: %v", err)
	}
	_, err = sink.Write(b)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (c *MaxVolumeIdCommand) Release() {
}
