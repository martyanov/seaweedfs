package shell

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"testing"
)

func TestEcDistribution(t *testing.T) {

	topologyInfo := parseOutput(topoData)

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots := collectEcVolumeServersByDc(topologyInfo, "")

	sortEcNodesByFreeslotsDescending(ecNodes)

	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		println("not enough free ec shard slots", totalFreeEcSlots)
	}
	allocatedDataNodes := ecNodes
	if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
		allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	}

	for _, dn := range allocatedDataNodes {
		// fmt.Printf("info %+v %+v\n", dn.info, dn)
		fmt.Printf("=> %+v %+v\n", dn.info.Id, dn.freeEcSlot)
	}

}
