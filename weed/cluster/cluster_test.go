package cluster

import (
	"strconv"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/stretchr/testify/assert"
)

func TestClusterAddRemoveNodes(t *testing.T) {
	c := NewCluster()

	c.AddClusterNode("", "filer", "", "", rpc.ServerAddress("111:1"), "23.45")
	c.AddClusterNode("", "filer", "", "", rpc.ServerAddress("111:2"), "23.45")
	assert.Equal(t, []rpc.ServerAddress{
		rpc.ServerAddress("111:1"),
		rpc.ServerAddress("111:2"),
	}, c.getGroupMembers("", "filer", true).leaders.GetLeaders())

	c.AddClusterNode("", "filer", "", "", rpc.ServerAddress("111:3"), "23.45")
	c.AddClusterNode("", "filer", "", "", rpc.ServerAddress("111:4"), "23.45")
	assert.Equal(t, []rpc.ServerAddress{
		rpc.ServerAddress("111:1"),
		rpc.ServerAddress("111:2"),
		rpc.ServerAddress("111:3"),
	}, c.getGroupMembers("", "filer", true).leaders.GetLeaders())

	c.AddClusterNode("", "filer", "", "", rpc.ServerAddress("111:5"), "23.45")
	c.AddClusterNode("", "filer", "", "", rpc.ServerAddress("111:6"), "23.45")
	c.RemoveClusterNode("", "filer", rpc.ServerAddress("111:4"))
	assert.Equal(t, []rpc.ServerAddress{
		rpc.ServerAddress("111:1"),
		rpc.ServerAddress("111:2"),
		rpc.ServerAddress("111:3"),
	}, c.getGroupMembers("", "filer", true).leaders.GetLeaders())

	// remove oldest
	c.RemoveClusterNode("", "filer", rpc.ServerAddress("111:1"))
	assert.Equal(t, []rpc.ServerAddress{
		rpc.ServerAddress("111:6"),
		rpc.ServerAddress("111:2"),
		rpc.ServerAddress("111:3"),
	}, c.getGroupMembers("", "filer", true).leaders.GetLeaders())

	// remove oldest
	c.RemoveClusterNode("", "filer", rpc.ServerAddress("111:1"))

}

func TestConcurrentAddRemoveNodes(t *testing.T) {
	c := NewCluster()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			address := strconv.Itoa(i)
			c.AddClusterNode("", "filer", "", "", rpc.ServerAddress(address), "23.45")
		}(i)
	}
	wg.Wait()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			address := strconv.Itoa(i)
			node := c.RemoveClusterNode("", "filer", rpc.ServerAddress(address))

			if len(node) == 0 {
				t.Errorf("TestConcurrentAddRemoveNodes: node[%s] not found", address)
				return
			} else if node[0].ClusterNodeUpdate.Address != address {
				t.Errorf("TestConcurrentAddRemoveNodes: expect:%s, actual:%s", address, node[0].ClusterNodeUpdate.Address)
				return
			}
		}(i)
	}
	wg.Wait()
}
