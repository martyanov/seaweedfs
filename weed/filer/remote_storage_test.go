package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/stretchr/testify/assert"
)

func TestFilerRemoteStorage_FindRemoteStorageClient(t *testing.T) {
	conf := &rpc.RemoteConfiguration{
		Name: "s7",
		Type: "s3",
	}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf

	rs.mapDirectoryToRemoteStorage("/a/b/c", &rpc.RemoteStorageLocation{
		Name:   "s7",
		Bucket: "some",
		Path:   "/dir",
	})

	_, _, found := rs.FindRemoteStorageClient("/a/b/c/d/e/f")
	assert.Equal(t, true, found, "find storage client")

	_, _, found2 := rs.FindRemoteStorageClient("/a/b")
	assert.Equal(t, false, found2, "should not find storage client")

	_, _, found3 := rs.FindRemoteStorageClient("/a/b/c")
	assert.Equal(t, false, found3, "should not find storage client")

	_, _, found4 := rs.FindRemoteStorageClient("/a/b/cc")
	assert.Equal(t, false, found4, "should not find storage client")
}
