package command

import (
	_ "net/http/pprof"

	_ "github.com/seaweedfs/seaweedfs/weed/filer/cassandra"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb3"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis2"
	_ "github.com/seaweedfs/seaweedfs/weed/remote_storage/s3"
	_ "github.com/seaweedfs/seaweedfs/weed/replication/sink/filersink"
	_ "github.com/seaweedfs/seaweedfs/weed/replication/sink/localsink"
	_ "github.com/seaweedfs/seaweedfs/weed/replication/sink/s3sink"
)
