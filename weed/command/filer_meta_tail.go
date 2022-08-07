package command

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdFilerMetaTail.Run = runFilerMetaTail // break init cycle
}

var cmdFilerMetaTail = &Command{
	UsageLine: "filer.meta.tail [-filer=localhost:8888] [-pathPrefix=/]",
	Short:     "see continuous changes on a filer",
	Long: `See continuous changes on a filer.

	weed filer.meta.tail -timeAgo=30h | grep truncate
	weed filer.meta.tail -timeAgo=30h | jq .
	weed filer.meta.tail -timeAgo=30h -untilTimeAgo=20h | jq .
	weed filer.meta.tail -timeAgo=30h | jq .eventNotification.newEntry.name

  `,
}

var (
	tailFiler   = cmdFilerMetaTail.Flag.String("filer", "localhost:8888", "filer hostname:port")
	tailTarget  = cmdFilerMetaTail.Flag.String("pathPrefix", "/", "path to a folder or common prefix for the folders or files on filer")
	tailStart   = cmdFilerMetaTail.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	tailStop    = cmdFilerMetaTail.Flag.Duration("untilTimeAgo", 0, "read until this time ago. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	tailPattern = cmdFilerMetaTail.Flag.String("pattern", "", "full path or just filename pattern, ex: \"/home/?opher\", \"*.pdf\", see https://golang.org/pkg/path/filepath/#Match ")
)

func runFilerMetaTail(cmd *Command, args []string) bool {
	grpcDialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	clientId := util.RandomInt32()

	var filterFunc func(dir, fname string) bool
	if *tailPattern != "" {
		if strings.Contains(*tailPattern, "/") {
			println("watch path pattern", *tailPattern)
			filterFunc = func(dir, fname string) bool {
				matched, err := filepath.Match(*tailPattern, dir+"/"+fname)
				if err != nil {
					fmt.Printf("error: %v", err)
				}
				return matched
			}
		} else {
			println("watch file pattern", *tailPattern)
			filterFunc = func(dir, fname string) bool {
				matched, err := filepath.Match(*tailPattern, fname)
				if err != nil {
					fmt.Printf("error: %v", err)
				}
				return matched
			}
		}
	}

	shouldPrint := func(resp *filer_pb.SubscribeMetadataResponse) bool {
		if filer_pb.IsEmpty(resp) {
			return false
		}
		if filterFunc == nil {
			return true
		}
		if resp.EventNotification.OldEntry != nil && filterFunc(resp.Directory, resp.EventNotification.OldEntry.Name) {
			return true
		}
		if resp.EventNotification.NewEntry != nil && filterFunc(resp.EventNotification.NewParentPath, resp.EventNotification.NewEntry.Name) {
			return true
		}
		return false
	}

	jsonpbMarshaler := jsonpb.Marshaler{
		EmitDefaults: false,
	}
	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		jsonpbMarshaler.Marshal(os.Stdout, resp)
		fmt.Fprintln(os.Stdout)
		return nil
	}

	var untilTsNs int64
	if *tailStop != 0 {
		untilTsNs = time.Now().Add(-*tailStop).UnixNano()
	}

	tailErr := pb.FollowMetadata(pb.ServerAddress(*tailFiler), grpcDialOption, "tail", clientId, 0, *tailTarget, nil,
		time.Now().Add(-*tailStart).UnixNano(), untilTsNs, 0, func(resp *filer_pb.SubscribeMetadataResponse) error {
			if !shouldPrint(resp) {
				return nil
			}
			if err := eachEntryFunc(resp); err != nil {
				return err
			}
			return nil
		}, pb.TrivialOnError)

	if tailErr != nil {
		fmt.Printf("tail %s: %v\n", *tailFiler, tailErr)
	}

	return true
}
