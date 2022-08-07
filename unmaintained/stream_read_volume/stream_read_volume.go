package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

var (
	volumeServer   = flag.String("volumeServer", "localhost:8080", "a volume server")
	volumeId       = flag.Int("volumeId", -1, "a volume id to stream read")
	grpcDialOption grpc.DialOption
)

func main() {
	flag.Parse()

	grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())

	vid := uint32(*volumeId)

	eachNeedleFunc := func(resp *volume_server_pb.ReadAllNeedlesResponse) error {
		fmt.Printf("%d,%x%08x %d\n", resp.VolumeId, resp.NeedleId, resp.Cookie, len(resp.NeedleBlob))
		return nil
	}

	err := operation.WithVolumeServerClient(true, pb.ServerAddress(*volumeServer), grpcDialOption, func(vs volume_server_pb.VolumeServerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		copyFileClient, err := vs.ReadAllNeedles(ctx, &volume_server_pb.ReadAllNeedlesRequest{
			VolumeIds: []uint32{vid},
		})
		if err != nil {
			return err
		}
		for {
			resp, err := copyFileClient.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if err = eachNeedleFunc(resp); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("read %s: %v\n", *volumeServer, err)
	}

}
