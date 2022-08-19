package command

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

var (
	filerCat FilerCatOptions
)

type FilerCatOptions struct {
	grpcDialOption grpc.DialOption
	filerAddress   rpc.ServerAddress
	filerClient    filer_pb.SeaweedFilerClient
	output         *string
}

func (fco *FilerCatOptions) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return func(fileId string) (targetUrls []string, err error) {
		vid := filer.VolumeId(fileId)
		resp, err := fco.filerClient.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
			VolumeIds: []string{vid},
		})
		if err != nil {
			return nil, err
		}
		locations := resp.LocationsMap[vid]
		for _, loc := range locations.Locations {
			targetUrls = append(targetUrls, fmt.Sprintf("http://%s/%s", loc.Url, fileId))
		}
		return
	}
}

func init() {
	cmdFilerCat.Run = runFilerCat // break init cycle
	filerCat.output = cmdFilerCat.Flag.String("o", "", "write to file instead of stdout")
}

var cmdFilerCat = &Command{
	UsageLine: "filer.cat [-o <file>] http://localhost:8888/path/to/file",
	Short:     "copy one file to local",
	Long: `read one file to stdout or write to a file

`,
}

func runFilerCat(cmd *Command, args []string) bool {
	if len(args) == 0 {
		return false
	}
	filerSource := args[len(args)-1]

	filerUrl, err := url.Parse(filerSource)
	if err != nil {
		fmt.Printf("The last argument should be a URL on filer: %v\n", err)
		return false
	}
	urlPath := filerUrl.Path
	if strings.HasSuffix(urlPath, "/") {
		fmt.Printf("The last argument should be a file: %v\n", err)
		return false
	}

	filerCat.filerAddress = rpc.ServerAddress(filerUrl.Host)
	filerCat.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())

	dir, name := util.FullPath(urlPath).DirAndName()

	writer := os.Stdout
	if *filerCat.output != "" {

		fmt.Printf("saving %s to %s\n", filerSource, *filerCat.output)

		f, err := os.OpenFile(*filerCat.output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			fmt.Printf("open file %s: %v\n", *filerCat.output, err)
			return false
		}
		defer f.Close()
		writer = f
	}

	rpc.WithFilerClient(false, filerCat.filerAddress, filerCat.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Name:      name,
			Directory: dir,
		}
		respLookupEntry, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			return err
		}

		if len(respLookupEntry.Entry.Content) > 0 {
			_, err = writer.Write(respLookupEntry.Entry.Content)
			return err
		}

		filerCat.filerClient = client

		return filer.StreamContent(&filerCat, writer, respLookupEntry.Entry.Chunks, 0, int64(filer.FileSize(respLookupEntry.Entry)))

	})

	return true
}
