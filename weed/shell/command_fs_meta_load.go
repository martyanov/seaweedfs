package shell

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMetaLoad{})
}

type commandFsMetaLoad struct {
}

func (c *commandFsMetaLoad) Name() string {
	return "fs.meta.load"
}

func (c *commandFsMetaLoad) Help() string {
	return `load saved filer meta data to restore the directory and file structure

	fs.meta.load <filer_host>-<port>-<time>.meta
	fs.meta.load -v=false <filer_host>-<port>-<time>.meta // skip printing out the verbose output

`
}

func (c *commandFsMetaLoad) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) == 0 {
		fmt.Fprintf(writer, "missing a metadata file\n")
		return nil
	}

	fileName := args[len(args)-1]

	metaLoadCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := metaLoadCommand.Bool("v", true, "verbose mode")
	if err = metaLoadCommand.Parse(args[0 : len(args)-1]); err != nil {
		return nil
	}

	dst, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil
	}
	defer dst.Close()

	var dirCount, fileCount uint64
	lastLogTime := time.Now()

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		sizeBuf := make([]byte, 4)

		for {
			if n, err := dst.Read(sizeBuf); n != 4 {
				if err == io.EOF {
					return nil
				}
				return err
			}

			size := util.BytesToUint32(sizeBuf)

			data := make([]byte, int(size))

			if n, err := dst.Read(data); n != len(data) {
				return err
			}

			fullEntry := &filer_pb.FullEntry{}
			if err = proto.Unmarshal(data, fullEntry); err != nil {
				return err
			}

			fullEntry.Entry.Name = strings.ReplaceAll(fullEntry.Entry.Name, "/", "x")
			if err := filer_pb.CreateEntry(client, &filer_pb.CreateEntryRequest{
				Directory: fullEntry.Dir,
				Entry:     fullEntry.Entry,
			}); err != nil {
				return err
			}

			if *verbose || lastLogTime.Add(time.Second).Before(time.Now()) {
				if !*verbose {
					lastLogTime = time.Now()
				}
				fmt.Fprintf(writer, "load %s\n", util.FullPath(fullEntry.Dir).Child(fullEntry.Entry.Name))
			}

			if fullEntry.Entry.IsDirectory {
				dirCount++
			} else {
				fileCount++
			}

		}

	})

	if err == nil {
		fmt.Fprintf(writer, "\ntotal %d directories, %d files", dirCount, fileCount)
		fmt.Fprintf(writer, "\n%s is loaded.\n", fileName)
	}

	return err
}
