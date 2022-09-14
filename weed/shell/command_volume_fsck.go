package shell

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"github.com/seaweedfs/seaweedfs/weed/rpc/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/rpc/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandVolumeFsck{})
}

type commandVolumeFsck struct {
	env          *CommandEnv
	forcePurging *bool
}

func (c *commandVolumeFsck) Name() string {
	return "volume.fsck"
}

func (c *commandVolumeFsck) Help() string {
	return `check all volumes to find entries not used by the filer

	Important assumption!!!
		the system is all used by one filer.

	This command works this way:
	1. collect all file ids from all volumes, as set A
	2. collect all file ids from the filer, as set B
	3. find out the set A subtract B

	If -findMissingChunksInFiler is enabled, this works
	in a reverse way:
	1. collect all file ids from all volumes, as set A
	2. collect all file ids from the filer, as set B
	3. find out the set B subtract A

`
}

func (c *commandVolumeFsck) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsckCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := fsckCommand.Bool("v", false, "verbose mode")
	findMissingChunksInFiler := fsckCommand.Bool("findMissingChunksInFiler", false, "see \"help volume.fsck\"")
	findMissingChunksInFilerPath := fsckCommand.String("findMissingChunksInFilerPath", "/", "used together with findMissingChunksInFiler")
	findMissingChunksInVolumeId := fsckCommand.Int("findMissingChunksInVolumeId", 0, "used together with findMissingChunksInFiler")
	applyPurging := fsckCommand.Bool("reallyDeleteFromVolume", false, "<expert only!> after detection, delete missing data from volumes / delete missing file entries from filer. Currently this only works with default filerGroup.")
	c.forcePurging = fsckCommand.Bool("forcePurging", false, "delete missing data from volumes in one replica used together with applyPurging")
	purgeAbsent := fsckCommand.Bool("reallyDeleteFilerEntries", false, "<expert only!> delete missing file entries from filer if the corresponding volume is missing for any reason, please ensure all still existing/expected volumes are connected! used together with findMissingChunksInFiler")
	tempPath := fsckCommand.String("tempPath", path.Join(os.TempDir()), "path for temporary idx files")
	cutoffTimeAgo := fsckCommand.Duration("cutoffTimeAgo", 5*time.Minute, "only include entries  on volume servers before this cutoff time to check orphan chunks")

	if err = fsckCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	c.env = commandEnv

	// create a temp folder
	tempFolder, err := os.MkdirTemp(*tempPath, "sw_fsck")
	if err != nil {
		return fmt.Errorf("failed to create temp folder: %v", err)
	}
	if *verbose {
		fmt.Fprintf(writer, "working directory: %s\n", tempFolder)
	}
	defer os.RemoveAll(tempFolder)

	// collect all volume id locations
	dataNodeVolumeIdToVInfo, err := c.collectVolumeIds(commandEnv, *verbose, writer)
	if err != nil {
		return fmt.Errorf("failed to collect all volume locations: %v", err)
	}

	isBucketsPath := false
	var fillerBucketsPath string
	if *findMissingChunksInFiler && *findMissingChunksInFilerPath != "/" {
		fillerBucketsPath, err = readFilerBucketsPath(commandEnv)
		if err != nil {
			return fmt.Errorf("read filer buckets path: %v", err)
		}
		if strings.HasPrefix(*findMissingChunksInFilerPath, fillerBucketsPath) {
			isBucketsPath = true
		}
	}
	if err != nil {
		return fmt.Errorf("read filer buckets path: %v", err)
	}

	collectMtime := time.Now().Unix()
	// collect each volume file ids
	for dataNodeId, volumeIdToVInfo := range dataNodeVolumeIdToVInfo {
		for volumeId, vinfo := range volumeIdToVInfo {
			if *findMissingChunksInVolumeId > 0 && uint32(*findMissingChunksInVolumeId) != volumeId {
				delete(volumeIdToVInfo, volumeId)
				continue
			}
			if isBucketsPath && !strings.HasPrefix(*findMissingChunksInFilerPath, fillerBucketsPath+"/"+vinfo.collection) {
				delete(volumeIdToVInfo, volumeId)
				continue
			}
			cutoffFrom := time.Now().Add(-*cutoffTimeAgo).UnixNano()
			err = c.collectOneVolumeFileIds(tempFolder, dataNodeId, volumeId, vinfo, *verbose, writer, uint64(cutoffFrom))
			if err != nil {
				return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, err)
			}
		}
	}

	if *findMissingChunksInFiler {
		// collect all filer file ids and paths
		if err = c.collectFilerFileIdAndPaths(dataNodeVolumeIdToVInfo, tempFolder, writer, *findMissingChunksInFilerPath, *verbose, *purgeAbsent, collectMtime); err != nil {
			return fmt.Errorf("collectFilerFileIdAndPaths: %v", err)
		}
		for dataNodeId, volumeIdToVInfo := range dataNodeVolumeIdToVInfo {
			// for each volume, check filer file ids
			if err = c.findFilerChunksMissingInVolumeServers(volumeIdToVInfo, tempFolder, dataNodeId, writer, *verbose, *applyPurging); err != nil {
				return fmt.Errorf("findFilerChunksMissingInVolumeServers: %v", err)
			}
		}
	} else {
		// collect all filer file ids
		if err = c.collectFilerFileIds(dataNodeVolumeIdToVInfo, tempFolder, writer, *verbose); err != nil {
			return fmt.Errorf("failed to collect file ids from filer: %v", err)
		}
		// volume file ids subtract filer file ids
		if err = c.findExtraChunksInVolumeServers(dataNodeVolumeIdToVInfo, tempFolder, writer, *verbose, *applyPurging); err != nil {
			return fmt.Errorf("findExtraChunksInVolumeServers: %v", err)
		}
	}

	return nil
}

func (c *commandVolumeFsck) collectFilerFileIdAndPaths(dataNodeVolumeIdToVInfo map[string]map[uint32]VInfo, tempFolder string, writer io.Writer, filerPath string, verbose bool, purgeAbsent bool, collectMtime int64) error {

	if verbose {
		fmt.Fprintf(writer, "checking each file from filer ...\n")
	}

	files := make(map[uint32]*os.File)
	for _, volumeIdToServer := range dataNodeVolumeIdToVInfo {
		for vid := range volumeIdToServer {
			if _, ok := files[vid]; ok {
				continue
			}
			dst, openErr := os.OpenFile(getFilerFileIdFile(tempFolder, vid), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if openErr != nil {
				return fmt.Errorf("failed to create file %s: %v", getFilerFileIdFile(tempFolder, vid), openErr)
			}
			files[vid] = dst
		}
	}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()

	type Item struct {
		vid     uint32
		fileKey uint64
		cookie  uint32
		path    util.FullPath
	}
	return doTraverseBfsAndSaving(c.env, nil, filerPath, false, func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
		if verbose && entry.Entry.IsDirectory {
			fmt.Fprintf(writer, "checking directory %s\n", util.NewFullPath(entry.Dir, entry.Entry.Name))
		}
		dataChunks, manifestChunks, resolveErr := filer.ResolveChunkManifest(filer.LookupFn(c.env), entry.Entry.Chunks, 0, math.MaxInt64)
		if resolveErr != nil {
			return nil
		}
		dataChunks = append(dataChunks, manifestChunks...)
		for _, chunk := range dataChunks {
			if chunk.Mtime > collectMtime {
				continue
			}
			outputChan <- &Item{
				vid:     chunk.Fid.VolumeId,
				fileKey: chunk.Fid.FileKey,
				cookie:  chunk.Fid.Cookie,
				path:    util.NewFullPath(entry.Dir, entry.Entry.Name),
			}
		}
		return nil
	}, func(outputChan chan interface{}) {
		buffer := make([]byte, 16)
		for item := range outputChan {
			i := item.(*Item)
			if f, ok := files[i.vid]; ok {
				util.Uint64toBytes(buffer, i.fileKey)
				util.Uint32toBytes(buffer[8:], i.cookie)
				util.Uint32toBytes(buffer[12:], uint32(len(i.path)))
				f.Write(buffer)
				f.Write([]byte(i.path))
				// fmt.Fprintf(writer, "%d,%x%08x %d %s\n", i.vid, i.fileKey, i.cookie, len(i.path), i.path)
			} else {
				fmt.Fprintf(writer, "%d,%x%08x %s volume not found\n", i.vid, i.fileKey, i.cookie, i.path)
				if purgeAbsent {
					fmt.Printf("deleting path %s after volume not found", i.path)
					c.httpDelete(i.path, verbose)
				}
			}
		}
	})

}

func (c *commandVolumeFsck) findFilerChunksMissingInVolumeServers(volumeIdToVInfo map[uint32]VInfo, tempFolder string, dataNodeId string, writer io.Writer, verbose bool, applyPurging bool) error {

	for volumeId, vinfo := range volumeIdToVInfo {
		checkErr := c.oneVolumeFileIdsCheckOneVolume(tempFolder, dataNodeId, volumeId, writer, verbose, applyPurging)
		if checkErr != nil {
			return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, checkErr)
		}
	}
	return nil
}

func (c *commandVolumeFsck) findExtraChunksInVolumeServers(dataNodeVolumeIdToVInfo map[string]map[uint32]VInfo, tempFolder string, writer io.Writer, verbose bool, applyPurging bool) error {

	var totalInUseCount, totalOrphanChunkCount, totalOrphanDataSize uint64
	volumeIdOrphanFileIds := make(map[uint32]map[string]bool)
	isSeveralReplicas := make(map[uint32]bool)
	isEcVolumeReplicas := make(map[uint32]bool)
	isReadOnlyReplicas := make(map[uint32]bool)
	serverReplicas := make(map[uint32][]rpc.ServerAddress)
	for dataNodeId, volumeIdToVInfo := range dataNodeVolumeIdToVInfo {
		for volumeId, vinfo := range volumeIdToVInfo {
			inUseCount, orphanFileIds, orphanDataSize, checkErr := c.oneVolumeFileIdsSubtractFilerFileIds(tempFolder, dataNodeId, volumeId, writer, verbose)
			if checkErr != nil {
				return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, checkErr)
			}
			isSeveralReplicas[volumeId] = false
			if _, found := volumeIdOrphanFileIds[volumeId]; !found {
				volumeIdOrphanFileIds[volumeId] = make(map[string]bool)
			} else {
				isSeveralReplicas[volumeId] = true
			}
			for _, fid := range orphanFileIds {
				if isSeveralReplicas[volumeId] {
					if _, found := volumeIdOrphanFileIds[volumeId][fid]; !found {
						continue
					}
				}
				volumeIdOrphanFileIds[volumeId][fid] = isSeveralReplicas[volumeId]
			}

			totalInUseCount += inUseCount
			totalOrphanChunkCount += uint64(len(orphanFileIds))
			totalOrphanDataSize += orphanDataSize

			if verbose {
				for _, fid := range orphanFileIds {
					fmt.Fprintf(writer, "%s\n", fid)
				}
			}
			isEcVolumeReplicas[volumeId] = vinfo.isEcVolume
			if isReadOnly, found := isReadOnlyReplicas[volumeId]; !(found && isReadOnly) {
				isReadOnlyReplicas[volumeId] = vinfo.isReadOnly
			}
			serverReplicas[volumeId] = append(serverReplicas[volumeId], vinfo.server)
		}

		for volumeId, orphanReplicaFileIds := range volumeIdOrphanFileIds {
			if !(applyPurging && len(orphanReplicaFileIds) > 0) {
				continue
			}
			orphanFileIds := []string{}
			for fid, foundInAllReplicas := range orphanReplicaFileIds {
				if !isSeveralReplicas[volumeId] || *c.forcePurging || (isSeveralReplicas[volumeId] && foundInAllReplicas) {
					orphanFileIds = append(orphanFileIds, fid)
				}
			}
			if !(len(orphanFileIds) > 0) {
				continue
			}
			if verbose {
				fmt.Fprintf(writer, "purging process for volume %d.\n", volumeId)
			}

			if isEcVolumeReplicas[volumeId] {
				fmt.Fprintf(writer, "skip purging for Erasure Coded volume %d.\n", volumeId)
				continue
			}
			for _, server := range serverReplicas[volumeId] {
				needleVID := needle.VolumeId(volumeId)

				if isReadOnlyReplicas[volumeId] {
					err := markVolumeWritable(c.env.option.GrpcDialOption, needleVID, server, true)
					if err != nil {
						return fmt.Errorf("mark volume %d read/write: %v", volumeId, err)
					}

					fmt.Fprintf(writer, "temporarily marked %d on server %v writable for forced purge\n", volumeId, server)
					defer markVolumeWritable(c.env.option.GrpcDialOption, needleVID, server, false)

					fmt.Fprintf(writer, "marked %d on server %v writable for forced purge\n", volumeId, server)
				}
				if verbose {
					fmt.Fprintf(writer, "purging files from volume %d\n", volumeId)
				}

				if err := c.purgeFileIdsForOneVolume(volumeId, orphanFileIds, writer); err != nil {
					return fmt.Errorf("purging volume %d: %v", volumeId, err)
				}
			}
		}
	}

	if !applyPurging {
		pct := float64(totalOrphanChunkCount*100) / (float64(totalOrphanChunkCount + totalInUseCount))
		fmt.Fprintf(writer, "\nTotal\t\tentries:%d\torphan:%d\t%.2f%%\t%dB\n",
			totalOrphanChunkCount+totalInUseCount, totalOrphanChunkCount, pct, totalOrphanDataSize)

		fmt.Fprintf(writer, "This could be normal if multiple filers or no filers are used.\n")
	}

	if totalOrphanChunkCount == 0 {
		fmt.Fprintf(writer, "no orphan data\n")
		//return nil
	}

	return nil
}

func (c *commandVolumeFsck) collectOneVolumeFileIds(tempFolder string, dataNodeId string, volumeId uint32, vinfo VInfo, verbose bool, writer io.Writer, cutoffFrom uint64) error {

	if verbose {
		fmt.Fprintf(writer, "collecting volume %d file ids from %s ...\n", volumeId, vinfo.server)
	}

	return operation.WithVolumeServerClient(false, vinfo.server, c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		ext := ".idx"
		if vinfo.isEcVolume {
			ext = ".ecx"
		}

		copyFileClient, err := volumeServerClient.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
			VolumeId:                 volumeId,
			Ext:                      ext,
			CompactionRevision:       math.MaxUint32,
			StopOffset:               math.MaxInt64,
			Collection:               vinfo.collection,
			IsEcVolume:               vinfo.isEcVolume,
			IgnoreSourceFileNotFound: false,
		})
		if err != nil {
			return fmt.Errorf("failed to start copying volume %d%s: %v", volumeId, ext, err)
		}

		var buf bytes.Buffer
		for {
			resp, err := copyFileClient.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			buf.Write(resp.FileContent)
		}
		if vinfo.isReadOnly == false {
			index, err := idx.FirstInvalidIndex(buf.Bytes(), func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error) {
				resp, err := volumeServerClient.ReadNeedleMeta(context.Background(), &volume_server_pb.ReadNeedleMetaRequest{
					VolumeId: volumeId,
					NeedleId: uint64(key),
					Offset:   offset.ToActualOffset(),
					Size:     int32(size),
				})
				if err != nil {
					return false, fmt.Errorf("to read needle meta with id %d  from volume %d with error %v", key, volumeId, err)
				}
				return resp.LastModified <= cutoffFrom, nil
			})
			if err != nil {
				fmt.Fprintf(writer, "Failed to search for last vilad index on volume %d with error %v", volumeId, err)
			}
			buf.Truncate(index * types.NeedleMapEntrySize)
		}
		idxFilename := getVolumeFileIdFile(tempFolder, dataNodeId, volumeId)
		err = writeToFile(buf.Bytes(), idxFilename)
		if err != nil {
			return fmt.Errorf("failed to copy %d%s from %s: %v", volumeId, ext, vinfo.server, err)
		}

		return nil
	})

}

func (c *commandVolumeFsck) collectFilerFileIds(dataNodeVolumeIdToVInfo map[string]map[uint32]VInfo, tempFolder string, writer io.Writer, verbose bool) error {

	if verbose {
		fmt.Fprintf(writer, "collecting file ids from filer ...\n")
	}

	files := make(map[uint32]*os.File)
	for _, volumeIdToServer := range dataNodeVolumeIdToVInfo {
		for vid := range volumeIdToServer {
			dst, openErr := os.OpenFile(getFilerFileIdFile(tempFolder, vid), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if openErr != nil {
				return fmt.Errorf("failed to create file %s: %v", getFilerFileIdFile(tempFolder, vid), openErr)
			}
			files[vid] = dst
		}
	}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()

	type Item struct {
		vid     uint32
		fileKey uint64
	}
	return doTraverseBfsAndSaving(c.env, nil, "/", false, func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
		dataChunks, manifestChunks, resolveErr := filer.ResolveChunkManifest(filer.LookupFn(c.env), entry.Entry.Chunks, 0, math.MaxInt64)
		if resolveErr != nil {
			if verbose {
				fmt.Fprintf(writer, "resolving manifest chunks in %s: %v\n", util.NewFullPath(entry.Dir, entry.Entry.Name), resolveErr)
			}
			return nil
		}
		dataChunks = append(dataChunks, manifestChunks...)
		for _, chunk := range dataChunks {
			outputChan <- &Item{
				vid:     chunk.Fid.VolumeId,
				fileKey: chunk.Fid.FileKey,
			}
		}
		return nil
	}, func(outputChan chan interface{}) {
		buffer := make([]byte, 8)
		for item := range outputChan {
			i := item.(*Item)
			util.Uint64toBytes(buffer, i.fileKey)
			files[i.vid].Write(buffer)
		}
	})
}

func (c *commandVolumeFsck) oneVolumeFileIdsCheckOneVolume(tempFolder string, dataNodeId string, volumeId uint32, writer io.Writer, verbose bool, applyPurging bool) (err error) {

	if verbose {
		fmt.Fprintf(writer, "find missing file chunks in dataNodeId %s volume %d ...\n", dataNodeId, volumeId)
	}

	db := needle_map.NewMemDb()
	defer db.Close()

	if err = db.LoadFromIdx(getVolumeFileIdFile(tempFolder, dataNodeId, volumeId)); err != nil {
		return
	}

	file := getFilerFileIdFile(tempFolder, volumeId)
	fp, err := os.Open(file)
	if err != nil {
		return
	}
	defer fp.Close()

	type Item struct {
		fileKey uint64
		cookie  uint32
		path    util.FullPath
	}

	br := bufio.NewReader(fp)
	buffer := make([]byte, 16)
	item := &Item{}
	var readSize int
	for {
		readSize, err = io.ReadFull(br, buffer)
		if err != nil || readSize != 16 {
			break
		}

		item.fileKey = util.BytesToUint64(buffer[:8])
		item.cookie = util.BytesToUint32(buffer[8:12])
		pathSize := util.BytesToUint32(buffer[12:16])
		pathBytes := make([]byte, int(pathSize))
		n, err := io.ReadFull(br, pathBytes)
		if err != nil {
			fmt.Fprintf(writer, "%d,%x%08x in unexpected error: %v\n", volumeId, item.fileKey, item.cookie, err)
		}
		if n != int(pathSize) {
			fmt.Fprintf(writer, "%d,%x%08x %d unexpected file name size %d\n", volumeId, item.fileKey, item.cookie, pathSize, n)
		}
		item.path = util.FullPath(string(pathBytes))

		needleId := types.NeedleId(item.fileKey)
		if _, found := db.Get(needleId); !found {
			fmt.Fprintf(writer, "%s\n", item.path)

			if applyPurging {
				// defining the URL this way automatically escapes complex path names
				c.httpDelete(item.path, verbose)
			}
		}
	}
	return nil
}

func (c *commandVolumeFsck) httpDelete(path util.FullPath, verbose bool) {
	req, err := http.NewRequest(http.MethodDelete, "", nil)

	req.URL = &url.URL{
		Scheme: "http",
		Host:   c.env.option.FilerAddress.ToHttpAddress(),
		Path:   string(path),
	}
	if verbose {
		fmt.Printf("full HTTP delete request to be sent: %v\n", req)
	}
	if err != nil {
		fmt.Errorf("HTTP delete request error: %v\n", err)
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Errorf("DELETE fetch error: %v\n", err)
	}
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Errorf("DELETE response error: %v\n", err)
	}

	if verbose {
		fmt.Println("delete response Status : ", resp.Status)
		fmt.Println("delete response Headers : ", resp.Header)
	}
}

func (c *commandVolumeFsck) oneVolumeFileIdsSubtractFilerFileIds(tempFolder string, dataNodeId string, volumeId uint32, writer io.Writer, verbose bool) (inUseCount uint64, orphanFileIds []string, orphanDataSize uint64, err error) {

	db := needle_map.NewMemDb()
	defer db.Close()

	if err = db.LoadFromIdx(getVolumeFileIdFile(tempFolder, dataNodeId, volumeId)); err != nil {
		return
	}

	filerFileIdsData, err := os.ReadFile(getFilerFileIdFile(tempFolder, volumeId))
	if err != nil {
		return
	}

	dataLen := len(filerFileIdsData)
	if dataLen%8 != 0 {
		return 0, nil, 0, fmt.Errorf("filer data is corrupted")
	}

	for i := 0; i < len(filerFileIdsData); i += 8 {
		fileKey := util.BytesToUint64(filerFileIdsData[i : i+8])
		db.Delete(types.NeedleId(fileKey))
		inUseCount++
	}

	var orphanFileCount uint64
	db.AscendingVisit(func(n needle_map.NeedleValue) error {
		// fmt.Printf("%d,%x\n", volumeId, n.Key)
		orphanFileIds = append(orphanFileIds, fmt.Sprintf("%d,%s00000000", volumeId, n.Key.String()))
		orphanFileCount++
		orphanDataSize += uint64(n.Size)
		return nil
	})

	if orphanFileCount > 0 {
		pct := float64(orphanFileCount*100) / (float64(orphanFileCount + inUseCount))
		fmt.Fprintf(writer, "dataNode:%s\tvolume:%d\tentries:%d\torphan:%d\t%.2f%%\t%dB\n",
			dataNodeId, volumeId, orphanFileCount+inUseCount, orphanFileCount, pct, orphanDataSize)
	}

	return

}

type VInfo struct {
	server     rpc.ServerAddress
	collection string
	isEcVolume bool
	isReadOnly bool
}

func (c *commandVolumeFsck) collectVolumeIds(commandEnv *CommandEnv, verbose bool, writer io.Writer) (volumeIdToServer map[string]map[uint32]VInfo, err error) {

	if verbose {
		fmt.Fprintf(writer, "collecting volume id and locations from master ...\n")
	}

	volumeIdToServer = make(map[string]map[uint32]VInfo)
	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	eachDataNode(topologyInfo, func(dc string, rack RackId, t *master_pb.DataNodeInfo) {
		for _, diskInfo := range t.DiskInfos {
			dataNodeId := t.GetId()
			volumeIdToServer[dataNodeId] = make(map[uint32]VInfo)
			for _, vi := range diskInfo.VolumeInfos {
				volumeIdToServer[dataNodeId][vi.Id] = VInfo{
					server:     rpc.NewServerAddressFromDataNode(t),
					collection: vi.Collection,
					isEcVolume: false,
					isReadOnly: vi.ReadOnly,
				}
			}
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				volumeIdToServer[dataNodeId][ecShardInfo.Id] = VInfo{
					server:     rpc.NewServerAddressFromDataNode(t),
					collection: ecShardInfo.Collection,
					isEcVolume: true,
					isReadOnly: true,
				}
			}
		}
	})

	if verbose {
		fmt.Fprintf(writer, "collected %d volumes and locations.\n", len(volumeIdToServer))
	}
	return
}

func (c *commandVolumeFsck) purgeFileIdsForOneVolume(volumeId uint32, fileIds []string, writer io.Writer) (err error) {
	fmt.Fprintf(writer, "purging orphan data for volume %d...\n", volumeId)
	locations, found := c.env.MasterClient.GetLocations(volumeId)
	if !found {
		return fmt.Errorf("failed to find volume %d locations", volumeId)
	}

	resultChan := make(chan []*volume_server_pb.DeleteResult, len(locations))
	var wg sync.WaitGroup
	for _, location := range locations {
		wg.Add(1)
		go func(server rpc.ServerAddress, fidList []string) {
			defer wg.Done()

			if deleteResults, deleteErr := operation.DeleteFilesAtOneVolumeServer(server, c.env.option.GrpcDialOption, fidList, false); deleteErr != nil {
				err = deleteErr
			} else if deleteResults != nil {
				resultChan <- deleteResults
			}

		}(location.ServerAddress(), fileIds)
	}
	wg.Wait()
	close(resultChan)

	for results := range resultChan {
		for _, result := range results {
			if result.Error != "" {
				fmt.Fprintf(writer, "purge error: %s\n", result.Error)
			}
		}
	}

	return
}

func getVolumeFileIdFile(tempFolder string, dataNodeid string, vid uint32) string {
	return filepath.Join(tempFolder, fmt.Sprintf("%s_%d.idx", dataNodeid, vid))
}

func getFilerFileIdFile(tempFolder string, vid uint32) string {
	return filepath.Join(tempFolder, fmt.Sprintf("%d.fid", vid))
}

func writeToFile(bytes []byte, fileName string) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	dst, err := os.OpenFile(fileName, flags, 0644)
	if err != nil {
		return nil
	}
	defer dst.Close()

	dst.Write(bytes)
	return nil
}
