package filer

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const (
	MsgFailDelNonEmptyFolder = "fail to delete non-empty folder"
)

type OnChunksFunc func([]*filer_pb.FileChunk) error
type OnHardLinkIdsFunc func([]HardLinkId) error

func (f *Filer) DeleteEntryMetaAndData(ctx context.Context, p util.FullPath, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32) (err error) {
	if p == "/" {
		return nil
	}

	entry, findErr := f.FindEntry(ctx, p)
	if findErr != nil {
		return findErr
	}

	isDeleteCollection := f.isBucket(entry)

	if entry.IsDirectory() {
		// delete the folder children, not including the folder itself
		err = f.doBatchDeleteFolderMetaAndData(ctx, entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks && !isDeleteCollection, isDeleteCollection, isFromOtherCluster, signatures, func(chunks []*filer_pb.FileChunk) error {
			if shouldDeleteChunks && !isDeleteCollection {
				f.DirectDeleteChunks(chunks)
			}
			return nil
		}, func(hardLinkIds []HardLinkId) error {
			// A case not handled:
			// what if the chunk is in a different collection?
			if shouldDeleteChunks {
				f.maybeDeleteHardLinks(hardLinkIds)
			}
			return nil
		})
		if err != nil {
			glog.V(0).Infof("delete directory %s: %v", p, err)
			return fmt.Errorf("delete directory %s: %v", p, err)
		}
	}

	if shouldDeleteChunks && !isDeleteCollection {
		f.DirectDeleteChunks(entry.Chunks)
	}

	// delete the file or folder
	err = f.doDeleteEntryMetaAndData(ctx, entry, shouldDeleteChunks, isFromOtherCluster, signatures)
	if err != nil {
		return fmt.Errorf("delete file %s: %v", p, err)
	}

	if isDeleteCollection {
		collectionName := entry.Name()
		f.doDeleteCollection(collectionName)
		f.deleteBucket(collectionName)
	}

	return nil
}

func (f *Filer) doBatchDeleteFolderMetaAndData(ctx context.Context, entry *Entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isDeletingBucket, isFromOtherCluster bool, signatures []int32, onChunksFn OnChunksFunc, onHardLinkIdsFn OnHardLinkIdsFunc) (err error) {

	lastFileName := ""
	includeLastFile := false
	if !isDeletingBucket || !f.Store.CanDropWholeBucket() {
		for {
			entries, _, err := f.ListDirectoryEntries(ctx, entry.FullPath, lastFileName, includeLastFile, PaginationSize, "", "", "")
			if err != nil {
				glog.Errorf("list folder %s: %v", entry.FullPath, err)
				return fmt.Errorf("list folder %s: %v", entry.FullPath, err)
			}
			if lastFileName == "" && !isRecursive && len(entries) > 0 {
				// only for first iteration in the loop
				glog.V(0).Infof("deleting a folder %s has children: %+v ...", entry.FullPath, entries[0].Name())
				return fmt.Errorf("%s: %s", MsgFailDelNonEmptyFolder, entry.FullPath)
			}

			for _, sub := range entries {
				lastFileName = sub.Name()
				if sub.IsDirectory() {
					subIsDeletingBucket := f.isBucket(sub)
					err = f.doBatchDeleteFolderMetaAndData(ctx, sub, isRecursive, ignoreRecursiveError, shouldDeleteChunks, subIsDeletingBucket, false, nil, onChunksFn, onHardLinkIdsFn)
				} else {
					f.NotifyUpdateEvent(ctx, sub, nil, shouldDeleteChunks, isFromOtherCluster, nil)
					if len(sub.HardLinkId) != 0 {
						// hard link chunk data are deleted separately
						err = onHardLinkIdsFn([]HardLinkId{sub.HardLinkId})
					} else {
						err = onChunksFn(sub.Chunks)
					}
				}
				if err != nil && !ignoreRecursiveError {
					return err
				}
			}

			if len(entries) < PaginationSize {
				break
			}
		}
	}

	glog.V(3).Infof("deleting directory %v delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	if storeDeletionErr := f.Store.DeleteFolderChildren(ctx, entry.FullPath); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %v", storeDeletionErr)
	}

	f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)

	return nil
}

func (f *Filer) doDeleteEntryMetaAndData(ctx context.Context, entry *Entry, shouldDeleteChunks bool, isFromOtherCluster bool, signatures []int32) (err error) {

	glog.V(3).Infof("deleting entry %v, delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	if !entry.IsDirectory() && !shouldDeleteChunks {
		if storeDeletionErr := f.Store.DeleteOneEntrySkipHardlink(ctx, entry.FullPath); storeDeletionErr != nil {
			return fmt.Errorf("filer store delete skip hardlink: %v", storeDeletionErr)
		}
	} else if storeDeletionErr := f.Store.DeleteOneEntry(ctx, entry); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %v", storeDeletionErr)
	}
	if !entry.IsDirectory() {
		f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	}

	return nil
}

func (f *Filer) doDeleteCollection(collectionName string) (err error) {

	return f.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(context.Background(), &master_pb.CollectionDeleteRequest{
			Name: collectionName,
		})
		if err != nil {
			glog.Infof("delete collection %s: %v", collectionName, err)
		}
		return err
	})

}

func (f *Filer) maybeDeleteHardLinks(hardLinkIds []HardLinkId) {
	for _, hardLinkId := range hardLinkIds {
		if err := f.Store.DeleteHardLink(context.Background(), hardLinkId); err != nil {
			glog.Errorf("delete hard link id %d : %v", hardLinkId, err)
		}
	}
}
