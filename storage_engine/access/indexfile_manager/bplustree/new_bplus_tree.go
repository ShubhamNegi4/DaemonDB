package bplus

import (
	"DaemonDB/storage_engine/bufferpool"
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"DaemonDB/types"
	"bytes"
	"fmt"
	"os"
)

// NewBPlusTree creates a B+ tree that stores its pages in the file identified
// by fileID, using the shared BufferPool and DiskManager.
//
// The tree's root page ID is stored in the file's metadata page (page 0).
// If the file is new, the root is initialized to 0 (invalid page ID).
//
// cmp is the key comparator function (typically bytes.Compare).
func OpenBPlusTree(indexPath string, fileID uint32, bufferPool *bufferpool.BufferPool, diskManager *diskmanager.DiskManager) (*BPlusTree, error) {
	// Check if the file already exists before OpenFile creates it.
	_, statErr := os.Stat(indexPath)
	isNew := os.IsNotExist(statErr)

	// OpenFile opens or creates the file and registers it with the disk manager.
	// Use OpenFileWithID not OpenFile
	_, err := diskManager.OpenFileWithID(indexPath, fileID)
	if err != nil {
		return nil, fmt.Errorf("OpenBPlusTree: failed to open index file %s: %w", indexPath, err)
	}

	t := &BPlusTree{
		fileID:      fileID,
		root:        -1,
		bufferPool:  bufferPool,
		diskManager: diskManager,
		cmp:         bytes.Compare,
	}

	if isNew {
		// Reserve page 0 for metadata by allocating it first
		_, err := diskManager.AllocatePage(fileID, types.PageTypeMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to reserve metadata page: %w", err)
		}

		// Now allocate root leaf — gets local page 1
		root, err := t.newNode(NodeLeaf)
		if err != nil {
			return nil, err
		}

		defer t.releaseNode(root, true)

		if err := t.writeNode(root); err != nil {
			return nil, err
		}

		t.root = root.pageID
		// Save LOCAL page ID to disk.
		localRootID, err := diskManager.GetLocalPageID(fileID, root.pageID)
		if err != nil {
			t.releaseNode(root, false)
			return nil, fmt.Errorf("failed to get local root ID: %w", err)
		}
		t.releaseNode(root, true)

		fmt.Printf("[BTree] new tree fileID=%d globalRoot=%d localRoot=%d\n", fileID, t.root, localRootID)

		if err := diskManager.WriteRootID(fileID, localRootID); err != nil {
			return nil, err
		}
	} else {
		// Register all existing pages
		fd, err := diskManager.GetFileDescriptor(fileID)
		if err != nil {
			return nil, err
		}
		for localPage := int64(0); localPage < fd.NextPageID; localPage++ {
			if err := diskManager.RegisterPage(fileID, localPage); err != nil {
				return nil, err
			}
		}

		// Read LOCAL page ID from disk.
		localRootID, err := diskManager.ReadRootID(fileID)
		if err != nil {
			return nil, err
		}

		// Convert LOCAL → GLOBAL.
		globalRootID, err := diskManager.GetGlobalPageID(fileID, localRootID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve root page: %w", err)
		}
		t.root = globalRootID
		fmt.Printf("[BTree] loaded tree fileID=%d localRoot=%d globalRoot=%d\n", fileID, localRootID, globalRootID)
	}

	return t, nil
}

// saveRoot persists the current root page ID to the file's metadata page.
// Called after every operation that changes the root (split, new root allocation).
func (t *BPlusTree) saveRoot() error {
	localRootID := t.root & 0xFFFFFFFF // extract local from global
	if err := t.diskManager.WriteRootID(t.fileID, localRootID); err != nil {
		return fmt.Errorf("saveRoot: failed to persist root ID: %w", err)
	}
	return nil
}

// Close flushes all dirty pages in the BufferPool that belong to this tree's
// file and closes the file handle in DiskManager.
//
// Call this when switching databases or on shutdown to avoid leaking file
// descriptors and to ensure all changes are persisted.
func (t *BPlusTree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.bufferPool.FlushAllPages(); err != nil {
		return fmt.Errorf("Close: failed to flush pages: %w", err)
	}

	if err := t.diskManager.Sync(); err != nil {
		return fmt.Errorf("Close: failed to sync disk: %w", err)
	}

	return nil
}
