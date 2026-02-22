package bplus

import (
	"DaemonDB/types"
	"fmt"
)

// NewNode creates a new node of given type and returns its pointer

// newNode creates a new page in the buffer pool and returns an empty Node.
// The returned node is pinned — caller must releaseNode when done.
func (t *BPlusTree) newNode(nodeType NodeType) (*Node, error) {
	pg, err := t.bufferPool.NewPage(t.fileID, types.PageTypeBPlusNode)
	if err != nil {
		return nil, fmt.Errorf("newNode: failed to allocate page: %w", err)
	}

	n := &Node{
		pageID:   pg.ID,
		nodeType: nodeType,
		keys:     make([][]byte, 0),
		children: make([]int64, 0),
		values:   make([][]byte, 0),
		next:     -1,
		parent:   -1,
		isDirty:  true,
		pincnt:   1,
	}

	// Serialize initial state immediately so the page is never garbage on eviction.
	if err := SerializeNode(n, pg.Data); err != nil {
		_ = t.bufferPool.UnpinPage(pg.ID, false)
		return nil, fmt.Errorf("newNode: initial serialize failed: %w", err)
	}
	pg.IsDirty = true

	return n, nil
}

// writeNode serializes a dirty node back into its buffer pool frame.
// It does NOT unpin — caller must releaseNode when done.
func (t *BPlusTree) writeNode(n *Node) error {
	pg, err := t.bufferPool.FetchPage(n.pageID)
	if err != nil {
		return fmt.Errorf("writeNode: failed to fetch page %d: %w", n.pageID, err)
	}
	// FetchPage adds an extra pin — release it after serialize.
	// The original pin from fetchNode/newNode remains.
	defer func() {
		_ = t.bufferPool.UnpinPage(n.pageID, false)
	}()

	if err := SerializeNode(n, pg.Data); err != nil {
		return fmt.Errorf("writeNode: serialize failed for page %d: %w", n.pageID, err)
	}

	// since a Node is the in-memory representation of a page
	// marks the page for as dirty in bufferpool for it to be synced later
	if err := t.bufferPool.MarkDirty(n.pageID); err != nil {
		return fmt.Errorf("writeNode: failed to mark page %d dirty: %w", n.pageID, err)
	}

	// since the node works of marking the dedicated page as dirty is finished, the in memory node itself now is not dirty
	n.isDirty = false
	return nil
}

// fetchNode loads a node from the buffer pool (or disk via the pool).
// The returned node is pinned — caller must releaseNode when done.
func (t *BPlusTree) fetchNode(pageID int64) (*Node, error) {
	if pageID < 0 {
		return nil, fmt.Errorf("fetchNode: invalid pageID %d", pageID)
	}

	pg, err := t.bufferPool.FetchPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("fetchNode: failed to fetch page %d: %w", pageID, err)
	}

	n, err := DeserializeNode(pg.Data, t.fileID) // ← pass fileID
	if err != nil {
		_ = t.bufferPool.UnpinPage(pageID, false)
		return nil, fmt.Errorf("fetchNode: deserialize failed for page %d: %w", pageID, err)
	}

	n.pageID = pageID // ← always override with actual global ID
	n.pincnt = 1
	return n, nil
}
