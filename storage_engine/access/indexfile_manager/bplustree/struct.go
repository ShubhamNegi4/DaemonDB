// Structure of B+ Tree
/*
Tree
 ├── Internal Node (keys + child pointers)
 │      └── Child Internal Nodes ...
 │             └── Leaf Nodes (keys + values + next pointer)


- keys: sorted ascending order
- internal nodes: children length == len(keys)+1
- leaf nodes: values length == len(keys)
- leaf nodes linked with `next` for fast range scans
- all leaf nodes at same depth

*/
package bplus

import (
	"DaemonDB/storage_engine/bufferpool"
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"sync"
)

type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

const (
	MaxKeys = 32
	MinKeys = MaxKeys / 2

	MaxKeyLen = 256  // in bytes
	MaxValLen = 4096 // in bytes
)

type Node struct {
	pageID   int64
	nodeType NodeType
	keys     [][]byte // keys in the node (sorted keys)
	children []int64  // only for internal node
	values   [][]byte // leaf nodes
	next     int64    // only for leaf node
	parent   int64

	isDirty bool         // to check if the node is modified
	pincnt  int16        // buffer pool pin count
	mu      sync.RWMutex // to handle concurrent access
}

type BPlusTree struct {
	fileID      uint32                   // DiskManager file ID for this index
	root        int64                    // global page ID of the root node
	bufferPool  *bufferpool.BufferPool   // shared buffer pool
	diskManager *diskmanager.DiskManager // shared disk manager
	cmp         func(a, b []byte) int    // key comparator (typically bytes.Compare)
	mu          sync.RWMutex             // protects tree structure during splits/merges
}

// BufferPool structure and methods are implemented in buffer_pool.go

func (tree *BPlusTree) Reset() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	// create a new empty leaf node
	root, err := tree.newNode(NodeLeaf)
	if err != nil {
		return err
	}
	defer tree.releaseNode(root, true)

	root.keys = root.keys[:0]
	root.values = root.values[:0]
	root.children = nil
	root.next = 0
	root.parent = 0
	root.isDirty = true

	if err := tree.writeNode(root); err != nil {
		return err
	}

	tree.root = root.pageID

	return nil
}
