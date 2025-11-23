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
	"sync"
)

type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

const (
	PageSize = 4096 // in bytes (4KB)
	MaxKeys  = 32
	MinKeys  = MaxKeys / 2

	MaxKeyLen = 256  // in bytes
	MaxValLen = 4096 // in bytes
)

type Node struct {
	id       int64
	nodeType NodeType
	key      [][]byte // keys in the node (sorted keys)
	children []int64  // only for internal node
	vals     [][]byte // leaf nodes
	next     int64    // only for leaf node
	parent   int64

	numKeys int16
	isDirty bool         // to check if the node is modified
	pincnt  int16        // buffer pool pin count
	mu      sync.RWMutex // to handle concurrent access
}

type BPlusTree struct {
	root  int64 // root node id
	pager Pager
	cache *BufferPool
	cmp   func(a, b []byte) int // comparison function for keys
	mu    sync.RWMutex
}

// Pager is the persistence abstraction. Implement an in-memory pager first
// then a file-backed pager that serializes nodes to disk/pages.
type Pager interface {
	ReadPage(pageID int64) ([]byte, error)
	WritePage(pageID int64, data []byte) error
	AllocatePage() (int64, error)
	DeallocatePage(pageID int64) error
	Sync() error
	Close() error
}

// BufferPool structure and methods are implemented in buffer_pool.go
