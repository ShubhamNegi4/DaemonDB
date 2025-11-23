package bplus

import (
	"fmt"
	"sync"
)

// BufferPool manages a cache of B+ tree nodes in memory
// It implements LRU eviction and handles loading nodes from disk via the pager
type BufferPool struct {
	mu       sync.Mutex
	pages    map[int64]*Node
	capacity int
	pager    Pager // Pager for loading nodes from disk
	// For LRU eviction: track access order
	accessOrder []int64 // Most recently used at the end
}

// NewBufferPool creates a new buffer pool with the given capacity
func NewBufferPool(capacity int) *BufferPool {
	return &BufferPool{
		pages:       make(map[int64]*Node, capacity),
		capacity:    capacity,
		accessOrder: make([]int64, 0, capacity),
	}
}

// SetPager sets the pager for this buffer pool (needed for loading nodes from disk)
func (bp *BufferPool) SetPager(pager Pager) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.pager = pager
}

// Get retrieves a node from the buffer pool, loading from disk if not in cache
func (bp *BufferPool) Get(pageID int64) (*Node, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check cache first
	if node, exists := bp.pages[pageID]; exists {
		// Update access order for LRU
		bp.updateAccessOrder(pageID)
		return node, nil
	}

	// Cache miss: load from disk
	if bp.pager == nil {
		return nil, fmt.Errorf("pager not set, cannot load page %d", pageID)
	}

	// Read page from disk
	pageData, err := bp.pager.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	// Decode node from page data
	node, err := decodeNode(pageData, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode node from page %d: %w", pageID, err)
	}

	// Add to cache (may evict if at capacity)
	bp.putNode(node)

	return node, nil
}

// Put adds a node to the buffer pool, evicting if necessary
func (bp *BufferPool) Put(node *Node) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	return bp.putNode(node)
}

// putNode is the internal method that assumes lock is held
func (bp *BufferPool) putNode(node *Node) error {
	// If node already in cache, just update access order
	if _, exists := bp.pages[node.id]; exists {
		bp.updateAccessOrder(node.id)
		return nil
	}

	// If at capacity, evict least recently used
	if len(bp.pages) >= bp.capacity {
		if err := bp.evictLRU(); err != nil {
			return fmt.Errorf("failed to evict during put: %w", err)
		}
	}

	// Add node to cache
	bp.pages[node.id] = node
	bp.updateAccessOrder(node.id)

	return nil
}

// Pin increments the pin count for a node, preventing eviction
func (bp *BufferPool) Pin(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	node, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("node %d not in buffer pool", pageID)
	}

	node.pincnt++
	return nil
}

// Unpin decrements the pin count for a node, allowing eviction when count reaches 0
func (bp *BufferPool) Unpin(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	node, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("node %d not in buffer pool", pageID)
	}

	if node.pincnt > 0 {
		node.pincnt--
	}

	return nil
}

// Flush writes all dirty nodes to disk
func (bp *BufferPool) Flush() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.pager == nil {
		return fmt.Errorf("pager not set, cannot flush")
	}

	for pageID, node := range bp.pages {
		if node.isDirty {
			pageData, err := encodeNode(node)
			if err != nil {
				return fmt.Errorf("failed to encode node %d: %w", pageID, err)
			}

			if err := bp.pager.WritePage(pageID, pageData); err != nil {
				return fmt.Errorf("failed to write page %d: %w", pageID, err)
			}

			node.isDirty = false
		}
	}

	return nil
}

// evictLRU evicts the least recently used unpinned node
func (bp *BufferPool) evictLRU() error {
	// Find first unpinned node in access order
	for i, pageID := range bp.accessOrder {
		node, exists := bp.pages[pageID]
		if !exists {
			// Remove from access order if node no longer exists
			bp.accessOrder = append(bp.accessOrder[:i], bp.accessOrder[i+1:]...)
			continue
		}

		// Skip pinned nodes
		if node.pincnt > 0 {
			continue
		}

		// Evict this node
		if node.isDirty && bp.pager != nil {
			// Write dirty node to disk before evicting
			pageData, err := encodeNode(node)
			if err != nil {
				return fmt.Errorf("failed to encode node %d for eviction: %w", pageID, err)
			}

			if err := bp.pager.WritePage(pageID, pageData); err != nil {
				return fmt.Errorf("failed to write page %d during eviction: %w", pageID, err)
			}
		}

		// Remove from cache and access order
		delete(bp.pages, pageID)
		bp.accessOrder = append(bp.accessOrder[:i], bp.accessOrder[i+1:]...)
		return nil
	}

	// All nodes are pinned, cannot evict
	return fmt.Errorf("all nodes are pinned, cannot evict")
}

// updateAccessOrder moves pageID to the end of the access order (most recently used)
func (bp *BufferPool) updateAccessOrder(pageID int64) {
	// Remove from current position
	for i, id := range bp.accessOrder {
		if id == pageID {
			bp.accessOrder = append(bp.accessOrder[:i], bp.accessOrder[i+1:]...)
			break
		}
	}
	// Add to end (most recently used)
	bp.accessOrder = append(bp.accessOrder, pageID)
}

// MarkDirty marks a node as dirty (modified)
func (bp *BufferPool) MarkDirty(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	node, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("node %d not in buffer pool", pageID)
	}

	node.isDirty = true
	return nil
}

// Size returns the current number of pages in the buffer pool
func (bp *BufferPool) Size() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.pages)
}

// Capacity returns the maximum capacity of the buffer pool
func (bp *BufferPool) Capacity() int {
	return bp.capacity
}
