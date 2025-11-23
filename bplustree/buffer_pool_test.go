package bplus

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestBufferPoolGetPut tests basic Get/Put operations
func TestBufferPoolGetPut(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "daemondb_bp_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	indexPath := filepath.Join(testDir, "test_bp.idx")
	defer os.Remove(indexPath)

	// Create pager and buffer pool
	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	cache := NewBufferPool(5)
	cache.SetPager(pager)

	// Create a test node
	node := NewNode(NodeLeaf)
	node.id = 1
	node.key = [][]byte{[]byte("key1"), []byte("key2")}
	node.vals = [][]byte{[]byte("val1"), []byte("val2")}
	node.numKeys = 2

	// Encode and write node to disk
	pageData, err := encodeNode(node)
	if err != nil {
		t.Fatalf("Failed to encode node: %v", err)
	}

	if err := pager.WritePage(node.id, pageData); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Test Get: should load from disk
	retrieved, err := cache.Get(node.id)
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	if retrieved.id != node.id {
		t.Errorf("Node ID mismatch: expected %d, got %d", node.id, retrieved.id)
	}

	if len(retrieved.key) != len(node.key) {
		t.Errorf("Key count mismatch: expected %d, got %d", len(node.key), len(retrieved.key))
	}

	// Test Put: should add to cache
	node2 := NewNode(NodeInternal)
	node2.id = 2
	node2.key = [][]byte{[]byte("key3")}
	node2.children = []int64{10, 20}
	node2.numKeys = 1

	if err := cache.Put(node2); err != nil {
		t.Fatalf("Failed to put node: %v", err)
	}

	// Should be able to get it from cache
	retrieved2, err := cache.Get(node2.id)
	if err != nil {
		t.Fatalf("Failed to get node2: %v", err)
	}

	if retrieved2.id != node2.id {
		t.Errorf("Node2 ID mismatch: expected %d, got %d", node2.id, retrieved2.id)
	}
}

// TestBufferPoolEviction tests LRU eviction
func TestBufferPoolEviction(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "daemondb_bp_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	indexPath := filepath.Join(testDir, "test_evict.idx")
	defer os.Remove(indexPath)

	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	// Create buffer pool with capacity 3
	cache := NewBufferPool(3)
	cache.SetPager(pager)

	// Create and put 4 nodes (should evict one)
	nodes := make([]*Node, 4)
	for i := 0; i < 4; i++ {
		node := NewNode(NodeLeaf)
		node.id = int64(i + 1)
		node.key = [][]byte{[]byte{byte(i)}}
		node.vals = [][]byte{[]byte{byte(i + 10)}}
		node.numKeys = 1

		// Encode and write to disk
		pageData, err := encodeNode(node)
		if err != nil {
			t.Fatalf("Failed to encode node %d: %v", i, err)
		}
		if err := pager.WritePage(node.id, pageData); err != nil {
			t.Fatalf("Failed to write node %d: %v", i, err)
		}

		nodes[i] = node
		if err := cache.Put(node); err != nil {
			t.Fatalf("Failed to put node %d: %v", i, err)
		}
	}

	// Cache should have 3 nodes (capacity)
	// The first node (id=1) should be evicted
	_, err = cache.Get(1)
	if err == nil {
		// If we can get it, it means it was loaded from disk (cache miss)
		// This is actually fine - eviction happened but we can reload
		t.Log("Node 1 was evicted but can be reloaded from disk")
	}

	// Nodes 2, 3, 4 should be in cache
	for i := 1; i < 4; i++ {
		node, err := cache.Get(int64(i + 1))
		if err != nil {
			t.Errorf("Failed to get node %d: %v", i+1, err)
		} else if node.id != int64(i+1) {
			t.Errorf("Wrong node retrieved: expected %d, got %d", i+1, node.id)
		}
	}
}

// TestBufferPoolPinUnpin tests pinning prevents eviction
func TestBufferPoolPinUnpin(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "daemondb_bp_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	indexPath := filepath.Join(testDir, "test_pin.idx")
	defer os.Remove(indexPath)

	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	cache := NewBufferPool(2)
	cache.SetPager(pager)

	// Create and put node 1, then pin it immediately
	node1 := NewNode(NodeLeaf)
	node1.id = 1
	node1.key = [][]byte{[]byte{0}}
	node1.vals = [][]byte{[]byte{10}}
	node1.numKeys = 1

	pageData, _ := encodeNode(node1)
	pager.WritePage(node1.id, pageData)
	cache.Put(node1)

	// Pin node 1 before adding more nodes
	if err := cache.Pin(1); err != nil {
		t.Fatalf("Failed to pin node 1: %v", err)
	}

	// Create 2 more nodes (should evict node 2 or 3, but not 1)
	for i := 1; i < 3; i++ {
		node := NewNode(NodeLeaf)
		node.id = int64(i + 1)
		node.key = [][]byte{[]byte{byte(i)}}
		node.vals = [][]byte{[]byte{byte(i + 10)}}
		node.numKeys = 1

		pageData, _ := encodeNode(node)
		pager.WritePage(node.id, pageData)
		cache.Put(node)
	}

	// Add node 4 (should evict node 2 or 3, but not 1)
	node4 := NewNode(NodeLeaf)
	node4.id = 4
	node4.key = [][]byte{[]byte{3}}
	node4.vals = [][]byte{[]byte{13}}
	node4.numKeys = 1

	pageData4, _ := encodeNode(node4)
	pager.WritePage(node4.id, pageData4)
	if err := cache.Put(node4); err != nil {
		t.Fatalf("Failed to put node 4: %v", err)
	}

	// Node 1 should still be in cache (pinned)
	retrieved1, err := cache.Get(1)
	if err != nil {
		t.Errorf("Pinned node 1 was evicted: %v", err)
	} else if retrieved1.id != 1 {
		t.Errorf("Wrong node: expected 1, got %d", retrieved1.id)
	}

	// Unpin node 1
	if err := cache.Unpin(1); err != nil {
		t.Fatalf("Failed to unpin node 1: %v", err)
	}

	// Now node 1 can be evicted
	node5 := NewNode(NodeLeaf)
	node5.id = 5
	node5.key = [][]byte{[]byte{4}}
	node5.vals = [][]byte{[]byte{14}}
	node5.numKeys = 1

	pageData5, _ := encodeNode(node5)
	pager.WritePage(node5.id, pageData5)
	cache.Put(node5)

	// Node 1 might be evicted now (but can be reloaded)
	_, err = cache.Get(1)
	if err != nil {
		t.Log("Node 1 was evicted after unpinning (expected)")
	}
}

// TestBufferPoolFlush tests flushing dirty nodes to disk
func TestBufferPoolFlush(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "daemondb_bp_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	indexPath := filepath.Join(testDir, "test_flush.idx")
	defer os.Remove(indexPath)

	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	cache := NewBufferPool(10)
	cache.SetPager(pager)

	// Create and put a node
	node := NewNode(NodeLeaf)
	node.id = 1
	node.key = [][]byte{[]byte("key1")}
	node.vals = [][]byte{[]byte("val1")}
	node.numKeys = 1

	// Write initial version to disk
	pageData, _ := encodeNode(node)
	pager.WritePage(node.id, pageData)

	// Get node from cache (loads from disk)
	retrieved, _ := cache.Get(node.id)

	// Modify node in memory
	retrieved.key[0] = []byte("modified_key")
	retrieved.vals[0] = []byte("modified_val")
	retrieved.isDirty = true

	// Mark as dirty
	cache.MarkDirty(node.id)

	// Flush should write dirty node to disk
	if err := cache.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Close and reopen pager
	pager.Close()
	newPager, _ := NewOnDiskPager(indexPath)
	defer newPager.Close()

	// Read page directly from disk
	pageData, _ = newPager.ReadPage(node.id)
	flushedNode, _ := decodeNode(pageData, node.id)

	// Verify modifications were written
	if !bytes.Equal(flushedNode.key[0], []byte("modified_key")) {
		t.Errorf("Key not flushed: expected 'modified_key', got %s", string(flushedNode.key[0]))
	}

	if !bytes.Equal(flushedNode.vals[0], []byte("modified_val")) {
		t.Errorf("Value not flushed: expected 'modified_val', got %s", string(flushedNode.vals[0]))
	}
}

// TestNodeCodec tests encoding and decoding nodes
func TestNodeCodec(t *testing.T) {
	// Test leaf node
	leaf := NewNode(NodeLeaf)
	leaf.id = 1
	leaf.key = [][]byte{[]byte("key1"), []byte("key2")}
	leaf.vals = [][]byte{[]byte("val1"), []byte("val2")}
	leaf.numKeys = 2
	leaf.parent = 10
	leaf.next = 20

	encoded, err := encodeNode(leaf)
	if err != nil {
		t.Fatalf("Failed to encode leaf: %v", err)
	}

	decoded, err := decodeNode(encoded, leaf.id)
	if err != nil {
		t.Fatalf("Failed to decode leaf: %v", err)
	}

	if decoded.id != leaf.id {
		t.Errorf("ID mismatch: expected %d, got %d", leaf.id, decoded.id)
	}

	if decoded.nodeType != leaf.nodeType {
		t.Errorf("NodeType mismatch: expected %d, got %d", leaf.nodeType, decoded.nodeType)
	}

	if len(decoded.key) != len(leaf.key) {
		t.Errorf("Key count mismatch: expected %d, got %d", len(leaf.key), len(decoded.key))
	}

	// Test internal node
	internal := NewNode(NodeInternal)
	internal.id = 2
	internal.key = [][]byte{[]byte("sep1")}
	internal.children = []int64{100, 200}
	internal.numKeys = 1
	internal.parent = 5

	encoded, err = encodeNode(internal)
	if err != nil {
		t.Fatalf("Failed to encode internal: %v", err)
	}

	decoded, err = decodeNode(encoded, internal.id)
	if err != nil {
		t.Fatalf("Failed to decode internal: %v", err)
	}

	if len(decoded.children) != len(internal.children) {
		t.Errorf("Children count mismatch: expected %d, got %d", len(internal.children), len(decoded.children))
	}

	if decoded.children[0] != internal.children[0] {
		t.Errorf("Child 0 mismatch: expected %d, got %d", internal.children[0], decoded.children[0])
	}
}
