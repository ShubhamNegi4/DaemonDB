package bplus

import (
	"DaemonDB/storage_engine/page"
	"encoding/binary"
	"fmt"
)

/*
SerializeNode writes a Node into a 4KB page buffer.
All page IDs (pageID, parent, next, children) are stored as LOCAL page IDs
(lower 32 bits only) so they remain valid across restarts regardless of
how global IDs are reassigned.

Layout:

	Header (34 bytes):
	  localPageID  int64  (8 bytes)
	  isLeaf       bool   (1 byte)  — 1=leaf, 0=internal
	  numKeys      int16  (2 bytes)
	  localParent  int64  (8 bytes) — -1 if no parent
	  localNext    int64  (8 bytes) — leaf-only, -1 if none
	  reserved            (7 bytes)

	Body:
	  numKeys × [ keyLen uint16 | key []byte ]
	  internal: (numKeys+1) × [ localChildID int64 ]
	  leaf:      numKeys    × [ valLen uint16 | val []byte ]

Local vs global IDs: All page IDs stored on disk are local (lower 32 bits).
On load, DeserializeNode reconstructs global IDs as int64(fileID)<<32 | localID.
In memory, all IDs are global. This ensures correctness across restarts regardless of load order.

Tree invariants:
	Internal nodes: len(children) == len(keys) + 1
	Leaf nodes: len(values) == len(keys)
	All leaves at same depth, linked via next pointer
	Keys sorted ascending, MaxKeys=32, MinKeys=16

*/

func SerializeNode(node *Node, data []byte) error {
	if len(data) != page.PageSize {
		return fmt.Errorf("serializeNode: data buffer must be %d bytes", page.PageSize)
	}

	// ── Header ────────────────────────────────────────────────────────────────

	// Store local pageID (lower 32 bits).
	localPageID := node.pageID & 0xFFFFFFFF
	// SerializeNode
	offset := 0

	// pageID at 0-7
	binary.LittleEndian.PutUint64(data[offset:], uint64(localPageID))
	offset += 8

	// byte 8 is reserved for WritePage page type stamp — skip it
	offset += 1

	// isLeaf at byte 9
	if node.nodeType == NodeLeaf {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset += 1

	// numKeys at 10-11
	numKeys := int16(len(node.keys))
	binary.LittleEndian.PutUint16(data[offset:], uint16(numKeys))
	offset += 2

	// parent at 12-19
	localParent := int64(-1)
	if node.parent >= 0 {
		localParent = node.parent & 0xFFFFFFFF
	}
	binary.LittleEndian.PutUint64(data[offset:], uint64(localParent))
	offset += 8

	// next at 20-27
	localNext := int64(-1)
	if node.next >= 0 {
		localNext = node.next & 0xFFFFFFFF
	}
	binary.LittleEndian.PutUint64(data[offset:], uint64(localNext))
	offset += 8

	// reserved 7 bytes
	offset += 7

	// ── Keys ──────────────────────────────────────────────────────────────────
	for _, key := range node.keys {
		keyLen := len(key)
		if keyLen > MaxKeyLen {
			return fmt.Errorf("serializeNode: key too long (%d bytes, max %d)", keyLen, MaxKeyLen)
		}
		if offset+2+keyLen > page.PageSize {
			return fmt.Errorf("serializeNode: page overflow while writing keys")
		}
		binary.LittleEndian.PutUint16(data[offset:], uint16(keyLen))
		offset += 2
		copy(data[offset:], key)
		offset += keyLen
	}

	// ── Node-specific data ────────────────────────────────────────────────────
	if node.nodeType == NodeLeaf {
		for _, val := range node.values {
			valLen := len(val)
			if valLen > MaxValLen {
				return fmt.Errorf("serializeNode: value too long (%d bytes, max %d)", valLen, MaxValLen)
			}
			if offset+2+valLen > page.PageSize {
				return fmt.Errorf("serializeNode: page overflow while writing values")
			}
			binary.LittleEndian.PutUint16(data[offset:], uint16(valLen))
			offset += 2
			copy(data[offset:], val)
			offset += valLen
		}
	} else {
		// Store local child IDs.
		for _, childID := range node.children {
			if offset+8 > page.PageSize {
				return fmt.Errorf("serializeNode: page overflow while writing children")
			}
			localChild := int64(-1)
			if childID >= 0 {
				localChild = childID & 0xFFFFFFFF
			}
			binary.LittleEndian.PutUint64(data[offset:], uint64(localChild))
			offset += 8
		}
	}

	return nil
}

// DeserializeNode reads a Node from a 4KB page buffer.
// fileID is required to reconstruct global page IDs from stored local IDs.
// The caller (fetchNode) always overrides node.pageID with the actual global
// page ID used to fetch the page — so the stored pageID is informational only.
func DeserializeNode(data []byte, fileID uint32) (*Node, error) {
	if len(data) != page.PageSize {
		return nil, fmt.Errorf("deserializeNode: data must be %d bytes", page.PageSize)
	}

	node := &Node{}
	offset := 0

	localPageID := int64(binary.LittleEndian.Uint64(data[offset:]))
	node.pageID = int64(fileID)<<32 | (localPageID & 0xFFFFFFFF)
	offset += 8

	offset += 1 // skip byte 8 (page type stamp)

	if data[offset] == 1 {
		node.nodeType = NodeLeaf
	} else {
		node.nodeType = NodeInternal
	}
	offset += 1

	numKeys := int16(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	localParent := int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	if localParent < 0 {
		node.parent = -1
	} else {
		node.parent = int64(fileID)<<32 | (localParent & 0xFFFFFFFF)
	}

	localNext := int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	if localNext < 0 {
		node.next = -1
	} else {
		node.next = int64(fileID)<<32 | (localNext & 0xFFFFFFFF)
	}

	offset += 7 // reserved

	// ── Keys ──────────────────────────────────────────────────────────────────
	node.keys = make([][]byte, 0, numKeys)
	for i := int16(0); i < numKeys; i++ {
		if offset+2 > page.PageSize {
			return nil, fmt.Errorf("deserializeNode: page overflow reading key %d length", i)
		}
		keyLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2

		if offset+keyLen > page.PageSize {
			return nil, fmt.Errorf("deserializeNode: page overflow reading key %d data", i)
		}
		key := make([]byte, keyLen)
		copy(key, data[offset:offset+keyLen])
		offset += keyLen
		node.keys = append(node.keys, key)
	}

	// ── Node-specific data ────────────────────────────────────────────────────
	if node.nodeType == NodeLeaf {
		node.values = make([][]byte, 0, numKeys)
		for i := int16(0); i < numKeys; i++ {
			if offset+2 > page.PageSize {
				return nil, fmt.Errorf("deserializeNode: page overflow reading value %d length", i)
			}
			valLen := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2

			if offset+valLen > page.PageSize {
				return nil, fmt.Errorf("deserializeNode: page overflow reading value %d data", i)
			}
			val := make([]byte, valLen)
			copy(val, data[offset:offset+valLen])
			offset += valLen
			node.values = append(node.values, val)
		}
		node.children = nil
	} else {
		node.children = make([]int64, 0, numKeys+1)
		for i := int16(0); i <= numKeys; i++ {
			if offset+8 > page.PageSize {
				return nil, fmt.Errorf("deserializeNode: page overflow reading child %d", i)
			}
			localChild := int64(binary.LittleEndian.Uint64(data[offset:]))
			offset += 8
			if localChild < 0 {
				node.children = append(node.children, -1)
			} else {
				node.children = append(node.children, int64(fileID)<<32|(localChild&0xFFFFFFFF))
			}
		}
		node.values = nil
	}

	return node, nil
}
