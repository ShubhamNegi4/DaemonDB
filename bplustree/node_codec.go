package bplus

import (
	"encoding/binary"
	"fmt"
)

// encodeNode serializes a Node to a 4KB page
// Format:
//   - Header (16 bytes): id(8), nodeType(1), numKeys(2), parent(8), next(8), reserved(1)
//   - For internal nodes: keys + children
//   - For leaf nodes: keys + values
//   - Variable length fields prefixed with length
func encodeNode(node *Node) ([]byte, error) {
	page := make([]byte, PageSize)
	offset := 0

	// Encode header (16 bytes)
	binary.LittleEndian.PutUint64(page[offset:], uint64(node.id))
	offset += 8
	page[offset] = byte(node.nodeType)
	offset += 1
	binary.LittleEndian.PutUint16(page[offset:], uint16(node.numKeys))
	offset += 2
	binary.LittleEndian.PutUint64(page[offset:], uint64(node.parent))
	offset += 8
	binary.LittleEndian.PutUint64(page[offset:], uint64(node.next))
	offset += 8
	// Reserved byte
	offset += 1

	// Encode keys (all nodes have keys)
	for i := 0; i < int(node.numKeys); i++ {
		key := node.key[i]
		keyLen := len(key)
		if keyLen > MaxKeyLen {
			return nil, fmt.Errorf("key %d too long: %d bytes (max: %d)", i, keyLen, MaxKeyLen)
		}

		// Write key length (2 bytes) + key data
		binary.LittleEndian.PutUint16(page[offset:], uint16(keyLen))
		offset += 2
		copy(page[offset:], key)
		offset += keyLen
	}

	// Encode node-specific data
	if node.nodeType == NodeInternal {
		// Internal node: encode children (int64 each)
		for i := 0; i <= int(node.numKeys); i++ { // numKeys+1 children
			if i < len(node.children) {
				binary.LittleEndian.PutUint64(page[offset:], uint64(node.children[i]))
				offset += 8
			}
		}
	} else {
		// Leaf node: encode values
		for i := 0; i < int(node.numKeys); i++ {
			val := node.vals[i]
			valLen := len(val)
			if valLen > MaxValLen {
				return nil, fmt.Errorf("value %d too long: %d bytes (max: %d)", i, valLen, MaxValLen)
			}

			// Write value length (2 bytes) + value data
			binary.LittleEndian.PutUint16(page[offset:], uint16(valLen))
			offset += 2
			copy(page[offset:], val)
			offset += valLen
		}
	}

	return page, nil
}

// decodeNode deserializes a Node from a 4KB page
func decodeNode(page []byte, pageID int64) (*Node, error) {
	if len(page) != PageSize {
		return nil, fmt.Errorf("page size mismatch: expected %d, got %d", PageSize, len(page))
	}

	node := &Node{}
	offset := 0

	// Decode header
	node.id = int64(binary.LittleEndian.Uint64(page[offset:]))
	offset += 8
	node.nodeType = NodeType(page[offset])
	offset += 1
	node.numKeys = int16(binary.LittleEndian.Uint16(page[offset:]))
	offset += 2
	node.parent = int64(binary.LittleEndian.Uint64(page[offset:]))
	offset += 8
	node.next = int64(binary.LittleEndian.Uint64(page[offset:]))
	offset += 8
	// Reserved byte
	offset += 1

	// Decode keys
	node.key = make([][]byte, 0, node.numKeys)
	for i := 0; i < int(node.numKeys); i++ {
		if offset+2 > PageSize {
			return nil, fmt.Errorf("page overflow while reading key %d length", i)
		}

		keyLen := int(binary.LittleEndian.Uint16(page[offset:]))
		offset += 2

		if offset+keyLen > PageSize {
			return nil, fmt.Errorf("page overflow while reading key %d data", i)
		}

		key := make([]byte, keyLen)
		copy(key, page[offset:offset+keyLen])
		offset += keyLen
		node.key = append(node.key, key)
	}

	// Decode node-specific data
	if node.nodeType == NodeInternal {
		// Internal node: decode children
		node.children = make([]int64, 0, node.numKeys+1)
		for i := 0; i <= int(node.numKeys); i++ { // numKeys+1 children
			if offset+8 > PageSize {
				return nil, fmt.Errorf("page overflow while reading child %d", i)
			}
			childID := int64(binary.LittleEndian.Uint64(page[offset:]))
			offset += 8
			node.children = append(node.children, childID)
		}
		node.vals = nil
	} else {
		// Leaf node: decode values
		node.vals = make([][]byte, 0, node.numKeys)
		node.children = nil
		for i := 0; i < int(node.numKeys); i++ {
			if offset+2 > PageSize {
				return nil, fmt.Errorf("page overflow while reading value %d length", i)
			}

			valLen := int(binary.LittleEndian.Uint16(page[offset:]))
			offset += 2

			if offset+valLen > PageSize {
				return nil, fmt.Errorf("page overflow while reading value %d data", i)
			}

			val := make([]byte, valLen)
			copy(val, page[offset:offset+valLen])
			offset += valLen
			node.vals = append(node.vals, val)
		}
	}

	// Initialize other fields
	node.isDirty = false
	node.pincnt = 0

	return node, nil
}
