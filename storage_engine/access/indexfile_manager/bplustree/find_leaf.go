package bplus

import "fmt"

func (t *BPlusTree) FindLeaf(nodeId int64, key []byte) (*Node, error) {
	for {
		if nodeId == 0 {
			return nil, fmt.Errorf("FindLeaf: invalid node ID 0")
		}
		node, err := t.fetchNode(nodeId)
		if err != nil {
			return nil, fmt.Errorf("FindLeaf: failed to fetch node %d: %w", nodeId, err)
		}

		// Leaf found — caller unpins.
		if node.nodeType == NodeLeaf {
			return node, nil
		}
		i := lowerBound(node.keys, key, t.cmp)
		if i < 0 {
			i = 0
		}
		if i >= len(node.children) {
			if len(node.children) == 0 {
				_ = t.bufferPool.UnpinPage(nodeId, false)
				return nil, fmt.Errorf("FindLeaf: internal node %d has no children", nodeId)
			}
			i = len(node.children) - 1
		}
		nextId := node.children[i]
		_ = t.bufferPool.UnpinPage(nodeId, false)
		nodeId = nextId
	}
}
