package bplus

func (t *BPlusTree) Delete(key []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.deleteRecursive(t.root, key)

	// Persist dirty nodes so delete is durable
	_ = t.cache.Flush()
	_ = t.pager.Sync()
}

func (t *BPlusTree) deleteRecursive(nodeId int64, key []byte) bool {
	node, _ := t.cache.Get(nodeId)
	if node.nodeType == NodeLeaf {
		idx := -1
		for i := 0; i < len(node.key); i++ {
			if t.cmp(node.key[i], key) == 0 {
				idx = i
				break
			}
		}
		if idx == -1 {
			return false
		}
		node.key = append(node.key[:idx], node.key[idx+1:]...)
		node.vals = append(node.vals[:idx], node.vals[idx+1:]...)
		node.numKeys--
		t.cache.MarkDirty(node.id)
		return len(node.key) < (MaxKeys+1)/2
	}
	i := 0
	for i < len(node.key) && t.cmp(node.key[i], key) < 0 {
		i++
	}
	underflow := t.deleteRecursive(node.children[i], key)
	if !underflow {
		return false
	}
	child, _ := t.cache.Get(node.children[i])
	var left *Node
	var right *Node
	if i > 0 {
		left, _ = t.cache.Get(node.children[i-1])
	}
	if i < len(node.children)-1 {
		right, _ = t.cache.Get(node.children[i+1])
	}
	// Try borrow from left sibling
	if left != nil && len(left.key) > (MaxKeys+1)/2 {
		// move one key from left to child
		if child.nodeType == NodeLeaf {
			// prepend left's last key/value
			child.key = append([][]byte{left.key[len(left.key)-1]}, child.key...)
			child.vals = append([][]byte{left.vals[len(left.vals)-1]}, child.vals...)
			left.vals = left.vals[:len(left.vals)-1]
		} else {
			// internal: move separator from parent down, and child's first promoted up later
			child.key = append([][]byte{node.key[i-1]}, child.key...)
			child.children = append([]int64{left.children[len(left.children)-1]}, child.children...)
			left.children = left.children[:len(left.children)-1]
			// parent separator becomes left's last key
			node.key[i-1] = left.key[len(left.key)-1]
		}
		left.key = left.key[:len(left.key)-1]
		if child.nodeType == NodeLeaf {
			// update parent separator to child's first key
			node.key[i-1] = child.key[0]
		}
		child.numKeys = int16(len(child.key))
		left.numKeys = int16(len(left.key))
		node.numKeys = int16(len(node.key))
		t.cache.MarkDirty(child.id)
		t.cache.MarkDirty(left.id)
		t.cache.MarkDirty(node.id)
		return false
	}
	// Try borrow from right sibling
	if right != nil && len(right.key) > (MaxKeys+1)/2 {
		if child.nodeType == NodeLeaf {
			child.key = append(child.key, right.key[0])
			child.vals = append(child.vals, right.vals[0])
			right.vals = right.vals[1:]
			// parent separator becomes right's new first key
			right.key = right.key[1:]
			node.key[i] = right.key[0]
		} else {
			// internal
			child.key = append(child.key, node.key[i])
			child.children = append(child.children, right.children[0])
			right.children = right.children[1:]
			// parent separator becomes right's first key
			node.key[i] = right.key[0]
			right.key = right.key[1:]
		}
		child.numKeys = int16(len(child.key))
		right.numKeys = int16(len(right.key))
		node.numKeys = int16(len(node.key))
		t.cache.MarkDirty(child.id)
		t.cache.MarkDirty(right.id)
		t.cache.MarkDirty(node.id)
		return false
	}
	if left != nil {
		// merge child into left
		left.key = append(left.key, child.key...)
		if child.nodeType == NodeLeaf {
			left.vals = append(left.vals, child.vals...)
			// fix leaf linking: left.next should skip removed child if needed
			left.next = child.next
		} else {
			left.children = append(left.children, child.children...)
			// update parent pointer for moved children
			for _, cid := range child.children {
				if c, _ := t.cache.Get(cid); c != nil {
					c.parent = left.id
					t.cache.MarkDirty(c.id)
				}
			}
		}
		// remove separator key at i-1 and child at i
		node.key = append(node.key[:i-1], node.key[i:]...)
		node.children = append(node.children[:i], node.children[i+1:]...)
	} else if right != nil {
		// merge right into child
		child.key = append(child.key, right.key...)
		if child.nodeType == NodeLeaf {
			child.vals = append(child.vals, right.vals...)
			child.next = right.next
		} else {
			child.children = append(child.children, right.children...)
			for _, cid := range right.children {
				if c, _ := t.cache.Get(cid); c != nil {
					c.parent = child.id
					t.cache.MarkDirty(c.id)
				}
			}
		}
		// remove separator key at i and right child at i+1
		node.key = append(node.key[:i], node.key[i+1:]...)
		node.children = append(node.children[:i+1], node.children[i+2:]...)
	}
	node.numKeys = int16(len(node.key))
	if nodeId == t.root && len(node.key) == 0 && len(node.children) > 0 {
		// collapse root
		t.root = node.children[0]
		if r, _ := t.cache.Get(t.root); r != nil {
			r.parent = 0
			t.cache.MarkDirty(r.id)
		}
		t.saveRoot()
	}
	t.cache.MarkDirty(node.id)
	return len(node.key) < (MaxKeys+1)/2
}
