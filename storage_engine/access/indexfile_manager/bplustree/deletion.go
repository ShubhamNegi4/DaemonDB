package bplus

func (t *BPlusTree) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.root == 0 {
		return nil // empty tree
	}

	underflow := t.deleteRecursive(t.root, key)
	if underflow {
		// Root underflow handled in deleteRecursive (collapse).
	}

	// Flush all dirty pages.
	return t.bufferPool.FlushAllPages()
}

func (t *BPlusTree) deleteRecursive(nodeId int64, key []byte) bool {
	node, err := t.fetchNode(nodeId)
	if err != nil {
		return false
	}
	defer t.bufferPool.UnpinPage(nodeId, true)

	if node.nodeType == NodeLeaf {
		idx := binarySearch(node.keys, key, t.cmp)
		if idx == -1 {
			return false // key not found
		}

		// Remove key and value.
		node.keys = remove(node.keys, idx)
		node.values = remove(node.values, idx)
		_ = t.writeNode(node)

		// Underflow if below MinKeys (except root).
		return len(node.keys) < MinKeys
	}
	i := lowerBound(node.keys, key, t.cmp)
	if i < 0 {
		i = 0
	}
	if i >= len(node.children) {
		i = len(node.children) - 1
	}
	underflow := t.deleteRecursive(node.children[i], key)
	if !underflow {
		return false
	}

	// Child underflowed — try borrow or merge.
	child, err := t.fetchNode(node.children[i])
	if err != nil {
		return false
	}
	defer t.bufferPool.UnpinPage(node.children[i], true)

	var left *Node
	var right *Node
	var leftID, rightID int64

	if i > 0 {
		leftID = node.children[i-1]
		left, err = t.fetchNode(leftID)
		if err == nil {
			defer t.bufferPool.UnpinPage(leftID, true)
		}
	}
	if i < len(node.children)-1 {
		rightID = node.children[i+1]
		right, err = t.fetchNode(rightID)
		if err == nil {
			defer t.bufferPool.UnpinPage(rightID, true)
		}
	}
	// ── Try borrow from left sibling ──────────────────────────────────────────
	if left != nil && len(left.keys) > MinKeys {
		if child.nodeType == NodeLeaf {
			// Leaf borrow: move left's last key/value to child's front.
			lastKey := left.keys[len(left.keys)-1]
			lastVal := left.values[len(left.values)-1]
			left.keys = left.keys[:len(left.keys)-1]
			left.values = left.values[:len(left.values)-1]

			child.keys = insert(child.keys, 0, lastKey)
			child.values = insert(child.values, 0, lastVal)

			// Update parent separator to child's new first key.
			node.keys[i-1] = child.keys[0]
		} else {
			// Internal borrow: rotate through parent.
			// Move separator from parent down to child, and left's last key up to parent.
			separatorKey := node.keys[i-1]
			lastKey := left.keys[len(left.keys)-1]
			lastChild := left.children[len(left.children)-1]

			left.keys = left.keys[:len(left.keys)-1]
			left.children = left.children[:len(left.children)-1]

			child.keys = insert(child.keys, 0, separatorKey)
			child.children = insert(child.children, 0, lastChild)

			// Update moved child's parent pointer.
			movedChild, err := t.fetchNode(lastChild)
			if err == nil {
				movedChild.parent = child.pageID
				_ = t.writeNode(movedChild)
				_ = t.bufferPool.UnpinPage(lastChild, true)
			}

			// Parent separator becomes left's last key.
			node.keys[i-1] = lastKey
		}

		_ = t.writeNode(left)
		_ = t.writeNode(child)
		_ = t.writeNode(node)
		return false
	}

	// ── Try borrow from right sibling ─────────────────────────────────────────
	if right != nil && len(right.keys) > MinKeys {
		if child.nodeType == NodeLeaf {
			// Leaf borrow: move right's first key/value to child's end.
			firstKey := right.keys[0]
			firstVal := right.values[0]
			right.keys = right.keys[1:]
			right.values = right.values[1:]

			child.keys = append(child.keys, firstKey)
			child.values = append(child.values, firstVal)

			// Update parent separator to right's new first key.
			node.keys[i] = right.keys[0]
		} else {
			// Internal borrow: rotate through parent.
			separatorKey := node.keys[i]
			firstKey := right.keys[0]
			firstChild := right.children[0]

			right.keys = right.keys[1:]
			right.children = right.children[1:]

			child.keys = append(child.keys, separatorKey)
			child.children = append(child.children, firstChild)

			// Update moved child's parent pointer.
			movedChild, err := t.fetchNode(firstChild)
			if err == nil {
				movedChild.parent = child.pageID
				_ = t.writeNode(movedChild)
				_ = t.bufferPool.UnpinPage(firstChild, true)
			}

			// Parent separator becomes right's first key.
			node.keys[i] = firstKey
		}

		_ = t.writeNode(right)
		_ = t.writeNode(child)
		_ = t.writeNode(node)
		return false
	}

	// ── Merge with a sibling ──────────────────────────────────────────────────
	if left != nil {
		// Merge child into left.
		if child.nodeType == NodeLeaf {
			left.keys = append(left.keys, child.keys...)
			left.values = append(left.values, child.values...)
			left.next = child.next // skip over merged child in leaf chain
		} else {
			// Internal merge: pull separator from parent down.
			separatorKey := node.keys[i-1]
			left.keys = append(left.keys, separatorKey)
			left.keys = append(left.keys, child.keys...)
			left.children = append(left.children, child.children...)

			// Update parent pointers of moved children.
			for _, childID := range child.children {
				movedChild, err := t.fetchNode(childID)
				if err == nil {
					movedChild.parent = left.pageID
					_ = t.writeNode(movedChild)
					_ = t.bufferPool.UnpinPage(childID, true)
				}
			}
		}

		// Remove separator key at i-1 and child at i from parent.
		node.keys = remove(node.keys, i-1)
		node.children = remove(node.children, i)

		_ = t.writeNode(left)
	} else if right != nil {
		// Merge right into child.
		if child.nodeType == NodeLeaf {
			child.keys = append(child.keys, right.keys...)
			child.values = append(child.values, right.values...)
			child.next = right.next
		} else {
			// Internal merge: pull separator from parent down.
			separatorKey := node.keys[i]
			child.keys = append(child.keys, separatorKey)
			child.keys = append(child.keys, right.keys...)
			child.children = append(child.children, right.children...)

			// Update parent pointers of moved children.
			for _, childID := range right.children {
				movedChild, err := t.fetchNode(childID)
				if err == nil {
					movedChild.parent = child.pageID
					_ = t.writeNode(movedChild)
					_ = t.bufferPool.UnpinPage(childID, true)
				}
			}
		}

		// Remove separator key at i and right child at i+1 from parent.
		node.keys = remove(node.keys, i)
		node.children = remove(node.children, i+1)

		_ = t.writeNode(child)
	}

	_ = t.writeNode(node)

	// ── Root collapse ─────────────────────────────────────────────────────────
	// If root becomes empty but has one child, promote the child to root.
	if nodeId == t.root && len(node.keys) == 0 && len(node.children) > 0 {
		newRoot := node.children[0]
		newRootNode, err := t.fetchNode(newRoot)
		if err == nil {
			newRootNode.parent = 0
			_ = t.writeNode(newRootNode)
			_ = t.bufferPool.UnpinPage(newRoot, true)
		}
		t.root = newRoot
		_ = t.saveRoot()
	}

	return len(node.keys) < MinKeys
}
