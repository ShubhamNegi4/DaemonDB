package bplus

// Iterator provides a forward-only range scan over the leaves.
type Iterator struct {
	tree  *BPlusTree
	leaf  *Node
	index int
	valid bool
}

// SeekGE positions the iterator at the first key >= target.
// The iterator holds a pinned leaf; call Close() when done to release it.
func (t *BPlusTree) SeekGE(target []byte) *Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()

	it := &Iterator{tree: t}
	if t.root < 0 {
		it.valid = false
		return it
	}

	leaf, err := t.FindLeaf(t.root, target)
	if err != nil || leaf == nil {
		it.valid = false
		return it
	}
	if len(leaf.keys) == 0 {
		_ = t.bufferPool.UnpinPage(leaf.pageID, false)
		it.valid = false
		return it
	}

	i := lowerBound(leaf.keys, target, t.cmp)
	if i >= len(leaf.keys) {
		if leaf.next <= 0 {
			_ = t.bufferPool.UnpinPage(leaf.pageID, false)
			it.valid = false
			return it
		}
		next, err := t.fetchNode(leaf.next)
		_ = t.bufferPool.UnpinPage(leaf.pageID, false)
		if err != nil || next == nil || len(next.keys) == 0 {
			it.valid = false
			return it
		}
		it.leaf = next
		it.index = 0
		it.valid = true
		return it
	}

	it.leaf = leaf
	it.index = i
	it.valid = true
	return it
}

// Next advances the iterator. Returns false when exhausted.
func (it *Iterator) Next() bool {
	if !it.valid {
		return false
	}
	it.index++
	if it.index < len(it.leaf.keys) {
		return true
	}

	nextId := it.leaf.next
	_ = it.tree.bufferPool.UnpinPage(it.leaf.pageID, false)
	if nextId <= 0 {
		it.leaf = nil
		it.valid = false
		return false
	}

	next, err := it.tree.fetchNode(nextId)
	if err != nil || next == nil || len(next.keys) == 0 {
		it.leaf = nil
		it.valid = false
		return false
	}

	it.leaf = next
	it.index = 0
	return true
}

// Close releases the pinned leaf. Call when done with the iterator.
func (it *Iterator) Close() {
	if it.leaf != nil {
		_ = it.tree.bufferPool.UnpinPage(it.leaf.pageID, false)
		it.leaf = nil
	}
	it.valid = false
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.leaf.keys[it.index]
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.leaf.values[it.index]
}
