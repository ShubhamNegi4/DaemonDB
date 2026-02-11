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
	leaf := t.FindLeaf(t.root, target)
	if leaf == nil || len(leaf.key) == 0 {
		it.valid = false
		return it
	}
	i := lowerBound(leaf.key, target, t.cmp)
	if i >= len(leaf.key) {
		// move to next leaf if present
		if leaf.next == 0 {
			_ = t.cache.Unpin(leaf.id)
			it.valid = false
			return it
		}
		next, _ := t.cache.Get(leaf.next)
		_ = t.cache.Unpin(leaf.id)
		if next == nil || len(next.key) == 0 {
			it.valid = false
			return it
		}
		_ = t.cache.Pin(next.id)
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
	if it.index < len(it.leaf.key) {
		return true
	}
	// move to next leaf
	nextId := it.leaf.next
	_ = it.tree.cache.Unpin(it.leaf.id)
	if nextId == 0 {
		it.leaf = nil
		it.valid = false
		return false
	}
	next, _ := it.tree.cache.Get(nextId)
	if next == nil || len(next.key) == 0 {
		it.leaf = nil
		it.valid = false
		return false
	}
	_ = it.tree.cache.Pin(next.id)
	it.leaf = next
	it.index = 0
	return true
}

// Close releases the pinned leaf. Call when done with the iterator.
func (it *Iterator) Close() {
	if it.leaf != nil {
		_ = it.tree.cache.Unpin(it.leaf.id)
		it.leaf = nil
	}
	it.valid = false
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.leaf.key[it.index]
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.leaf.vals[it.index]
}
