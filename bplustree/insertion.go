package bplus

func (t *BPlusTree) Insertion(key []byte, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// If tree is empty
	if t.root == 0 {
		root := NewNode(NodeLeaf)
		root.key = append(root.key, key)
		root.vals = append(root.vals, value)
		root.numKeys = 1
		newID, _ := t.pager.AllocatePage()
		root.id = newID
		t.root = root.id
		t.cache.Put(root)
		t.cache.MarkDirty(root.id)
		t.saveRoot()
		_ = t.cache.Flush()
		return
	}

	//find leaf
	leaf := t.FindLeaf(t.root, key)
	if leaf == nil {
		// Tree metadata may exist without nodes (e.g., fresh index file). Reinitialize root leaf.
		newLeaf := NewNode(NodeLeaf)
		newLeaf.key = append(newLeaf.key, key)
		newLeaf.vals = append(newLeaf.vals, value)
		newLeaf.numKeys = 1
		id, _ := t.pager.AllocatePage()
		newLeaf.id = id
		t.root = newLeaf.id
		t.cache.Put(newLeaf)
		t.cache.MarkDirty(newLeaf.id)
		t.saveRoot()
		_ = t.cache.Flush()
		return
	}
	defer t.cache.Unpin(leaf.id)

	i := binarySearchInsert(leaf.key, key, t.cmp)

	leaf.key = append(leaf.key[:i], append([][]byte{key}, leaf.key[i:]...)...)
	leaf.vals = append(leaf.vals[:i], append([][]byte{value}, leaf.vals[i:]...)...)
	leaf.numKeys = int16(len(leaf.key))
	t.cache.MarkDirty(leaf.id)

	if leaf.numKeys > MaxKeys {
		t.SplitLeaf(leaf)
	}

	// persist dirty nodes so reopening the tree can load nodes from disk
	_ = t.cache.Flush()
}
