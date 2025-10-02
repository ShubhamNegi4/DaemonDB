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
		t.cache.pages[root.id] = root
		return
	}

	//find leaf
	leaf := t.FindLeaf(t.root, key)

	i := binarySearchInsert(leaf.key, key, t.cmp)

	leaf.key = append(leaf.key[:i], append([][]byte{key}, leaf.key[i:]...)...)
	leaf.vals = append(leaf.vals[:i], append([][]byte{value}, leaf.vals[i:]...)...)
	leaf.numKeys = int16(len(leaf.key))
	t.cache.pages[leaf.id] = leaf

	if leaf.numKeys > MaxKeys {
		t.SplitLeaf(leaf)
	}

}
