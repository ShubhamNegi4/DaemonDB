package bplus

// SplitInternal splits a full internal node and promotes the middle key.
func (t *BPlusTree) SplitInternal(node *Node) {
	// mid is the index of the key to promote
	mid := int(node.numKeys) / 2

	right := NewNode(NodeInternal)
	right.id, _ = t.pager.AllocatePage()

	// keys: left keeps [0:mid), promote key[mid], right gets (mid, end]
	// children: left keeps [0:mid], right gets [mid+1:]
	promote := node.key[mid]

	right.key = append(right.key, node.key[mid+1:]...)
	right.children = append(right.children, node.children[mid+1:]...)
	right.numKeys = int16(len(right.key))

	// update parent pointers for children moved to right
	for _, cid := range right.children {
		if c, _ := t.cache.Get(cid); c != nil {
			_ = t.cache.Pin(c.id)
			c.parent = right.id
			t.cache.MarkDirty(c.id)
			_ = t.cache.Unpin(c.id)
		}
	}

	// shrink left node
	node.key = node.key[:mid]
	node.children = node.children[:mid+1]
	node.numKeys = int16(len(node.key))

	// set right's parent
	right.parent = node.parent

	// write right to cache
	t.cache.Put(right)
	t.cache.MarkDirty(right.id)

	if node.id == t.root {
		// create new root
		newRoot := NewNode(NodeInternal)
		newRoot.id, _ = t.pager.AllocatePage()
		newRoot.key = append(newRoot.key, promote)
		newRoot.children = append(newRoot.children, node.id, right.id)
		newRoot.numKeys = 1
		node.parent = newRoot.id
		right.parent = newRoot.id
		t.root = newRoot.id
		t.cache.Put(newRoot)
		t.cache.MarkDirty(newRoot.id)
		t.cache.MarkDirty(node.id)
		t.cache.MarkDirty(right.id)
		t.saveRoot()
		return
	}

	// insert promote into parent
	t.insertIntoParent(node.parent, node.id, promote, right.id)
}
