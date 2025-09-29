package main

// Search looks for a key in the B+Tree and returns its value if found, else nil.
func (t *BPlusTree) Search(key []byte) []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leaf := t.FindLeaf(t.root, key)
	if leaf == nil {
		return nil
	}

	for i := 0; i < len(leaf.key); i++ {
		if t.cmp(leaf.key[i], key) == 0 {
			return leaf.vals[i]
		}
	}
	return nil
}
