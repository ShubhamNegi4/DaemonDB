package bplus

// Search looks for a key in the B+Tree and returns its value if found, else nil.
func (t *BPlusTree) Search(key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leaf := t.FindLeaf(t.root, key)
	if leaf == nil {
		return []byte(nil), nil
	}
	//linear search
	// for i := 0; i < len(leaf.key); i++ {
	// 	if t.cmp(leaf.key[i], key) == 0 {
	// 		return leaf.vals[i]
	// 	}
	// }
	//binary search
	idx := binarySearch(leaf.key, key, t.cmp)
	if idx != -1 {
		return leaf.vals[idx], nil
	}
	return []byte(nil), nil
}
