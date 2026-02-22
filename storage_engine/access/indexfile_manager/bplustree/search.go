package bplus

import "fmt"

// Search looks for a key in the B+Tree and returns its value if found, else nil.
func (t *BPlusTree) Search(key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root < 0 {
		return nil, nil
	}

	leaf, err := t.FindLeaf(t.root, key)
	if err != nil {
		return nil, fmt.Errorf("failed to find leaf: %w", err)
	}
	if leaf == nil {
		return nil, nil
	}
	defer t.bufferPool.UnpinPage(leaf.pageID, false)

	idx := binarySearch(leaf.keys, key, t.cmp)
	if idx != -1 {
		return leaf.values[idx], nil
	}
	return nil, nil
}
