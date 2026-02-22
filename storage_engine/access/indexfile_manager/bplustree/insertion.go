package bplus

import "fmt"

func (t *BPlusTree) Insertion(key []byte, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// If tree is empty
	if t.root < 0 {
		root, err := t.newNode(NodeLeaf)
		if err != nil {
			return fmt.Errorf("Insertion: failed to allocate root: %w", err)
		}
		root.keys = append(root.keys, key)
		root.values = append(root.values, value)
		root.isDirty = true
		if err := t.writeNode(root); err != nil {
			return err
		}
		t.root = root.pageID
		if err := t.saveRoot(); err != nil {
			return err
		}
		return nil
	}

	//find leaf
	leaf, err := t.FindLeaf(t.root, key)
	if err != nil {
		return fmt.Errorf("Insertion: failed to find leaf: %w", err)
	}
	if leaf == nil {
		return fmt.Errorf("Insertion: leaf is nil for root=%d", t.root)
	}
	defer t.bufferPool.UnpinPage(leaf.pageID, true)

	idx := binarySearch(leaf.keys, key, t.cmp)
	if idx != -1 {
		// Key exists — update value in place.
		leaf.values[idx] = value
		leaf.isDirty = true
		return t.writeNode(leaf)
	}

	// Insert key/value in sorted position.
	insertPos := lowerBound(leaf.keys, key, t.cmp)
	if insertPos < 0 {
		insertPos = 0
	}
	leaf.keys = insert(leaf.keys, insertPos, key)
	leaf.values = insert(leaf.values, insertPos, value)
	leaf.isDirty = true
	if err := t.writeNode(leaf); err != nil {
		return err
	}

	// Split if overflow.
	if len(leaf.keys) > MaxKeys {
		return t.SplitLeaf(leaf)
	}
	return nil
}
