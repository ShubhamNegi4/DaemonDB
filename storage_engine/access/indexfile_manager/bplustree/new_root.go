package bplus

import "fmt"

// createNewRoot creates a new root internal node with leftPageID and rightPageID
// as its two children, separated by promoteKey.
func (t *BPlusTree) createNewRoot(leftPageID int64, promoteKey []byte, rightPageID int64) error {
	root, err := t.newNode(NodeInternal)
	if err != nil {
		return fmt.Errorf("createNewRoot: failed to allocate new root: %w", err)
	}
	defer t.releaseNode(root, true)

	root.keys = append(root.keys, promoteKey)
	root.children = append(root.children, leftPageID, rightPageID)
	root.parent = -1

	// Update parent pointers on both children.
	for _, childID := range []int64{leftPageID, rightPageID} {
		child, err := t.fetchNode(childID)
		if err != nil {
			return fmt.Errorf("createNewRoot: failed to fetch child %d: %w", childID, err)
		}
		child.parent = root.pageID
		child.isDirty = true
		if err := t.writeNode(child); err != nil {
			t.releaseNode(child, false)
			return err
		}
		t.releaseNode(child, true)
	}

	root.isDirty = true
	if err := t.writeNode(root); err != nil {
		return err
	}

	t.root = root.pageID
	return t.saveRoot()
}
