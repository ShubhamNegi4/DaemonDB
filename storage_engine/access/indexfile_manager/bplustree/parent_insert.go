package bplus

import "fmt"

// insertIntoParent inserts sepKey and rightId into the parent of leftId.
// If the parent overflows, it splits and propagates upward.
func (t *BPlusTree) insertIntoParent(parentId int64, leftId int64, sepKey []byte, rightId int64) error {
	parent, err := t.fetchNode(parentId)
	if err != nil {
		return fmt.Errorf("insertIntoParent: failed to fetch parent %d: %w", parentId, err)
	}

	// Find leftID in parent's children.
	idx := 0
	for idx < len(parent.children) && parent.children[idx] != leftId {
		idx++
	}
	if idx > len(parent.children) {
		idx = len(parent.children)
	}

	// Insert sepKey at idx, rightID at idx+1.
	parent.keys = insert(parent.keys, idx, sepKey)
	parent.children = insert(parent.children, idx+1, rightId)

	// Update right child's parent pointer.
	right, err := t.fetchNode(rightId)
	if err == nil {
		right.parent = parentId
		right.isDirty = true
		_ = t.writeNode(right)
		_ = t.bufferPool.UnpinPage(rightId, true)
	}
	parent.isDirty = true
	if err := t.writeNode(parent); err != nil {
		return err
	}

	// Split parent if overflow.
	if len(parent.keys) > MaxKeys {
		return t.splitInternal(parent)
	}

	return nil
}
