package bplus

import "fmt"

func (t *BPlusTree) SplitLeaf(leaf *Node) error {
	mid := len(leaf.keys) / 2

	right, err := t.newNode(NodeLeaf)
	if err != nil {
		return fmt.Errorf("splitLeaf: failed to allocate right sibling: %w", err)
	}
	defer t.releaseNode(right, true)

	right.keys = append(right.keys, leaf.keys[mid:]...)
	right.values = append(right.values, leaf.values[mid:]...)
	right.next = leaf.next // right inherits leaf's old next pointer
	right.parent = leaf.parent
	right.isDirty = true

	leaf.keys = leaf.keys[:mid]
	leaf.values = leaf.values[:mid]
	leaf.next = right.pageID
	leaf.isDirty = true

	if err := t.writeNode(leaf); err != nil {
		return err
	}
	if err := t.writeNode(right); err != nil {
		return err
	}

	sepKey := right.keys[0]

	if leaf.pageID == t.root {
		return t.createNewRoot(leaf.pageID, sepKey, right.pageID)
	}
	return t.insertIntoParent(leaf.parent, leaf.pageID, sepKey, right.pageID)
}
