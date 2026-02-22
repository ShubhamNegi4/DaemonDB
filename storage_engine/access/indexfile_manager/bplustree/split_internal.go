package bplus

import "fmt"

// SplitInternal splits a full internal node and promotes the middle key.
func (t *BPlusTree) splitInternal(node *Node) error {
	// mid is the index of the key to promote
	mid := len(node.keys) / 2

	promoteKey := node.keys[mid]

	// Allocate right sibling.
	right, err := t.newNode(NodeInternal)
	if err != nil {
		return fmt.Errorf("splitInternal: failed to allocate right sibling: %w", err)
	}
	defer t.releaseNode(right, true)

	right.keys = append(right.keys, node.keys[mid+1:]...)
	right.children = append(right.children, node.children[mid+1:]...)
	right.parent = node.parent

	// Update parent pointers of moved children.
	for _, childID := range right.children {
		child, err := t.fetchNode(childID)
		if err != nil {
			return fmt.Errorf("splitInternal: failed to fetch child %d: %w", childID, err)
		}
		child.parent = right.pageID
		child.isDirty = true
		if err := t.writeNode(child); err != nil {
			t.releaseNode(child, false)
			return err
		}
		t.releaseNode(child, true)
	}

	// Shrink left.
	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]
	node.isDirty = true
	if err := t.writeNode(node); err != nil {
		return err
	}
	right.isDirty = true
	if err := t.writeNode(right); err != nil {
		return err
	}

	// Root split?
	if node.pageID == t.root {
		return t.createNewRoot(node.pageID, promoteKey, right.pageID)
	}

	return t.insertIntoParent(node.parent, node.pageID, promoteKey, right.pageID)
}

func (t *BPlusTree) releaseNode(n *Node, dirty bool) {
	if n == nil {
		return
	}
	_ = t.bufferPool.UnpinPage(n.pageID, dirty || n.isDirty)
	n.pincnt = 0
}
