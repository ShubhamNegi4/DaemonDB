package bplus

// insertIntoParent inserts sepKey and rightId into the parent of leftId.
// If the parent overflows, it splits and propagates upward.
func (t *BPlusTree) insertIntoParent(parentId int64, leftId int64, sepKey []byte, rightId int64) {
	parent := t.cache.pages[parentId]
	if parent == nil {
		return
	}

	// find index of leftId in parent's children
	idx := 0
	for idx < len(parent.children) && parent.children[idx] != leftId {
		idx++
	}
	if idx > len(parent.children) {
		idx = len(parent.children)
	}

	// insert sepKey at idx, and rightId after idx
	// parent.keys: ..., key[idx], ... ; parent.children: ..., leftId(idx), ...
	// after insert => keys: insert at idx, children: insert rightId at idx+1
	parent.key = append(parent.key[:idx], append([][]byte{sepKey}, parent.key[idx:]...)...)
	if idx+1 <= len(parent.children) {
		parent.children = append(parent.children[:idx+1], append([]int64{rightId}, parent.children[idx+1:]...)...)
	} else {
		parent.children = append(parent.children, rightId)
	}
	parent.numKeys = int16(len(parent.key))

	// set right child's parent
	if rc := t.cache.pages[rightId]; rc != nil {
		rc.parent = parent.id
	}

	// if overflow, split internal and propagate
	if parent.numKeys > MaxKeys {
		t.SplitInternal(parent)
	}

	// update cache
	t.cache.pages[parent.id] = parent
}
