package main

func (t* BPlusTree) SplitLeaf(leaf 	*Node) {
	mid := leaf.numKeys/2
	newLeaf := NewNode(NodeLeaf)
    newLeaf.id, _ = t.pager.AllocatePage()
	newLeaf.key = append(newLeaf.key, leaf.key[mid:]...)
	newLeaf.vals = append(newLeaf.vals, leaf.vals[mid:]...)
	newLeaf.numKeys = int16(len(newLeaf.key))


	leaf.key = leaf.key[:mid]
	leaf.vals = leaf.vals[:mid]
	leaf.numKeys = int16(len(leaf.key))



	newLeaf.next = leaf.next
	leaf.next = newLeaf.id


	if leaf.id == t.root {
        newRoot := NewNode(NodeInternal)
        newRoot.key = append(newRoot.key, newLeaf.key[0])
        newRoot.children = append(newRoot.children, leaf.id, newLeaf.id)
        newRoot.numKeys = 1

        newRoot.id,_ = t.pager.AllocatePage()
        t.root = newRoot.id
        t.cache.pages[newRoot.id] = newRoot
    } else {
        // TODO: insert promoted key into parent (recursive case)
    }

    // Add new leaf to cache
    t.cache.pages[newLeaf.id] = newLeaf

}