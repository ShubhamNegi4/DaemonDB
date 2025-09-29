package main


func (t *BPlusTree) FindLeaf(nodeId int64, key []byte) *Node {
	if nodeId == 0 || t == nil || t.cache == nil || t.cache.pages == nil {
		return nil
	}
	n := t.cache.pages[nodeId]
	if n == nil {
		return nil
	}
	if n.nodeType == NodeLeaf {
		return n
	}
	i := lowerBound(n.key, key, t.cmp)
	// safe child index: internal nodes must have len(children) == len(keys)+1
	if i < 0 {
		i = 0
	}
	if i >= len(n.children) {
		if len(n.children) == 0 {
			return nil
		}
		i = len(n.children) - 1
	}
	childId := n.children[i]
	return t.FindLeaf(childId, key)
}
