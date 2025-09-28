package main

func (t* BPlusTree) FindLeaf(nodeId int64, key[] byte) *Node {
	n:= t.cache.pages[nodeId]
	if n.nodeType == NodeLeaf {
		return n
	}
	i:= 0
	for i< len(n.key) && t.cmp(key, n.key[i]) < 0 {
		i++
	}
	childId := n.children[i]
	return t.FindLeaf(childId,key)
}