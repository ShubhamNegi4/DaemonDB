package bplus

func (t *BPlusTree) FindLeaf(nodeId int64, key []byte) *Node {
	for {
		if nodeId == 0 || t == nil || t.cache == nil {
			return nil
		}
		n, err := t.cache.Get(nodeId)
		if err != nil || n == nil {
			return nil
		}
		_ = t.cache.Pin(n.id)
		if n.nodeType == NodeLeaf {
			return n // caller must Unpin when done
		}
		i := lowerBound(n.key, key, t.cmp)
		if i < 0 {
			i = 0
		}
		if i >= len(n.children) {
			if len(n.children) == 0 {
				_ = t.cache.Unpin(n.id)
				return nil
			}
			i = len(n.children) - 1
		}
		nextId := n.children[i]
		_ = t.cache.Unpin(n.id)
		nodeId = nextId
	}
}
