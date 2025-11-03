package bplus

func (t *BPlusTree) Delete(key []byte) {
	t.deleteRecursive(t.root, key)
}

func (t *BPlusTree) deleteRecursive(nodeId int64, key []byte) bool {
	node := t.cache.pages[nodeId]
	if node.nodeType == NodeLeaf {
		idx := -1
		for i := 0; i < len(node.key); i++ {
			if t.cmp(node.key[i], key) == 0 {
				idx = i
				break
			}
		}
		if idx == -1 {
			return false
		}
		node.key = append(node.key[:idx], node.key[idx+1:]...)
		node.vals = append(node.vals[:idx], node.vals[idx+1:]...)
		node.numKeys--
		return len(node.key) < (MaxKeys+1)/2
	}
	i := 0
	for i < len(node.key) && t.cmp(node.key[i], key) < 0 {
		i++
	}
	underflow := t.deleteRecursive(node.children[i], key)
	if !underflow {
		return false
	}
	child := t.cache.pages[node.children[i]]
	var left *Node
	var right *Node
	if i > 0 {
		left = t.cache.pages[node.children[i-1]]
	}
	if i < len(node.children)-1 {
		right = t.cache.pages[node.children[i+1]]
	}
	if left != nil && len(left.key) > (MaxKeys+1)/2 {
		child.key = append([][]byte{left.key[len(left.key)-1]}, child.key...)
		if child.nodeType == NodeLeaf {
			child.vals = append([][]byte{left.vals[len(left.vals)-1]}, child.vals...)
			left.vals = left.vals[:len(left.vals)-1]
		} else {
			child.children = append([]int64{left.children[len(left.children)-1]}, child.children...)
			left.children = left.children[:len(left.children)-1]
		}
		left.key = left.key[:len(left.key)-1]
		node.key[i-1] = child.key[0]
		return false
	}
	if right != nil && len(right.key) > (MaxKeys+1)/2 {
		child.key = append(child.key, right.key[0])
		if child.nodeType == NodeLeaf {
			child.vals = append(child.vals, right.vals[0])
			right.vals = right.vals[1:]
		} else {
			child.children = append(child.children, right.children[0])
			right.children = right.children[1:]
		}
		right.key = right.key[1:]
		node.key[i] = right.key[0]
		return false
	}
	if left != nil {
		left.key = append(left.key, child.key...)
		if child.nodeType == NodeLeaf {
			left.vals = append(left.vals, child.vals...)
		} else {
			left.children = append(left.children, child.children...)
		}
		node.key = append(node.key[:i-1], node.key[i:]...)
		node.children = append(node.children[:i], node.children[i+1:]...)
	} else if right != nil {
		child.key = append(child.key, right.key...)
		if child.nodeType == NodeLeaf {
			child.vals = append(child.vals, right.vals...)
		} else {
			child.children = append(child.children, right.children...)
		}
		node.key = append(node.key[:i], node.key[i+1:]...)
		node.children = append(node.children[:i+1], node.children[i+2:]...)
	}
	node.numKeys = int16(len(node.key))
	if nodeId == t.root && len(node.key) == 0 && len(node.children) > 0 {
		t.root = node.children[0]
	}
	return len(node.key) < (MaxKeys+1)/2
}
