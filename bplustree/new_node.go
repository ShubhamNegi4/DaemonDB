package bplus

// NewNode creates a new node of given type and returns its pointer

func NewNode(nodeType NodeType) *Node {
	n := &Node{
		nodeType: nodeType,
		key:      make([][]byte, 0, MaxKeys+1),
		numKeys:  0,
	}
	if nodeType == NodeInternal {
		n.children = make([]int64, 0, MaxKeys+2)
	} else {
		n.vals = make([][]byte, 0, MaxKeys+1)
	}
	return n
}
