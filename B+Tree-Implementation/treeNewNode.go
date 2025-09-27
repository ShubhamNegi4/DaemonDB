//NewNode creates a new node of given type and returns its pointer
func NewNode(nodeType nodeType) *Node {
	n:= &Node{
		nodeType: nodeType,
		key: {[][]byte, 0, MaxKeys+1},
		numsKeys: 0,
	}
	if nodeType == NodeInternal {
		n.children = {[]int64, 0, MaxKeys+2},
	}else{
		n.vals = {[][]byte, 0, MaxKeys+1},
	}
	return n
}
