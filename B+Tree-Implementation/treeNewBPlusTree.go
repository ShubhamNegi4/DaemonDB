package main
func NewBPlusTree(p Pager, bp *BufferPool, cmp func(a, b[]byte) int) *BPlusTree {
	return &BPlusTree{
		root:0,
		pager: p,
		cache: bp,
		cmp: cmp,
	}
}