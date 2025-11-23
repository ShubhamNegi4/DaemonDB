package bplus

func NewBPlusTree(p Pager, bp *BufferPool, cmp func(a, b []byte) int) *BPlusTree {
	// Set the pager on the buffer pool so it can load nodes from disk
	bp.SetPager(p)
	return &BPlusTree{
		root:  0,
		pager: p,
		cache: bp,
		cmp:   cmp,
	}
}
