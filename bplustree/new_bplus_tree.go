package bplus

import "encoding/binary"

func NewBPlusTree(p Pager, bp *BufferPool, cmp func(a, b []byte) int) *BPlusTree {
	// Set the pager on the buffer pool so it can load nodes from disk
	bp.SetPager(p)

	t := &BPlusTree{
		root:  0,
		pager: p,
		cache: bp,
		cmp:   cmp,
	}

	// Try to load persisted root from page 0 metadata.
	meta, err := p.ReadPage(0)
	if err == nil && len(meta) >= 8 {
		t.root = int64(binary.LittleEndian.Uint64(meta[0:8]))
	}

	return t
}

// persist current root to metadata page 0
func (t *BPlusTree) saveRoot() {
	buf := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t.root))
	_ = t.pager.WritePage(0, buf)
}

// Close flushes dirty nodes to disk and closes the pager (releases the file handle).
// Call this when switching database or on VM shutdown to avoid leaking file descriptors.
func (t *BPlusTree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_ = t.cache.Flush()
	return t.pager.Close()
}
