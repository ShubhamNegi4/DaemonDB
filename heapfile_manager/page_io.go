package heapfile

// readPage reads a 4KB page from disk at the given page number using the pager
func (hf *HeapFile) readPage(pageNum uint32) ([]byte, error) {
	return hf.pager.ReadPage(int64(pageNum))
}

// writePage writes a 4KB page to disk at the given page number using the pager
func (hf *HeapFile) writePage(pageNum uint32, page []byte) error {
	return hf.pager.WritePage(int64(pageNum), page)
}
