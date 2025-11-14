package heapfile

// readPage reads a 4KB page from disk at the given page number
func (hf *HeapFile) readPage(pageNum uint32) ([]byte, error) {
	page := make([]byte, PageSize)
	offset := int64(pageNum) * PageSize

	n, err := hf.file.ReadAt(page, offset)
	if err != nil && n == 0 {
		return nil, err
	}

	return page, nil
}

// writePage writes a 4KB page to disk at the given page number
func (hf *HeapFile) writePage(pageNum uint32, page []byte) error {
	offset := int64(pageNum) * PageSize
	_, err := hf.file.WriteAt(page, offset)
	if err != nil {
		return err
	}
	return hf.file.Sync()
}
