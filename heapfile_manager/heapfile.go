package heapfile

/*
This file contains the pager for the heap file
It has implementation of functions that deals with accessing page headers, writing and reading form heap files,


	B+ Tree leaf node:
	┌───────────────────────────────────┐
	| key | RowPointer(FileID, PageNo)  |
	└───────────────────────────────────┘
					│
					▼
			Heapfile (data pages)
*/

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// ############################################# ---- PAGE ----- #############################################
const (
	pageSize       = 4096 // 4KB page
	pageHeaderSize = 32   // 32 bytes
)

// this is for a single 4KB page
type pageHeader struct {
	fileID      uint32 // fileID which heap file this page belongs to
	pageNo      uint32 // current page number inside the heap file
	freePtr     uint16 // ptr to the next free location, where insertion can be done
	numRows     uint16 // number of rows/slots it can accomodate
	numRowsFree uint16 // free rows/slots inside the current page
	isPageFull  uint16 // is the page full
}

// to be stored at the bottom of the page, which will tell the exact offset of the row in the current page
// // to search for a row, we need in which heap file it is stored in,
// in which pageNumber, and to retreive it quickly get the slot (and get the offset from start of the page instantly)
// TODO: later
// type Slot struct {
// 	Offset uint16 // Offset from start of page to row data
// 	Length uint16 // Length of the row data
// }

type RowPointer struct {
	FileID     uint32 `json:"file_id"`
	PageNumber uint32 `json:"page_number"`
}

// ############################################# ---- HEAP FILE ----- #############################################

type HeapFile struct {
	fileID   uint32 // which file it is
	file     *os.File
	filePath string
	mu       sync.RWMutex
}

type HeapFileManager struct {
	baseDir string
	files   map[uint32]*HeapFile
	mu      sync.RWMutex
}

func NewHeapFileManager(baseDir string) (*HeapFileManager, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &HeapFileManager{baseDir: baseDir, files: make(map[uint32]*HeapFile)}, nil
}

// creating a fresh heap file, for a new table or when a heap file is filled
func (hfm *HeapFileManager) CreateHeapfile(tableName string, fileID uint32) error {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()

	filePath := fmt.Sprintf("%s/%s_%d.heap", hfm.baseDir, tableName, fileID)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create heap file: %w", err)
	}

	heapFile := &HeapFile{
		fileID:   fileID,
		file:     file,
		filePath: filePath,
	}

	// Initialize first page
	if err := heapFile.initializePage(0); err != nil {
		file.Close()
		return err
	}

	hfm.files[fileID] = heapFile
	return nil
}

func (hf *HeapFile) initializePage(pageNo uint32) error {
	page := make([]byte, pageSize)

	// Write header
	header := pageHeader{
		fileID:      hf.fileID,
		pageNo:      pageNo,
		freePtr:     pageHeaderSize,
		numRows:     0,
		numRowsFree: pageSize - pageHeaderSize,
		isPageFull:  0,
	}

	hf.writePageHeader(page, &header)

	// Write page to disk
	offset := int64(pageNo) * pageSize // within the heap file, find the correct page offset
	_, err := hf.file.WriteAt(page, offset)
	return err
}

// inserting row in heap file
func (hfm *HeapFileManager) InsertRow(fileID uint32, rowData []byte) (*RowPointer, error) {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()
	hfm.mu.RLock()
	heapFile, exists := hfm.files[fileID]
	hfm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("heap file %d not found", fileID)
	}

	return heapFile.insertRow(rowData)
}

// ########################################## --- HEAP FILE METHODS --- ##########################################

func (hf *HeapFile) insertRow(rowData []byte) (*RowPointer, error) {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	rowLen := uint16(len(rowData))
	if rowLen > pageSize-pageHeaderSize-4 { // 4 bytes for slot entry
		// TODO: implement extended payload heap file
		return nil, fmt.Errorf("row too large: %d bytes", rowLen)
	}

	// find a page with enough capacity to hold
	pageNum, err := hf.findSuitablePage(rowLen)
	if err != nil {
		return nil, err
	}

	// read the page, that has some capacity to store the current row
	page, err := hf.readPage(pageNum)
	if err != nil {
		return nil, err
	}

	// reads its header
	header := hf.readPageHeader(page)

	// Check if there is enough space
	requiredSpace := rowLen + 4 // row data + slot entry
	if header.freePtr < requiredSpace {
		// Page is full, try next page
		return hf.insertRow(rowData) // Recursive call will find/create new page
	}

	// write on the file
	copy(page[header.freePtr:header.freePtr+rowLen], rowData)

	header.freePtr += rowLen // move freeSpacePointer of the file
	header.freePtr -= requiredSpace
	hf.writePageHeader(page, header)

	// write page back to disk
	// TODO: this could be done in batches
	if err := hf.writePage(pageNum, page); err != nil {
		return nil, err
	}

	return &RowPointer{
		FileID:     hf.fileID,
		PageNumber: pageNum,
	}, nil
}

func (hf *HeapFile) findSuitablePage(requiredSpace uint16) (uint32, error) {
	// Get file size
	stat, err := hf.file.Stat()
	if err != nil {
		return 0, err
	}

	numPages := uint32(stat.Size() / pageSize)
	for pageNum := uint32(0); pageNum < numPages; pageNum++ {
		page, err := hf.readPage(pageNum)
		if err != nil {
			continue
		}

		header := hf.readPageHeader(page)
		if header.freePtr >= requiredSpace+4 {
			return pageNum, nil
		}
	}

	// No page found, create new one
	newPageNum := numPages
	if err := hf.initializePage(newPageNum); err != nil {
		return 0, err
	}

	return newPageNum, nil
}

func (hf *HeapFile) readPage(pageNum uint32) ([]byte, error) {
	page := make([]byte, pageSize)
	offset := int64(pageNum) * pageSize

	n, err := hf.file.ReadAt(page, offset)
	if err != nil && n == 0 {
		return nil, err
	}

	return page, nil
}

// ######################################## --- LOW LEVEL READ/WRITES --- ########################################

func (hf *HeapFile) writePage(pageNum uint32, page []byte) error {
	offset := int64(pageNum) * pageSize
	_, err := hf.file.WriteAt(page, offset)
	if err != nil {
		return err
	}
	return hf.file.Sync()
}

func (hf *HeapFile) writePageHeader(page []byte, header *pageHeader) {
	binary.LittleEndian.PutUint32(page[0:4], uint32(header.fileID))
	binary.LittleEndian.PutUint32(page[4:8], uint32(header.pageNo))
	binary.LittleEndian.PutUint16(page[8:10], uint16(header.freePtr))
	binary.LittleEndian.PutUint16(page[10:12], uint16(header.numRows))
	binary.LittleEndian.PutUint16(page[12:14], uint16(header.freePtr))
	binary.LittleEndian.PutUint16(page[14:16], header.isPageFull)
}

func (hf *HeapFile) readPageHeader(page []byte) *pageHeader {
	return &pageHeader{
		fileID:      binary.LittleEndian.Uint32(page[0:4]),
		pageNo:      binary.LittleEndian.Uint32(page[4:8]),
		freePtr:     binary.LittleEndian.Uint16(page[8:10]),
		numRows:     binary.LittleEndian.Uint16(page[10:12]),
		numRowsFree: binary.LittleEndian.Uint16(page[12:14]),
		isPageFull:  binary.LittleEndian.Uint16(page[14:16]),
	}
}
