package heapfile

import (
	page "DaemonDB/storage_engine/page"
	"encoding/binary"
	"fmt"
)

/*
/*
This file contains standalone functions operating on *page.Page for heap file operations.
All functions take *page.Page as first argument since methods cannot be defined on
types from external packages.

Heap page binary layout (all values little-endian):

	Offset  Size  Field
	──────────────────────────────────────────────────────
	0       8     LastAppliedLSN  uint64  — WAL LSN, first in every page type
	8       1     PageType        uint8   — stamped by DiskManager on write
	9       4     FileID          uint32
	13      4     PageNo          uint32
	17      2     RecordEndPtr    uint16  — first free byte after last record
	19      2     SlotRegionStart uint16  — first byte of slot directory
	21      2     NumRows         uint16  — live records
	23      2     NumRowsFree     uint16  — tombstone slots
	25      2     IsPageFull      uint16  — 1 when no usable space remains
	27      2     SlotCount       uint16  — total slot entries (live + tombstone)
	──────────────────────────────────────────────────────
	29            HeapHeaderSize

LSN is always the first 8 bytes of every page type (heap and index) so the
BufferPool can read pg.LSN without knowing the page layout. This is the
agreed convention across all page types.

Standard slotted-page layout:

	[ header 29B ][ records → ][ free space ][ ← slot dir ]
	0            29            ^             ^             4096
	                           RecordEndPtr  SlotRegionStart

	Records grow FORWARD  from HeapHeaderSize.
	Slot directory grows BACKWARD from PageSize.
	Free space is the gap between RecordEndPtr and SlotRegionStart.

A slot entry is 4 bytes: [ Offset uint16 ][ Length uint16 ]

	Offset  — absolute byte offset from start of page to the record data.
	Length  — byte length of the record (0 = tombstone / deleted).

Slot i lives at:  PageSize - (i+1)*SlotSize
This means slot 0 is at bytes 4092-4095, slot 1 at 4088-4091, etc.
*/
const (
	heapOffLSN             = 0  // uint64 (8) ← LSN first
	heapOffPageType        = 8  // uint8  (1)
	heapOffFileID          = 9  // uint32 (4)
	heapOffPageNo          = 13 // uint32 (4)
	heapOffRecordEndPtr    = 17 // uint16 (2)
	heapOffSlotRegionStart = 19 // uint16 (2)
	heapOffNumRows         = 21 // uint16 (2)
	heapOffNumRowsFree     = 23 // uint16 (2)
	heapOffIsPageFull      = 25 // uint16 (2)
	heapOffSlotCount       = 27 // uint16 (2)

	// HeapHeaderSize is the fixed header size in bytes.
	// Records start at this offset on a fresh page.
	HeapHeaderSize = 29

	// SlotSize is the byte size of one slot entry: Offset(2) + Length(2).
	SlotSize = 4
)

// ─────────────────────────────────────────────────────────────────────────────
// Initialisation
// ─────────────────────────────────────────────────────────────────────────────

// InitHeapPage stamps a fresh heap-page header into pg.Data.
//
// After this call:
//   - LastAppliedLSN  == 0              (offset 0, shared convention with index pages)
//   - RecordEndPtr    == HeapHeaderSize (records start right after header)
//   - SlotRegionStart == PageSize       (slot dir starts at end of page, empty)
//   - NumRows         == 0
//   - NumRowsFree     == 0
//   - IsPageFull      == 0
//   - SlotCount       == 0
//   - pg.LSN          == 0              (in-memory field kept in sync with Data)
//   - All non-header bytes zeroed

func InitHeapPage(pg *page.Page) {
	// Zero everything after the page-type byte so recycled frames are clean.
	for i := 1; i < page.PageSize; i++ {
		pg.Data[i] = 0
	}

	binary.LittleEndian.PutUint64(pg.Data[heapOffLSN:], 0) // LSN = 0
	binary.LittleEndian.PutUint32(pg.Data[heapOffFileID:], pg.FileID)
	binary.LittleEndian.PutUint32(pg.Data[heapOffPageNo:], 0)
	binary.LittleEndian.PutUint16(pg.Data[heapOffRecordEndPtr:], HeapHeaderSize)
	binary.LittleEndian.PutUint16(pg.Data[heapOffSlotRegionStart:], page.PageSize)
	binary.LittleEndian.PutUint16(pg.Data[heapOffNumRows:], 0)
	binary.LittleEndian.PutUint16(pg.Data[heapOffNumRowsFree:], 0)
	binary.LittleEndian.PutUint16(pg.Data[heapOffIsPageFull:], 0)
	binary.LittleEndian.PutUint16(pg.Data[heapOffSlotCount:], 0)

	pg.LSN = 0
	pg.IsDirty = true
}

// ─────────────────────────────────────────────────────────────────────────────
// Record operations
// ─────────────────────────────────────────────────────────────────────────────

// InsertRecord writes data into the page and returns the slot index.
// The slot index is the local part of a RowPointer (PageID + SlotIdx).
// Returns an error if there is insufficient space — caller must get a new page.
func InsertRecord(pg *page.Page, data []byte) (slotIdx uint16, err error) {
	recordLen := uint16(len(data))
	if recordLen == 0 {
		return 0, fmt.Errorf("InsertRecord: data must not be empty")
	}
	if FreeSpace(pg) < int(recordLen) {
		return 0, fmt.Errorf("InsertRecord: need %d bytes, only %d available",
			recordLen, FreeSpace(pg))
	}

	// Reuse a tombstone slot if one exists — avoids shrinking SlotRegionStart.
	slotIdx = GetSlotCount(pg) // default: new slot
	for i := uint16(0); i < GetSlotCount(pg); i++ {
		if _, l := readSlot(pg, i); l == 0 {
			slotIdx = i
			break
		}
	}

	// Write record data at RecordEndPtr and advance it forward.
	recordOffset := GetRecordEndPtr(pg)
	copy(pg.Data[recordOffset:], data)
	setRecordEndPtr(pg, recordOffset+recordLen)

	// Write the slot entry pointing at the record.
	writeSlot(pg, slotIdx, recordOffset, recordLen)

	// Update header counts.
	if slotIdx == GetSlotCount(pg) {
		// New slot — grow slot directory backward.
		newSlotRegionStart := GetSlotRegionStart(pg) - SlotSize
		setSlotRegionStart(pg, newSlotRegionStart)
		setSlotCount(pg, GetSlotCount(pg)+1)
	} else {
		// Recycled tombstone — one fewer free slot.
		setNumRowsFree(pg, GetNumRowsFree(pg)-1)
	}
	setNumRows(pg, GetNumRows(pg)+1)

	if FreeSpace(pg) <= 0 {
		setIsPageFull(pg, true)
	}

	pg.IsDirty = true
	return slotIdx, nil
}

// GetRecord returns a copy of the record at slotIdx.
func GetRecord(pg *page.Page, slotIdx uint16) ([]byte, error) {
	if slotIdx >= GetSlotCount(pg) {
		return nil, fmt.Errorf("GetRecord: slot %d out of range (count=%d)",
			slotIdx, GetSlotCount(pg))
	}
	offset, length := readSlot(pg, slotIdx)
	if length == 0 {
		return nil, fmt.Errorf("GetRecord: slot %d is a tombstone", slotIdx)
	}
	out := make([]byte, length)
	copy(out, pg.Data[offset:offset+length])
	return out, nil
}

// DeleteRecord marks slotIdx as a tombstone.
// Space used by the record is NOT reclaimed until a compaction pass.
// The slot entry remains so existing RowPointers stay valid.
func DeleteRecord(pg *page.Page, slotIdx uint16) error {
	if slotIdx >= GetSlotCount(pg) {
		return fmt.Errorf("DeleteRecord: slot %d out of range (count=%d)",
			slotIdx, GetSlotCount(pg))
	}
	if _, length := readSlot(pg, slotIdx); length == 0 {
		return fmt.Errorf("DeleteRecord: slot %d already deleted", slotIdx)
	}
	writeSlot(pg, slotIdx, 0, 0) // tombstone: offset=0, length=0
	setNumRows(pg, GetNumRows(pg)-1)
	setNumRowsFree(pg, GetNumRowsFree(pg)+1)
	setIsPageFull(pg, false) // space is theoretically reclaimable
	pg.IsDirty = true
	return nil
}

// UpdateRecord replaces the record at slotIdx with newData in place.
// Returns true  — updated in place (newData fits within original allocation).
// Returns false — original record tombstoned; caller must re-insert on a page
//
//	with enough FreeSpace() for the larger record.
func UpdateRecord(pg *page.Page, slotIdx uint16, newData []byte) (bool, error) {
	if slotIdx >= GetSlotCount(pg) {
		return false, fmt.Errorf("UpdateRecord: slot %d out of range (count=%d)",
			slotIdx, GetSlotCount(pg))
	}
	offset, length := readSlot(pg, slotIdx)
	if length == 0 {
		return false, fmt.Errorf("UpdateRecord: slot %d is a tombstone", slotIdx)
	}

	newLen := uint16(len(newData))
	if newLen <= length {
		// Fits within the original allocation — overwrite in place.
		copy(pg.Data[offset:], newData)
		writeSlot(pg, slotIdx, offset, newLen)
		pg.IsDirty = true
		return true, nil
	}

	// Does not fit — tombstone and tell caller to re-insert elsewhere.
	if err := DeleteRecord(pg, slotIdx); err != nil {
		return false, err
	}
	return false, nil
}

func InsertRecordAtSlot(pg *page.Page, slotIdx uint16, data []byte) error {
	recordLen := uint16(len(data))

	// If slot already has data at this index, it's already recovered — idempotent
	if slotIdx < GetSlotCount(pg) {
		offset, length := readSlot(pg, slotIdx)
		if length > 0 && offset > 0 {
			return nil // slot already occupied, skip
		}
	}

	if FreeSpace(pg) < int(recordLen) {
		return fmt.Errorf("insufficient space on page for recovery insert")
	}

	recordOffset := GetRecordEndPtr(pg)
	copy(pg.Data[recordOffset:], data)
	setRecordEndPtr(pg, recordOffset+recordLen)
	writeSlot(pg, slotIdx, recordOffset, recordLen)

	// Extend slot directory if needed
	if slotIdx >= GetSlotCount(pg) {
		setSlotCount(pg, slotIdx+1)
		setSlotRegionStart(pg, GetSlotRegionStart(pg)-SlotSize)
	}

	setNumRows(pg, GetNumRows(pg)+1)
	pg.IsDirty = true
	return nil
}
