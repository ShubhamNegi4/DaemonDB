package heapfile

import (
	page "DaemonDB/storage_engine/page"
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// Header accessors
// ─────────────────────────────────────────────────────────────────────────────

func GetFileID(pg *page.Page) uint32 {
	return binary.LittleEndian.Uint32(pg.Data[heapOffFileID:])
}

func GetPageNo(pg *page.Page) uint32 {
	return binary.LittleEndian.Uint32(pg.Data[heapOffPageNo:])
}
func SetPageNo(pg *page.Page, n uint32) {
	binary.LittleEndian.PutUint32(pg.Data[heapOffPageNo:], n)
	pg.IsDirty = true
}

// RecordEndPtr is the first free byte after the last written record.
// New records are written starting at this offset, then it advances forward.
func GetRecordEndPtr(pg *page.Page) uint16 {
	return binary.LittleEndian.Uint16(pg.Data[heapOffRecordEndPtr:])
}
func setRecordEndPtr(pg *page.Page, v uint16) {
	binary.LittleEndian.PutUint16(pg.Data[heapOffRecordEndPtr:], v)
}

// SlotRegionStart is the byte offset of the first (highest-index) slot entry.
// The slot directory grows backward from PageSize; this pointer moves left
// each time a new slot is appended.
func GetSlotRegionStart(pg *page.Page) uint16 {
	return binary.LittleEndian.Uint16(pg.Data[heapOffSlotRegionStart:])
}
func setSlotRegionStart(pg *page.Page, v uint16) {
	binary.LittleEndian.PutUint16(pg.Data[heapOffSlotRegionStart:], v)
}

func GetNumRows(pg *page.Page) uint16 {
	return binary.LittleEndian.Uint16(pg.Data[heapOffNumRows:])
}
func setNumRows(pg *page.Page, n uint16) {
	binary.LittleEndian.PutUint16(pg.Data[heapOffNumRows:], n)
}

func GetNumRowsFree(pg *page.Page) uint16 {
	return binary.LittleEndian.Uint16(pg.Data[heapOffNumRowsFree:])
}
func setNumRowsFree(pg *page.Page, n uint16) {
	binary.LittleEndian.PutUint16(pg.Data[heapOffNumRowsFree:], n)
}

func GetIsPageFull(pg *page.Page) bool {
	return binary.LittleEndian.Uint16(pg.Data[heapOffIsPageFull:]) == 1
}
func setIsPageFull(pg *page.Page, full bool) {
	v := uint16(0)
	if full {
		v = 1
	}
	binary.LittleEndian.PutUint16(pg.Data[heapOffIsPageFull:], v)
}

func GetSlotCount(pg *page.Page) uint16 {
	return binary.LittleEndian.Uint16(pg.Data[heapOffSlotCount:])
}
func setSlotCount(pg *page.Page, n uint16) {
	binary.LittleEndian.PutUint16(pg.Data[heapOffSlotCount:], n)
}

func GetLastAppliedLSN(pg *page.Page) uint64 {
	return binary.LittleEndian.Uint64(pg.Data[heapOffLSN:])
}

func SetLastAppliedLSN(pg *page.Page, lsn uint64) {
	binary.LittleEndian.PutUint64(pg.Data[heapOffLSN:], lsn)
	pg.LSN = lsn
	pg.IsDirty = true
}

// ─────────────────────────────────────────────────────────────────────────────
// Free space
// ─────────────────────────────────────────────────────────────────────────────

// FreeSpace returns the bytes available for a new record including the slot
// entry it would consume.
//
//	available = SlotRegionStart - RecordEndPtr - SlotSize
//
// SlotRegionStart moves left as slots are added.
// RecordEndPtr    moves right as records are added.
// When they meet, the page is full.
func FreeSpace(pg *page.Page) int {
	available := int(GetSlotRegionStart(pg)) - int(GetRecordEndPtr(pg)) - SlotSize
	if available < 0 {
		return 0
	}
	return available
}

// ─────────────────────────────────────────────────────────────────────────────
// Slot directory
// ─────────────────────────────────────────────────────────────────────────────

// slotByteOffset returns the byte offset in Data where slot i begins.
// Slot 0 is at the highest address (PageSize - SlotSize),
// slot 1 is just below it, and so on.
//
//	slot 0: bytes 4092–4095
//	slot 1: bytes 4088–4091
//	slot i: PageSize - (i+1)*SlotSize
func slotByteOffset(i uint16) int {
	return page.PageSize - (int(i)+1)*SlotSize
}

func readSlot(pg *page.Page, i uint16) (offset, length uint16) {
	base := slotByteOffset(i)
	return binary.LittleEndian.Uint16(pg.Data[base:]),
		binary.LittleEndian.Uint16(pg.Data[base+2:])
}

func writeSlot(pg *page.Page, i uint16, offset, length uint16) {
	base := slotByteOffset(i)
	binary.LittleEndian.PutUint16(pg.Data[base:], offset)
	binary.LittleEndian.PutUint16(pg.Data[base+2:], length)
}

func IsSlotLive(pg *page.Page, i uint16) bool {
	if i >= GetSlotCount(pg) {
		return false
	}
	offset, length := readSlot(pg, i)
	return offset != 0 && length != 0
}
