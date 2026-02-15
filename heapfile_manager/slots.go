package heapfile

import (
	"encoding/binary"
)

// getSlotDirectoryOffset returns the offset where the slot directory starts
// Slot directory grows backward from the end of the page
// func getSlotDirectoryOffset(header *PageHeader) uint16 {
// 	return PageSize - (header.SlotCount * SlotSize)
// }

// readSlot reads a slot entry from the slot directory
// Slots are stored backward from the end: slot 0 is at PageSize-SlotSize, slot 1 at PageSize-2*SlotSize, etc.
func readSlot(page []byte, slotIndex uint16) *Slot {
	header := readPageHeader(page)
	if slotIndex >= header.SlotCount {
		return nil // Invalid slot index
	}
	// Slot directory grows backward: slot 0 is at end, slot 1 is before it, etc.
	slotOffset := PageSize - ((slotIndex + 1) * SlotSize)

	return &Slot{
		Offset: binary.LittleEndian.Uint16(page[slotOffset : slotOffset+2]),
		Length: binary.LittleEndian.Uint16(page[slotOffset+2 : slotOffset+4]),
	}
}

// writeSlot writes a slot entry to the slot directory
func writeSlot(page []byte, slotIndex uint16, slot *Slot) {
	// Slot directory grows backward: slot 0 is at end, slot 1 is before it, etc.
	slotOffset := PageSize - ((slotIndex + 1) * SlotSize)

	binary.LittleEndian.PutUint16(page[slotOffset:slotOffset+2], slot.Offset)
	binary.LittleEndian.PutUint16(page[slotOffset+2:slotOffset+4], slot.Length)
}

// addSlot adds a new slot entry and returns its index
func addSlot(page []byte, rowOffset uint16, rowLength uint16) uint16 {
	header := readPageHeader(page)
	slotIndex := header.SlotCount

	// Calculate new slot directory offset
	newSlotDirOffset := PageSize - ((header.SlotCount + 1) * SlotSize)

	// Write slot entry at the new position
	binary.LittleEndian.PutUint16(page[newSlotDirOffset:newSlotDirOffset+2], rowOffset)
	binary.LittleEndian.PutUint16(page[newSlotDirOffset+2:newSlotDirOffset+4], rowLength)

	// Update header
	header.SlotCount++
	writePageHeader(page, header)

	return slotIndex
}

// getRowData retrieves row data using a slot entry
func getRowData(page []byte, slot *Slot) []byte {
	if slot.Offset == 0 || slot.Length == 0 {
		return nil
	}
	return page[slot.Offset : slot.Offset+slot.Length]
}

// calculateFreeSpace calculates available space in a page considering slot directory
func calculateFreeSpace(header *PageHeader) uint16 {
	slotDirSize := header.SlotCount * SlotSize
	usedSpace := header.FreePtr - PageHeaderSize
	availableSpace := PageSize - PageHeaderSize - slotDirSize - usedSpace
	return availableSpace
}
