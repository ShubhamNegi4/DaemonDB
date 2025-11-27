package heapfile

import (
	"encoding/binary"
)

// writePageHeader serializes the page header to the first 32 bytes of the page
func writePageHeader(page []byte, header *PageHeader) {
	binary.LittleEndian.PutUint32(page[0:4], header.FileID)
	binary.LittleEndian.PutUint32(page[4:8], header.PageNo)
	binary.LittleEndian.PutUint16(page[8:10], header.FreePtr)
	binary.LittleEndian.PutUint16(page[10:12], header.NumRows)
	binary.LittleEndian.PutUint16(page[12:14], header.NumRowsFree)
	binary.LittleEndian.PutUint16(page[14:16], header.IsPageFull)
	binary.LittleEndian.PutUint16(page[16:18], header.SlotCount)
	// bytes 18-31 are reserved for future use
}

// readPageHeader deserializes the page header from the first 32 bytes of the page
func readPageHeader(page []byte) *PageHeader {
	return &PageHeader{
		FileID:      binary.LittleEndian.Uint32(page[0:4]),
		PageNo:      binary.LittleEndian.Uint32(page[4:8]),
		FreePtr:     binary.LittleEndian.Uint16(page[8:10]),
		NumRows:     binary.LittleEndian.Uint16(page[10:12]),
		NumRowsFree: binary.LittleEndian.Uint16(page[12:14]),
		IsPageFull:  binary.LittleEndian.Uint16(page[14:16]),
		SlotCount:   binary.LittleEndian.Uint16(page[16:18]),
	}
}
