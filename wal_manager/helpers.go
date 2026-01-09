package wal_manager

import (
	"encoding/binary"
	"hash/crc32"
)

func (r *WALRecord) Encode() []byte {
	totalSize := RecordHeaderSize + len(r.Data)
	buf := make([]byte, totalSize)

	binary.BigEndian.PutUint64(buf[0:8], r.LSN)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(r.Data)))
	binary.BigEndian.PutUint32(buf[12:16], r.CRC)
	copy(buf[16:], r.Data)

	return buf
}

func (r *WALRecord) ValidateCRC() bool {
	computedCRC := calculateCRC(r.LSN, r.Data)
	return computedCRC == r.CRC
}

// calculateCRC computes CRC32 checksum over LSN and data
func calculateCRC(lsn uint64, data []byte) uint32 {
	hasher := crc32.NewIEEE()

	// LSN in CRC calculation
	lsnBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lsnBytes, lsn)
	hasher.Write(lsnBytes)

	// data in CRC calculation
	hasher.Write(data)

	return hasher.Sum32()
}
