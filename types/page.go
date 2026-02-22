package types

const (
	PageSize           = 4096 // 4KB page
	HeapPageHeaderSize = 32   // 32 bytes
	SlotSize           = 4    // 4 bytes per slot entry (offset: 2B, length: 2B)
)

type PageType uint8

const (
	PageTypeUnknown PageType = iota
	PageTypeHeapData
	PageTypeBPlusNode
	PageTypeMetadata
)
