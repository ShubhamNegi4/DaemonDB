package catalog

import (
	types "DaemonDB/types"
)

type CatalogManager struct {
	dbRoot        string
	currDb        string
	TableToFileId map[string]TableFileMapping
	nextFileID    uint32
	tableSchemas  map[string]types.TableSchema
}

type TableFileMapping struct {
	HeapFileID  uint32 `json:"heap_file_id"`
	IndexFileID uint32 `json:"index_file_id"`
}
