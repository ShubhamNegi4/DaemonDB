package storageengine

import (
	"encoding/binary"

	bplus "DaemonDB/storage_engine/access/indexfile_manager/bplustree"
	"DaemonDB/types"
)

/*
This file contains function related to index in index files
generating primary key from fileID and page number when it is not mentioned explicitly
*/

func (se *StorageEngine) getIndex(tableName string) (*bplus.BPlusTree, error) {
	indexFileID, err := se.CatalogManager.GetIndexFileID(tableName)
	if err != nil {
		return nil, err
	}
	return se.IndexManager.GetOrCreateIndex(tableName, indexFileID)
}

func (se *StorageEngine) ExtractPrimaryKey(schema types.TableSchema, values []any, rowPtr *types.RowPointer) ([]byte, string, error) {
	for i, col := range schema.Columns {
		if col.IsPrimaryKey {
			keyBytes, err := ValueToBytes(values[i], col.Type)
			if err != nil {
				return nil, "", err
			}
			return keyBytes, col.Name, nil
		}
	}

	return se.GenerateImplicitKey(rowPtr), "__rowid__", nil
}

func (se *StorageEngine) GenerateImplicitKey(rowPtr *types.RowPointer) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], rowPtr.FileID)
	binary.BigEndian.PutUint32(buf[4:8], rowPtr.PageNumber)
	return buf
}
