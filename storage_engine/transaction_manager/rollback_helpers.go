package txn

import "DaemonDB/types"

/*
Before the transaction gets completed, it is not sure whether it will actually be commited or not (rollbacked or aborted)

the InsertedRows and UpdatedRows slices helps in keeping track of the changes made in case they might be rollbacked

*/

// RecordInsert adds a row to the transaction's InsertedRows list for rollback.
// Called by StorageEngine.InsertRow after the row is written to the heap file.
func (txn *Transaction) RecordInsert(table string, rowPtr types.RowPointer, primaryKey []byte) {
	txn.InsertedRows = append(txn.InsertedRows, InsertedRow{
		Table:      table,
		RowPtr:     rowPtr,
		PrimaryKey: primaryKey,
	})
}

// RecordUpdate saves the old row state before an update for rollback.
func (txn *Transaction) RecordUpdate(table string, oldPtr, newPtr types.RowPointer, oldRowData []byte, primaryKey []byte) {
	txn.UpdatedRows = append(txn.UpdatedRows, UpdatedRow{
		Table:      table,
		OldRowPtr:  oldPtr,
		NewRowPtr:  newPtr,
		OldRowData: oldRowData,
		PrimaryKey: primaryKey,
	})
}
