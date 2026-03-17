# DaemonDB StorageEngine Database Commands

This package provides core database commands for **DaemonDB**, including creating databases, switching between them, and listing existing databases.  

---

The database command layer provides basic operations to manage and interact with databases. 
CREATE DATABASE initializes a new database by creating its folder structure, including directories for tables and WAL logs.   

SHOW DATABASES lists all existing databases in the system.  

USE DATABASE switches the current database context, safely closing any previously open database, and initializes the database environment by setting up the DiskManager, BufferPool, HeapFileManager, IndexFileManager, WAL manager, TransactionManager, CheckpointManager, and loading catalog metadata.   

On Termination/Session End the close database function handles cleanup by flushing buffers, closing files, and clearing the current database context to ensure safe transitions between databases.

## Features

1. **Create Database**
   - Creates a new database directory under the root DB folder.
   - Automatically creates:
     - `logs` directory for Write-Ahead Logs (WAL)
     - `tables` directory to store table data
   - Validates that the database name is not empty and prevents overwriting existing databases.
   - Example:
     ```go
     err := storageEngine.CreateDatabase("mydb")
     ```

2. **List Databases**
   - Returns a list of all databases in the DB root directory.
   - Skips any non-directory files.
   - Example:
     ```go
     dbs, err := storageEngine.ExecuteShowDatabases()
     for _, db := range dbs {
         fmt.Println(db)
     }
     ```

3. **Use Database**
   - Switches the current working database.
   - Initializes the necessary components (DiskManager, BufferPool, HeapFileManager, IndexFileManager, WAL, TransactionManager, CheckpointManager).
   - Loads catalog metadata, table schemas, heap files, and indexes.
   - Performs WAL recovery to ensure the database is consistent.
   - Example:
     ```go
     err := storageEngine.UseDatabase("mydb")
     ```

4. **Close Current Database**
   - Safely flushes and closes all resources of the current database.
   - Ensures WAL is synced, buffer pool pages are flushed, and disk files are closed.

---

## Usage Example

```go
se := NewStorageEngine("/path/to/dbroot")

// Create a new database
if err := se.CreateDatabase("mydb"); err != nil {
    log.Fatal(err)
}

// Switch to the database
if err := se.UseDatabase("mydb"); err != nil {
    log.Fatal(err)
}

// List all databases
dbs, err := se.ExecuteShowDatabases()
if err != nil {
    log.Fatal(err)
}
fmt.Println("Databases:", dbs)