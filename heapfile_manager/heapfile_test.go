package heapfile

import (
	"fmt"
	"os"
	"testing"
)

func TestHeapFileOperations(t *testing.T) {
	// Create a temporary test directory
	testDir := "./test_heap"
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("failed to create test dir: %v", err)
	}
	// defer os.RemoveAll(testDir) // Clean up after test

	// Initialize heap file manager
	hfm, err := NewHeapFileManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create heap file manager: %v", err)
	}

	// Create a test heap file
	tableName := "students"
	fileID := uint32(1)
	err = hfm.CreateHeapfile(tableName, fileID)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}

	fmt.Printf("✓ Created heap file: %s_%d.heap\n", tableName, fileID)

	// Test data: multiple rows of different sizes
	testRows := []struct {
		name string
		data []byte
	}{
		{"Row1", []byte("Alice|20|A")},
		{"Row2", []byte("Bob|21|B")},
		{"Row3", []byte("Charlie|22|A")},
		{"Row4", []byte("Diana|19|C")},
		{"Row5", []byte("Eve|20|B")},
		{"Row6", []byte("Frank|21|A")},
		{"Row7", []byte("Grace|20|B")},
		{"Row8", []byte("Henry|22|C")},
	}

	// Store RowPointers for later retrieval
	rowPointers := make([]*RowPointer, 0, len(testRows))

	// Insert all rows
	fmt.Println("\n=== Inserting Rows ===")
	for i, row := range testRows {
		rowPtr, err := hfm.InsertRow(fileID, row.data)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", row.name, err)
		}
		rowPointers = append(rowPointers, rowPtr)
		fmt.Printf("✓ Inserted %s → File:%d, Page:%d, Slot:%d\n",
			row.name, rowPtr.FileID, rowPtr.PageNumber, rowPtr.SlotIndex)

		// Check if we're creating new pages
		if i > 0 && rowPtr.PageNumber != rowPointers[i-1].PageNumber {
			fmt.Printf("  → New page created! (Page %d)\n", rowPtr.PageNumber)
		}
	}

	// Verify we can read all rows back
	fmt.Println("\n=== Reading Rows Back ===")
	for i, rowPtr := range rowPointers {
		expectedData := testRows[i].data

		// Read row back
		readData, err := hfm.GetRow(rowPtr)
		if err != nil {
			t.Fatalf("Failed to read %s: %v", testRows[i].name, err)
		}

		// Verify data matches
		if string(readData) != string(expectedData) {
			t.Errorf("Data mismatch for %s:\n  Expected: %s\n  Got:      %s",
				testRows[i].name, string(expectedData), string(readData))
		} else {
			fmt.Printf("✓ Read %s → %s\n", testRows[i].name, string(readData))
		}
	}

	// Test reading by RowPointer directly
	fmt.Println("\n=== Testing Direct RowPointer Access ===")
	testPtr := rowPointers[2] // Charlie's row
	readData, err := hfm.GetRow(testPtr)
	if err != nil {
		t.Fatalf("Failed to read by RowPointer: %v", err)
	}
	fmt.Printf("✓ Read row at Page:%d, Slot:%d → %s\n",
		testPtr.PageNumber, testPtr.SlotIndex, string(readData))

	// Check that heap file was created
	heapFile := hfm.files[fileID]
	if heapFile == nil {
		t.Fatalf("Heap file not found")
	}
	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total rows inserted: %d\n", len(testRows))
	fmt.Printf("Heap file created successfully\n")
}

func TestMultiplePages(t *testing.T) {
	// Test that we create multiple pages when needed
	testDir := "./test_multipage"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	hfm, err := NewHeapFileManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create heap file manager: %v", err)
	}

	tableName := "large_table"
	fileID := uint32(1)
	err = hfm.CreateHeapfile(tableName, fileID)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}

	// Insert enough rows to force multiple pages
	// Each row is ~20 bytes, so we need ~200 rows to fill a page
	// Let's insert 50 rows which should fit in 1-2 pages
	fmt.Println("\n=== Testing Multiple Pages ===")
	pageCounts := make(map[uint32]int)

	for i := 0; i < 50; i++ {
		rowData := []byte(fmt.Sprintf("Student_%03d|Age_%d|Grade_%c", i, 20+i%5, 'A'+(i%3)))
		rowPtr, err := hfm.InsertRow(fileID, rowData)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
		pageCounts[rowPtr.PageNumber]++

		if i%10 == 0 {
			fmt.Printf("Inserted row %d → Page %d\n", i, rowPtr.PageNumber)
		}
	}

	fmt.Printf("\n=== Page Distribution ===\n")
	for pageNum, count := range pageCounts {
		fmt.Printf("Page %d: %d rows\n", pageNum, count)
	}

	if len(pageCounts) == 1 {
		fmt.Println("⚠ All rows fit in single page (might need more rows to test multi-page)")
	} else {
		fmt.Printf("✓ Successfully created %d pages\n", len(pageCounts))
	}

	// Verify we can read from different pages
	fmt.Println("\n=== Reading from Different Pages ===")
	for pageNum := range pageCounts {
		// Find a row pointer on this page
		for i := 0; i < 50; i++ {
			rowData := []byte(fmt.Sprintf("Student_%03d|Age_%d|Grade_%c", i, 20+i%5, 'A'+(i%3)))
			rowPtr, _ := hfm.InsertRow(fileID, rowData)
			if rowPtr.PageNumber == pageNum {
				readData, err := hfm.GetRow(rowPtr)
				if err != nil {
					t.Fatalf("Failed to read from page %d: %v", pageNum, err)
				}
				fmt.Printf("✓ Read from Page %d: %s\n", pageNum, string(readData))
				break
			}
		}
	}
}

func TestSlotDirectory(t *testing.T) {
	// Test that slot directory works correctly
	testDir := "./test_slots"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	hfm, err := NewHeapFileManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create heap file manager: %v", err)
	}

	tableName := "slot_test"
	fileID := uint32(1)
	err = hfm.CreateHeapfile(tableName, fileID)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}

	fmt.Println("\n=== Testing Slot Directory ===")

	// Insert rows and verify slot indices are sequential
	rowPointers := make([]*RowPointer, 0)
	for i := 0; i < 10; i++ {
		rowData := []byte(fmt.Sprintf("Row_%d", i))
		rowPtr, err := hfm.InsertRow(fileID, rowData)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
		rowPointers = append(rowPointers, rowPtr)

		// Verify slot index matches insertion order
		if rowPtr.SlotIndex != uint16(i) {
			t.Errorf("Expected slot index %d, got %d", i, rowPtr.SlotIndex)
		}
	}

	fmt.Println("✓ Slot indices are sequential:")
	for i, ptr := range rowPointers {
		fmt.Printf("  Row %d → Slot %d\n", i, ptr.SlotIndex)
	}

	// Verify we can read rows using slot indices
	fmt.Println("\n=== Reading via Slot Indices ===")
	for i, ptr := range rowPointers {
		readData, err := hfm.GetRow(ptr)
		if err != nil {
			t.Fatalf("Failed to read row at slot %d: %v", i, err)
		}
		expected := fmt.Sprintf("Row_%d", i)
		if string(readData) != expected {
			t.Errorf("Expected %s, got %s", expected, string(readData))
		} else {
			fmt.Printf("✓ Slot %d → %s\n", i, string(readData))
		}
	}
}

func TestPageHeader(t *testing.T) {
	// Test that page headers are correctly maintained
	testDir := "./test_header"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	hfm, err := NewHeapFileManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create heap file manager: %v", err)
	}

	tableName := "header_test"
	fileID := uint32(1)
	err = hfm.CreateHeapfile(tableName, fileID)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}

	fmt.Println("\n=== Testing Page Header Updates ===")

	heapFile := hfm.files[fileID]

	// Insert a few rows and check header after each
	for i := 0; i < 5; i++ {
		rowData := []byte(fmt.Sprintf("TestRow_%d", i))
		rowPtr, err := heapFile.insertRow(rowData)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}

		// Read page and check header
		page, err := heapFile.readPage(rowPtr.PageNumber)
		if err != nil {
			t.Fatalf("Failed to read page: %v", err)
		}

		header := readPageHeader(page)
		fmt.Printf("After row %d:\n", i)
		fmt.Printf("  NumRows: %d\n", header.NumRows)
		fmt.Printf("  SlotCount: %d\n", header.SlotCount)
		fmt.Printf("  FreePtr: %d\n", header.FreePtr)
		fmt.Printf("  IsPageFull: %d\n", header.IsPageFull)

		// Verify header matches expected values
		if header.NumRows != uint16(i+1) {
			t.Errorf("Expected NumRows=%d, got %d", i+1, header.NumRows)
		}
		if header.SlotCount != uint16(i+1) {
			t.Errorf("Expected SlotCount=%d, got %d", i+1, header.SlotCount)
		}
		fmt.Println()
	}
}

// Main test runner
func TestAll(t *testing.T) {
	fmt.Println("========================================")
	fmt.Println("Heap File System Test Suite")
	fmt.Println("========================================")
	fmt.Println()

	t.Run("BasicOperations", TestHeapFileOperations)
	t.Run("MultiplePages", TestMultiplePages)
	t.Run("SlotDirectory", TestSlotDirectory)
	t.Run("PageHeader", TestPageHeader)

	fmt.Println("\n========================================")
	fmt.Println("All tests completed!")
	fmt.Println("========================================")
}
