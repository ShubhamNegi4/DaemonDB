package bplus

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestDiskPagerBasicOperations tests basic pager operations
func TestDiskPagerBasicOperations(t *testing.T) {
	// Create a temporary test file
	testDir := filepath.Join(os.TempDir(), "daemondb_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	indexPath := filepath.Join(testDir, "test_index.idx")
	defer os.Remove(indexPath)

	// Create a new disk pager
	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to create disk pager: %v", err)
	}
	defer pager.Close()

	// Test 1: Allocate a new page
	pageID, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	if pageID != 1 {
		t.Errorf("Expected first page ID to be 1, got %d", pageID)
	}

	// Test 2: Write data to the page
	testData := make([]byte, PageSize)
	copy(testData, []byte("Hello, Disk Pager!"))
	if err := pager.WritePage(pageID, testData); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Test 3: Read the page back
	readData, err := pager.ReadPage(pageID)
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}

	// Verify the data matches
	if !bytes.Equal(testData, readData) {
		t.Errorf("Data mismatch: expected %q, got %q", string(testData[:20]), string(readData[:20]))
	}

	// Test 4: Allocate multiple pages
	pageID2, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate second page: %v", err)
	}
	if pageID2 != 2 {
		t.Errorf("Expected second page ID to be 2, got %d", pageID2)
	}

	// Test 5: Sync to ensure data is written to disk
	if err := pager.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Test 6: Close and reopen to test persistence
	pager.Close()

	newPager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer newPager.Close()

	// Read the data again after reopening
	persistedData, err := newPager.ReadPage(pageID)
	if err != nil {
		t.Fatalf("Failed to read persisted page: %v", err)
	}

	if !bytes.Equal(testData, persistedData) {
		t.Errorf("Data not persisted correctly: expected %q, got %q", string(testData[:20]), string(persistedData[:20]))
	}
}

// TestDiskPagerWithBPlusTree tests B+ tree operations with disk pager
func TestDiskPagerWithBPlusTree(t *testing.T) {
	// Create a temporary test file
	testDir := filepath.Join(os.TempDir(), "daemondb_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	indexPath := filepath.Join(testDir, "test_btree.idx")
	defer os.Remove(indexPath)

	// Create disk pager
	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to create disk pager: %v", err)
	}
	defer pager.Close()

	// Create buffer pool
	cache := NewBufferPool(10)

	// Create B+ tree with disk pager
	tree := NewBPlusTree(pager, cache, bytes.Compare)

	// Test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
	}

	// Test 1: Insert data
	t.Log("Testing insertions...")
	for _, data := range testData {
		tree.Insertion([]byte(data.key), []byte(data.value))
		t.Logf("Inserted: %s -> %s", data.key, data.value)
	}

	// Test 2: Search for inserted data
	t.Log("Testing searches...")
	for _, data := range testData {
		result, err := tree.Search([]byte(data.key))
		if err != nil {
			t.Fatalf("Failed to search for key %s: %v", data.key, err)
		}
		if result == nil {
			t.Errorf("Key %s not found after insertion", data.key)
			continue
		}
		if !bytes.Equal(result, []byte(data.value)) {
			t.Errorf("Value mismatch for key %s: expected %s, got %s", data.key, data.value, string(result))
		}
		t.Logf("Found: %s -> %s", data.key, string(result))
	}

	// Test 3: Search for non-existent key
	notFound, err := tree.Search([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Search for non-existent key returned error: %v", err)
	}
	if notFound != nil {
		t.Errorf("Expected nil for non-existent key, got %s", string(notFound))
	}

	// Test 4: Sync to ensure data is written
	if err := pager.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	t.Logf("Root node ID: %d", tree.root)
	t.Logf("Pager next page ID: %d", pager.nextPage)
}

// TestDiskPagerPageSizeEnforcement tests that the pager enforces PageSize
func TestDiskPagerPageSizeEnforcement(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "daemondb_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	indexPath := filepath.Join(testDir, "test_size.idx")
	defer os.Remove(indexPath)

	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to create disk pager: %v", err)
	}
	defer pager.Close()

	pageID, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	// Test: Writing data that's too small should fail
	smallData := make([]byte, PageSize-1)
	err = pager.WritePage(pageID, smallData)
	if err == nil {
		t.Error("Expected error when writing data smaller than PageSize")
	}

	// Test: Writing data that's too large should fail
	largeData := make([]byte, PageSize+1)
	err = pager.WritePage(pageID, largeData)
	if err == nil {
		t.Error("Expected error when writing data larger than PageSize")
	}

	// Test: Writing exactly PageSize should succeed
	correctData := make([]byte, PageSize)
	copy(correctData, []byte("Correct size data"))
	err = pager.WritePage(pageID, correctData)
	if err != nil {
		t.Errorf("Writing correct size data should succeed, got: %v", err)
	}
}

// TestDiskPagerMultiplePages tests writing and reading multiple pages
func TestDiskPagerMultiplePages(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "daemondb_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	indexPath := filepath.Join(testDir, "test_multi.idx")
	defer os.Remove(indexPath)

	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		t.Fatalf("Failed to create disk pager: %v", err)
	}
	defer pager.Close()

	// Allocate and write to multiple pages
	numPages := 5
	pageIDs := make([]int64, numPages)
	pageData := make([][]byte, numPages)

	for i := 0; i < numPages; i++ {
		pageID, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page %d: %v", i, err)
		}
		pageIDs[i] = pageID

		data := make([]byte, PageSize)
		copy(data, []byte{byte(i), byte(i + 1), byte(i + 2)})
		pageData[i] = data

		if err := pager.WritePage(pageID, data); err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	// Read back all pages and verify
	for i := 0; i < numPages; i++ {
		readData, err := pager.ReadPage(pageIDs[i])
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", i, err)
		}

		if !bytes.Equal(pageData[i], readData) {
			t.Errorf("Page %d data mismatch", i)
		}
	}
}
