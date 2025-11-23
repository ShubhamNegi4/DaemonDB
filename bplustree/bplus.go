package bplus

import (
	"bytes"
	"fmt"
)

func Bplus() {
	pager := NewInMemoryPager()
	cache := NewBufferPool(10)
	tree := NewBPlusTree(pager, cache, bytes.Compare)
	fmt.Println("=== Student Database Test ===")

	students := []struct {
		id    string
		name  string
		grade string
	}{
		{"S001", "Alice Johnson", "A"},
		{"S002", "Bob Smith", "B"},
		{"S003", "Charlie Brown", "A"},
		{"S004", "Diana Prince", "C"},
		{"S005", "Eve Wilson", "B"},
	}

	// Insert all students
	for _, student := range students {
		// Create a simple record: "name|grade"
		record := student.name + "|" + student.grade
		tree.Insertion([]byte(student.id), []byte(record))
		fmt.Printf("Inserted: %s -> %s\n", student.id, record)
	}

	fmt.Println("\n=== Searching Students ===")

	// Search for specific students
	searchIDs := []string{"S001", "S003", "S999"}

	for _, id := range searchIDs {
		result, _ := tree.Search([]byte(id))
		if result != nil {
			fmt.Printf("Found %s: %s\n", id, string(result))
		} else {
			fmt.Printf("Student %s not found\n", id)
		}
	}

	fmt.Println("\n=== Database Stats ===")
	fmt.Printf("Root ID: %d\n", tree.root)
	fmt.Printf("Cache size: %d/%d pages\n", cache.Size(), cache.Capacity())
	fmt.Printf("Pager next ID: %d\n", pager.nextPage)
}
