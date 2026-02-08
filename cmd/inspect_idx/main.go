// Inspect a B+ tree primary key index file (.idx).
// Usage: go run ./cmd/inspect_idx <path-to-.idx>
// Example: go run ./cmd/inspect_idx databases/demp/indexes/students_primary.idx
package main

import (
	"fmt"
	"os"

	"DaemonDB/bplustree"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <index.idx>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s databases/demp/indexes/students_primary.idx\n", os.Args[0])
		os.Exit(1)
	}
	path := os.Args[1]
	if err := bplus.InspectIndexFile(path); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
