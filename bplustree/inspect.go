// Package bplus: index file inspection for debugging.
// Use InspectIndexFile(path) to print a human-readable dump of a primary key index (.idx).

package bplus

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// InspectIndexFile opens a B+ tree index file and prints its structure to stdout.
func InspectIndexFile(indexPath string) error {
	return InspectIndexFileTo(os.Stdout, indexPath)
}

// InspectIndexFileTo writes a human-readable dump of the index file to w:
// page 0 = root id, then each node's keys and (for leaves) key → row pointer.
func InspectIndexFileTo(w io.Writer, indexPath string) error {
	pager, err := NewOnDiskPager(indexPath)
	if err != nil {
		return err
	}
	defer pager.Close()

	meta, err := pager.ReadPage(0)
	if err != nil {
		return fmt.Errorf("read meta page: %w", err)
	}
	if len(meta) < 8 {
		return fmt.Errorf("meta page too short")
	}

	rootID := int64(binary.LittleEndian.Uint64(meta[0:8]))
	p := func(format string, args ...interface{}) { fmt.Fprintf(w, format, args...) }
	pln := func(s string) { fmt.Fprintln(w, s) }

	p("Index file: %s\n", indexPath)
	p("  Page 0 (meta): root page id = %d\n", rootID)
	if rootID == 0 {
		pln("  (empty tree)")
		return nil
	}

	pln("\n  Nodes (BFS):")
	pln("  ---")

	var queue []int64
	queue = append(queue, rootID)
	level := 0

	for len(queue) > 0 {
		size := len(queue)
		p("  Level %d:\n", level)
		for i := 0; i < size; i++ {
			pageID := queue[i]
			page, err := pager.ReadPage(pageID)
			if err != nil {
				p("    [page %d] read error: %v\n", pageID, err)
				continue
			}
			node, err := decodeNode(page, pageID)
			if err != nil {
				p("    [page %d] decode error: %v\n", pageID, err)
				continue
			}

			if node.nodeType == NodeInternal {
				keyStrs := make([]string, len(node.key))
				for j, k := range node.key {
					keyStrs[j] = string(k)
				}
				p("    [page %d] INTERNAL keys=%v children=%v\n",
					pageID, keyStrs, node.children)
				for _, c := range node.children {
					if c != 0 {
						queue = append(queue, c)
					}
				}
			} else {
				p("    [page %d] LEAF numKeys=%d next=%d\n", pageID, node.numKeys, node.next)
				for j := 0; j < int(node.numKeys); j++ {
					key := node.key[j]
					keyStr := formatKey(key)
					var valStr string
					if j < len(node.vals) {
						valStr = formatRowPointer(node.vals[j])
					} else {
						valStr = "?"
					}
					p("      %s -> %s\n", keyStr, valStr)
				}
				if node.next != 0 {
					queue = append(queue, node.next)
				}
			}
		}
		pln("  ---")
		queue = queue[size:]
		level++
	}

	return nil
}

// formatKey shows key bytes: 4-byte = int, else length-prefixed = string, else quoted.
func formatKey(b []byte) string {
	if len(b) == 4 {
		return fmt.Sprintf("%d", binary.LittleEndian.Uint32(b))
	}
	if len(b) >= 2 {
		ln := binary.LittleEndian.Uint16(b[0:2])
		if int(ln) == len(b)-2 && ln > 0 && ln < 256 {
			return fmt.Sprintf("%q", string(b[2:2+ln]))
		}
	}
	return fmt.Sprintf("%q", string(b))
}

// formatRowPointer formats 10-byte value as (FileID, PageNum, Slot) and raw hex.
// Row pointer layout: FileID uint32 LE (4B) | PageNumber uint32 LE (4B) | SlotIndex uint16 LE (2B) = 10 bytes.
func formatRowPointer(b []byte) string {
	if len(b) < 10 {
		return fmt.Sprintf("<%d bytes>", len(b))
	}
	fileID := binary.LittleEndian.Uint32(b[0:4])
	pageNum := binary.LittleEndian.Uint32(b[4:8])
	slot := binary.LittleEndian.Uint16(b[8:10])
	hexStr := fmt.Sprintf("%02x %02x %02x %02x %02x %02x %02x %02x %02x %02x",
		b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9])
	return fmt.Sprintf("(file=%d page=%d slot=%d) [%s]", fileID, pageNum, slot, hexStr)
}
