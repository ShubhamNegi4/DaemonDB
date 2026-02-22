package page

import (
	"DaemonDB/types"
	"sync"
)

const (
	PageSize      = 4096
	PageLSNOffset = 0 // first 8 bytes of every page = LSN
)

/*
This contains a page struct
this can be moved to seperate entites of index file manager and heap file manager
but a central package allows more clear way, since both of the pages are ultimately be send to bufferpool

the package is to use common struct variables, the actual format of writing the data is different in both the page type
for heap page: the implementation is at : /DaemonDB/storage_engine/access/heapfile_manager/heap_page.go
for index page: the implementation is at: /DaemonDB/storage_engine/access/indexfile_manager/bplustree/node_to_index_page.go


Additionally the concept of LSN for WAL replay is used only for heap page, since they are the ones that are replay on a bigger level, internally they create/read the indexes like normal queries
so for index pages lsn remains untouched more or less (the Node of B+ tree doesnt have to do anything with lsn too)
so keeping it simple
*/

type Page struct {
	ID       int64
	FileID   uint32
	Data     []byte
	IsDirty  bool
	PinCount int32
	PageType types.PageType
	LSN      uint64 // in-memory, set by heap/index layer
	mu       sync.RWMutex
}

func (p *Page) Lock() {
	p.mu.Lock()
}

func (p *Page) Unlock() {
	p.mu.Unlock()
}

func (p *Page) RLock() {
	p.mu.RLock()
}

func (p *Page) RUnlock() {
	p.mu.RUnlock()
}
