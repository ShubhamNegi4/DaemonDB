package bplus

import (
	"fmt"
	"sync"
)

type InMemoryPager struct {
	pages    map[int64][]byte
	nextPage int64
	mu       sync.RWMutex
	closed   bool
}

func NewInMemoryPager() *InMemoryPager {
	return &InMemoryPager{
		pages:    make(map[int64][]byte),
		nextPage: 1,
		closed:   false,
	}
}

func (p *InMemoryPager) ReadPage(pageId int64) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, fmt.Errorf("pager is closed")
	}

	data, ok := p.pages[pageId]
	if !ok {
		return nil, fmt.Errorf("page %d not found", pageId)
	}

	// Return a copy so the caller cannot modify internal state directly
	// without calling WritePage
	out := make([]byte, PageSize)
	copy(out, data)
	return out, nil
}

func (p *InMemoryPager) WritePage(pageId int64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("pager is closed")
	}

	if len(data) != PageSize {
		return fmt.Errorf("data size %d does not match page size %d", len(data), PageSize)
	}

	dest := make([]byte, PageSize)
	copy(dest, data)
	p.pages[pageId] = dest

	return nil
}

func (p *InMemoryPager) AllocatePage() (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, fmt.Errorf("pager is closed")
	}

	id := p.nextPage
	p.nextPage++

	p.pages[id] = make([]byte, PageSize)
	return id, nil
}

func (p *InMemoryPager) DeallocatePage(pageId int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("pager is closed")
	}

	delete(p.pages, pageId)
	return nil
}

func (p *InMemoryPager) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("pager is closed")
	}

	// In-memory sync is a no-op, but we should still check if we are closed
	return nil
}

func (p *InMemoryPager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	// This helps catch bugs where you might try to access the DB after closing it.
	p.pages = nil
	p.closed = true

	return nil
}

func (p *InMemoryPager) TotalPages() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextPage
}
