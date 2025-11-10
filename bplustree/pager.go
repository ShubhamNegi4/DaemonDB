package bplus

/*
type Pager interface {
	ReadPage(pageID int64) ([]byte, error)
	WritePage(pageID int64, data []byte) error
	AllocatePage() (int64, error)
	DeallocatePage(pageID int64) error
	Sync() error
	Close() error
}
*/
import (
	"fmt"
	"os"
	"sync"
)

type InMemoryPager struct {
	pages    map[int64][]byte
	nextPage int64
	mu       sync.RWMutex
}

type OnDiskPager struct {
	file       *os.File
	pageSize   int
	numPages   uint32
	IsPageFull int16 // is page full
}

func NewInMemoryPager() *InMemoryPager {
	return &InMemoryPager{
		pages:    make(map[int64][]byte),
		nextPage: 1,
	}
}

func NewOnDiskPager(indexPath string) (*OnDiskPager, error) {
	fmt.Printf("this pager is to inserted on : %s", indexPath)
	return &OnDiskPager{
		// TODO: implement this
	}, nil
}

func (p *InMemoryPager) ReadPage(pageId int64) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	data, ok := p.pages[pageId]
	if !ok {
		return nil, fmt.Errorf("page %d not found", pageId)
	}
	return append([]byte(nil), data...), nil
}

func (p *InMemoryPager) WritePage(pageId int64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pages[pageId] = append([]byte(nil), data...)
	return nil
}

func (p *InMemoryPager) AllocatePage() (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := p.nextPage
	p.nextPage++
	p.pages[id] = make([]byte, PageSize)
	return id, nil
}

func (p *InMemoryPager) DeallocatePage(pageId int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.pages, pageId)
	return nil
}

func (p *InMemoryPager) Sync() error {
	return nil
}

func (p *InMemoryPager) Close() error {
	return nil
}
