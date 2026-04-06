package bufferpool

/*
policy.go — eviction policy interface + wrappers for external libraries.

Dependency map:
  LRU-K   → your own implementation (no external dep)
  TinyLFU → github.com/dgraph-io/ristretto/v2

go get commands:
  go get github.com/hashicorp/golang-lru/v2
  go get github.com/dgraph-io/ristretto/v2
*/

import (
	"DaemonDB/storage_engine/page"
	"fmt"

	"github.com/dgraph-io/ristretto/v2"
)

type EvictionPolicy interface {
	OnAccess(pageID int64)
	ChooseVictim(pages map[int64]*page.Page) int64
	OnEvict(pageID int64)
	Name() string
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. LRU-K  (your own implementation)
// ─────────────────────────────────────────────────────────────────────────────

type LRUKPolicy struct {
	k     int
	clock uint64
	meta  map[int64]*lruKMeta
}

func NewLRUKPolicy(k int) *LRUKPolicy {
	return &LRUKPolicy{
		k:    k,
		meta: make(map[int64]*lruKMeta),
	}
}

func (p *LRUKPolicy) OnAccess(pageID int64) {
	p.clock++
	m := p.meta[pageID]
	if m == nil {
		m = &lruKMeta{}
		p.meta[pageID] = m
	}
	m.recordAccess(p.clock)
}

func (p *LRUKPolicy) ChooseVictim(pages map[int64]*page.Page) int64 {
	now := p.clock
	var (
		coldVictim    int64  = -1
		coldTimestamp uint64 = ^uint64(0)
		hotVictim     int64  = -1
		hotMaxDist    uint64 = 0
	)
	for pageID, pg := range pages {
		pg.RLock()
		pinned := pg.PinCount > 0
		pg.RUnlock()
		if pinned {
			continue
		}
		m := p.meta[pageID]
		if m == nil || m.isCold() {
			var lastAccess uint64
			if m != nil && len(m.hist) > 0 {
				lastAccess = m.hist[len(m.hist)-1]
			}
			if lastAccess < coldTimestamp {
				coldTimestamp = lastAccess
				coldVictim = pageID
			}
		} else {
			if dist := m.backwardKDist(now); dist > hotMaxDist {
				hotMaxDist = dist
				hotVictim = pageID
			}
		}
	}
	if coldVictim != -1 {
		return coldVictim
	}
	return hotVictim
}

func (p *LRUKPolicy) OnEvict(pageID int64) { delete(p.meta, pageID) }
func (p *LRUKPolicy) Name() string         { return fmt.Sprintf("lru-%d", p.k) }

// ─────────────────────────────────────────────────────────────────────────────
// 2. W-TinyLFU  (github.com/dgraph-io/ristretto/v2)
// ─────────────────────────────────────────────────────────────────────────────

type TinyLFUPolicy struct {
	cache *ristretto.Cache[int64, struct{}]
	freq  map[int64]int64
}

func NewTinyLFUPolicy(capacity int) *TinyLFUPolicy {
	cfg := &ristretto.Config[int64, struct{}]{
		NumCounters: int64(capacity) * 10,
		MaxCost:     int64(capacity),
		BufferItems: 64,
	}
	c, err := ristretto.NewCache(cfg)
	if err != nil {
		panic(fmt.Sprintf("tinylfu: failed to create cache: %v", err))
	}
	return &TinyLFUPolicy{
		cache: c,
		freq:  make(map[int64]int64, capacity),
	}
}

func (p *TinyLFUPolicy) OnAccess(pageID int64) {
	p.freq[pageID]++
	p.cache.Set(pageID, struct{}{}, 1)
}

func (p *TinyLFUPolicy) ChooseVictim(pages map[int64]*page.Page) int64 {
	var victim int64 = -1
	var minFreq int64 = -1
	for pageID, pg := range pages {
		pg.RLock()
		pinned := pg.PinCount > 0
		pg.RUnlock()
		if pinned {
			continue
		}
		f := p.freq[pageID]
		if victim == -1 || f < minFreq {
			minFreq = f
			victim = pageID
		}
	}
	return victim
}

func (p *TinyLFUPolicy) OnEvict(pageID int64) {
	delete(p.freq, pageID)
	p.cache.Del(pageID)
}

func (p *TinyLFUPolicy) Name() string { return "w-tinylfu" }
