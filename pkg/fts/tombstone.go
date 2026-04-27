package fts

import "sync"

// Tombstones tracks deleted document ords. Indexes still hold postings
// that reference these ords, but search paths filter them out before
// returning results.
// The zero value is empty (no tombstones). Safe for concurrent use.
type Tombstones struct {
	mu   sync.RWMutex
	bits []uint64
	any  bool // fast-path flag: false --> IsSet always returns false
}

func NewTombstones() *Tombstones {
	return &Tombstones{}
}

func (t *Tombstones) Set(ord DocOrd) {
	t.mu.Lock()
	defer t.mu.Unlock()
	word := uint32(ord) / 64
	for int(word) >= len(t.bits) {
		t.bits = append(t.bits, 0)
	}
	t.bits[word] |= 1 << (uint32(ord) % 64)
	t.any = true
}

func (t *Tombstones) IsSet(ord DocOrd) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.any {
		return false
	}
	word := uint32(ord) / 64
	if int(word) >= len(t.bits) {
		return false
	}
	return t.bits[word]&(1<<(uint32(ord)%64)) != 0
}

func (t *Tombstones) Any() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.any
}

func (t *Tombstones) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.any {
		return 0
	}
	var n int
	for _, w := range t.bits {
		n += popcount(w)
	}
	return n
}

func (t *Tombstones) Snapshot() []uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.bits) == 0 {
		return nil
	}
	out := make([]uint64, len(t.bits))
	copy(out, t.bits)
	return out
}

func RestoreTombstones(bits []uint64) *Tombstones {
	t := &Tombstones{
		bits: make([]uint64, len(bits)),
	}
	copy(t.bits, bits)
	for _, w := range t.bits {
		if w != 0 {
			t.any = true
			break
		}
	}
	return t
}

func popcount(x uint64) int {
	x = x - ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f
	return int((x * 0x0101010101010101) >> 56)
}
