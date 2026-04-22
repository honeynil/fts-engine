package fts

import "sync"

type collectionStats struct {
	mu       sync.RWMutex
	docsSeen map[DocID]struct{}
	docLen   map[string]map[DocID]uint32
	totalLen map[string]uint64
}

func newCollectionStats() *collectionStats {
	return &collectionStats{
		docsSeen: make(map[DocID]struct{}),
		docLen:   make(map[string]map[DocID]uint32),
		totalLen: make(map[string]uint64),
	}
}

func (c *collectionStats) observe(field string, id DocID, tokens uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, seen := c.docsSeen[id]; !seen {
		c.docsSeen[id] = struct{}{}
	}

	perField, ok := c.docLen[field]
	if !ok {
		perField = make(map[DocID]uint32)
		c.docLen[field] = perField
	}
	perField[id] += tokens
	c.totalLen[field] += uint64(tokens)
}

func (c *collectionStats) TotalDocs() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.docsSeen)
}

func (c *collectionStats) DocLen(field string, id DocID) uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if m, ok := c.docLen[field]; ok {
		return m[id]
	}
	return 0
}

func (c *collectionStats) AvgDocLen(field string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	perField, ok := c.docLen[field]
	if !ok || len(perField) == 0 {
		return 0
	}
	return float64(c.totalLen[field]) / float64(len(perField))
}

func (c *collectionStats) FieldDocCount(field string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if m, ok := c.docLen[field]; ok {
		return len(m)
	}
	return 0
}
