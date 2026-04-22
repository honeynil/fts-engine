package fts

import (
	"context"
	"fmt"
	"io"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"
)

type Service struct {
	indexFactory IndexFactory
	keyGen       KeyGenerator
	pipeline     Pipeline
	filter       Filter
	scorer       Scorer
	collection   *collectionStats

	mu      sync.RWMutex
	indexes map[string]Index
}

func New(index Index, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	s.indexFactory = func(name string) (Index, error) {
		return nil, fmt.Errorf("fts: field %q is not available (service was built with fts.New — single-field mode; use fts.NewMultiField to index arbitrary fields)", name)
	}
	if index != nil {
		s.indexes[DefaultField] = index
	}
	return s
}

func NewMultiField(factory IndexFactory, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	s.indexFactory = factory
	return s
}

func newService(keyGen KeyGenerator, opts ...Option) *Service {
	s := &Service{
		keyGen:     keyGen,
		pipeline:   defaultPipeline{},
		indexes:    make(map[string]Index),
		collection: newCollectionStats(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.keyGen == nil {
		s.keyGen = WordKeys
	}
	return s
}

func NewFromReader(r io.Reader, loader IndexLoader, keyGen KeyGenerator, opts ...Option) (*Service, error) {
	if loader == nil {
		return nil, fmt.Errorf("fts: nil index loader")
	}

	index, err := loader(r)
	if err != nil {
		return nil, fmt.Errorf("fts: load index: %w", err)
	}

	return New(index, keyGen, opts...), nil
}

func NewMultiFieldFromIndexes(indexes map[string]Index, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	maps.Copy(s.indexes, indexes)
	s.indexFactory = func(name string) (Index, error) {
		return nil, fmt.Errorf("fts: field %q was not present in the restored snapshot", name)
	}
	return s
}

func (s *Service) Index(ctx context.Context, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if doc.ID == "" {
		return fmt.Errorf("fts: document id is empty")
	}
	if len(doc.Fields) == 0 {
		return fmt.Errorf("fts: document %q has no fields", doc.ID)
	}

	for name, field := range doc.Fields {
		if err := s.indexField(ctx, doc.ID, name, field); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) IndexDocument(ctx context.Context, docID DocID, content string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if docID == "" {
		return fmt.Errorf("fts: document id is empty")
	}
	return s.indexField(ctx, docID, DefaultField, Field{Value: content})
}

func (s *Service) indexField(ctx context.Context, docID DocID, name string, field Field) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	index, err := s.getOrCreateIndex(name)
	if err != nil {
		return fmt.Errorf("fts: index document %q: %w", docID, err)
	}

	pipeline := field.Pipeline
	if pipeline == nil {
		pipeline = s.pipeline
	}

	tokens := pipeline.Process(field.Value)

	if s.scorer != nil {
		s.collection.observe(name, docID, uint32(len(tokens)))
	}

	positional, hasPositions := index.(PositionalIndex)

	for pos, token := range tokens {
		if err := ctx.Err(); err != nil {
			return err
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return fmt.Errorf("fts: index document %q field %q: keygen: %w", docID, name, err)
		}

		for _, key := range keys {
			if s.filter != nil {
				if ok := s.filter.Add([]byte(key)); !ok {
					return fmt.Errorf("fts: index document %q field %q: filter add failed for key %q", docID, name, key)
				}
			}
			if hasPositions {
				if err := positional.InsertAt(key, docID, uint32(pos)); err != nil {
					return fmt.Errorf("fts: index document %q field %q: insert: %w", docID, name, err)
				}
			} else {
				if err := index.Insert(key, docID); err != nil {
					return fmt.Errorf("fts: index document %q field %q: insert: %w", docID, name, err)
				}
			}
		}
	}

	return nil
}

func (s *Service) SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error) {
	return s.searchFields(ctx, s.fieldNames(), query, maxResults)
}

func (s *Service) SearchField(ctx context.Context, field string, query string, maxResults int) (*SearchResult, error) {
	return s.searchFields(ctx, []string{field}, query, maxResults)
}

func (s *Service) searchFields(ctx context.Context, fields []string, query string, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	start := time.Now()
	timings := make(map[string]string, 3)

	preStart := time.Now()
	tokens := s.pipeline.Process(query)
	timings["preprocess"] = formatDuration(time.Since(preStart))

	searchStart := time.Now()
	uniqueMatches := make(map[DocID]int)
	totalMatches := make(map[DocID]int)
	scores := make(map[DocID]float64)

	for _, field := range fields {
		s.mu.RLock()
		index, ok := s.indexes[field]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		var fieldStats FieldStats
		if s.scorer != nil {
			fieldStats = FieldStats{
				N:         s.collection.FieldDocCount(field),
				AvgLength: s.collection.AvgDocLen(field),
			}
		}

		for _, token := range tokens {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			keys, err := s.keyGen(token)
			if err != nil {
				return nil, fmt.Errorf("fts: search: keygen: %w", err)
			}

			for _, key := range keys {
				if s.filter != nil && !s.filter.Contains([]byte(key)) {
					continue
				}

				docs, err := index.Search(key)
				if err != nil {
					return nil, fmt.Errorf("fts: search field %q: %w", field, err)
				}

				if s.scorer != nil {
					df := uint32(len(docs))
					for _, doc := range docs {
						uniqueMatches[doc.ID]++
						totalMatches[doc.ID] += int(doc.Count)

						ts := TermStats{Field: field, Term: token, TF: doc.Count, DF: df}
						ds := DocStats{ID: doc.ID, Length: s.collection.DocLen(field, doc.ID)}
						scores[doc.ID] += s.scorer.Score(ts, ds, fieldStats)
					}
				} else {
					for _, doc := range docs {
						uniqueMatches[doc.ID]++
						totalMatches[doc.ID] += int(doc.Count)
					}
				}
			}
		}
	}

	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	results := make([]Result, 0, len(uniqueMatches))
	for id, unique := range uniqueMatches {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: unique,
			TotalMatches:  totalMatches[id],
			Score:         scores[id],
		})
	}

	if s.scorer != nil {
		sort.Slice(results, func(i, j int) bool {
			if results[i].Score != results[j].Score {
				return results[i].Score > results[j].Score
			}
			return results[i].ID < results[j].ID
		})
	} else {
		sort.Slice(results, func(i, j int) bool {
			if results[i].UniqueMatches != results[j].UniqueMatches {
				return results[i].UniqueMatches > results[j].UniqueMatches
			}
			if results[i].TotalMatches != results[j].TotalMatches {
				return results[i].TotalMatches > results[j].TotalMatches
			}
			return results[i].ID < results[j].ID
		})
	}

	totalFound := len(results)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}

	timings["total"] = formatDuration(time.Since(start))

	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
		Timings:           timings,
	}, nil
}

func (s *Service) SearchPhrase(ctx context.Context, phrase string, maxResults int) (*SearchResult, error) {
	return s.searchPhrase(ctx, s.fieldNames(), phrase, maxResults)
}

func (s *Service) SearchPhraseField(ctx context.Context, field string, phrase string, maxResults int) (*SearchResult, error) {
	return s.searchPhrase(ctx, []string{field}, phrase, maxResults)
}

func (s *Service) searchPhrase(ctx context.Context, fields []string, phrase string, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	start := time.Now()
	timings := make(map[string]string, 3)

	preStart := time.Now()
	tokens := s.pipeline.Process(phrase)
	timings["preprocess"] = formatDuration(time.Since(preStart))

	if len(tokens) == 0 {
		timings["total"] = formatDuration(time.Since(start))
		return &SearchResult{Results: []Result{}, Timings: timings}, nil
	}
	if len(tokens) == 1 {
		return s.searchFields(ctx, fields, phrase, maxResults)
	}

	searchStart := time.Now()
	phraseTerm := strings.Join(tokens, " ")
	phraseCounts := make(map[DocID]uint32)
	scores := make(map[DocID]float64)

	for _, field := range fields {
		s.mu.RLock()
		index, ok := s.indexes[field]
		s.mu.RUnlock()
		if !ok {
			continue
		}
		positional, ok := index.(PositionalIndex)
		if !ok {
			continue
		}

		tokenPostings := make([]map[DocID][]uint32, len(tokens))
		skipField := false
		for i, token := range tokens {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			keys, err := s.keyGen(token)
			if err != nil {
				return nil, fmt.Errorf("fts: phrase search: keygen: %w", err)
			}

			merged := make(map[DocID][]uint32)
			for _, key := range keys {
				if s.filter != nil && !s.filter.Contains([]byte(key)) {
					continue
				}
				refs, err := positional.SearchPositional(key)
				if err != nil {
					return nil, fmt.Errorf("fts: phrase search field %q: %w", field, err)
				}
				for _, r := range refs {
					if len(r.Positions) == 0 {
						continue
					}
					if existing, ok := merged[r.ID]; ok {
						merged[r.ID] = mergeSortedPositions(existing, r.Positions)
					} else {
						merged[r.ID] = append([]uint32(nil), r.Positions...)
					}
				}
			}
			if len(merged) == 0 {
				skipField = true
				break
			}
			tokenPostings[i] = merged
		}
		if skipField {
			continue
		}

		fieldCounts := make(map[DocID]uint32)
		for docID, startPositions := range tokenPostings[0] {
			missing := false
			for i := 1; i < len(tokens); i++ {
				if _, found := tokenPostings[i][docID]; !found {
					missing = true
					break
				}
			}
			if missing {
				continue
			}

			var matches uint32
			for _, p := range startPositions {
				aligned := true
				for i := 1; i < len(tokens); i++ {
					if !containsSortedUint32(tokenPostings[i][docID], p+uint32(i)) {
						aligned = false
						break
					}
				}
				if aligned {
					matches++
				}
			}
			if matches > 0 {
				fieldCounts[docID] = matches
			}
		}

		if len(fieldCounts) == 0 {
			continue
		}

		var fieldStats FieldStats
		if s.scorer != nil {
			fieldStats = FieldStats{
				N:         s.collection.FieldDocCount(field),
				AvgLength: s.collection.AvgDocLen(field),
			}
		}
		df := uint32(len(fieldCounts))
		for docID, cnt := range fieldCounts {
			phraseCounts[docID] += cnt
			if s.scorer != nil {
				ts := TermStats{Field: field, Term: phraseTerm, TF: cnt, DF: df}
				ds := DocStats{ID: docID, Length: s.collection.DocLen(field, docID)}
				scores[docID] += s.scorer.Score(ts, ds, fieldStats)
			}
		}
	}

	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	results := make([]Result, 0, len(phraseCounts))
	for id, cnt := range phraseCounts {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: 1,
			TotalMatches:  int(cnt),
			Score:         scores[id],
		})
	}

	if s.scorer != nil {
		sort.Slice(results, func(i, j int) bool {
			if results[i].Score != results[j].Score {
				return results[i].Score > results[j].Score
			}
			return results[i].ID < results[j].ID
		})
	} else {
		sort.Slice(results, func(i, j int) bool {
			if results[i].TotalMatches != results[j].TotalMatches {
				return results[i].TotalMatches > results[j].TotalMatches
			}
			return results[i].ID < results[j].ID
		})
	}

	totalFound := len(results)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}

	timings["total"] = formatDuration(time.Since(start))

	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
		Timings:           timings,
	}, nil
}

func containsSortedUint32(s []uint32, v uint32) bool {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s[mid] < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(s) && s[lo] == v
}

func mergeSortedPositions(a, b []uint32) []uint32 {
	out := make([]uint32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

func (s *Service) Fields() []string {
	return s.fieldNames()
}

func (s *Service) fieldNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.indexes))
	for k := range s.indexes {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func (s *Service) getOrCreateIndex(name string) (Index, error) {
	s.mu.RLock()
	idx, ok := s.indexes[name]
	s.mu.RUnlock()
	if ok {
		return idx, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if idx, ok := s.indexes[name]; ok {
		return idx, nil
	}
	if s.indexFactory == nil {
		return nil, fmt.Errorf("fts: no index factory configured for field %q", name)
	}
	idx, err := s.indexFactory(name)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, fmt.Errorf("fts: index factory returned nil for field %q", name)
	}
	s.indexes[name] = idx
	return idx, nil
}

func (s *Service) Analyze() (Stats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var combined Stats
	found := false
	for _, idx := range s.indexes {
		analyzer, ok := idx.(Analyzer)
		if !ok {
			continue
		}
		found = true
		combined = mergeStats(combined, analyzer.Analyze())
	}
	return combined, found
}

func (s *Service) SnapshotComponents() (Index, Filter) {
	if s == nil {
		return nil, nil
	}
	s.mu.RLock()
	idx := s.indexes[DefaultField]
	s.mu.RUnlock()
	return idx, s.filter
}

func (s *Service) SnapshotFields() (map[string]Index, Filter) {
	if s == nil {
		return nil, nil
	}
	s.mu.RLock()
	out := make(map[string]Index, len(s.indexes))
	maps.Copy(out, s.indexes)
	s.mu.RUnlock()
	return out, s.filter
}

func (s *Service) BuildFilter() error {
	if s == nil || s.filter == nil {
		return nil
	}

	buildable, ok := s.filter.(BuildableFilter)
	if !ok {
		return nil
	}

	if err := buildable.Build(); err != nil {
		return fmt.Errorf("fts: build filter: %w", err)
	}

	return nil
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}
