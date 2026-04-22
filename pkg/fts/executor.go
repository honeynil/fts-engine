package fts

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type docAccum struct {
	UniqueMatches int
	TotalMatches  int
	Score         float64
}

func addAccum(a, b docAccum) docAccum {
	return docAccum{
		UniqueMatches: a.UniqueMatches + b.UniqueMatches,
		TotalMatches:  a.TotalMatches + b.TotalMatches,
		Score:         a.Score + b.Score,
	}
}

func (s *Service) Search(ctx context.Context, q Query, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q == nil {
		return &SearchResult{Results: []Result{}, Timings: map[string]string{}}, nil
	}

	start := time.Now()
	timings := make(map[string]string, 2)

	searchStart := time.Now()
	hits, err := s.executeQuery(ctx, q, maxResults)
	if err != nil {
		return nil, err
	}
	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	results := make([]Result, 0, len(hits))
	for id, h := range hits {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: h.UniqueMatches,
			TotalMatches:  h.TotalMatches,
			Score:         h.Score,
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

func (s *Service) executeQuery(ctx context.Context, q Query, topK int) (map[DocID]docAccum, error) {
	switch t := q.(type) {
	case TermQuery:
		return s.execTerm(ctx, t)
	case *TermQuery:
		return s.execTerm(ctx, *t)
	case PhraseQuery:
		return s.execPhrase(ctx, t)
	case *PhraseQuery:
		return s.execPhrase(ctx, *t)
	case PrefixQuery:
		return s.execPrefix(ctx, t)
	case *PrefixQuery:
		return s.execPrefix(ctx, *t)
	case *BooleanQuery:
		return s.execBoolean(ctx, t, topK)
	default:
		return nil, fmt.Errorf("fts: unsupported query type %T", q)
	}
}

func (s *Service) resolveFields(explicit string) []string {
	if explicit != "" {
		return []string{explicit}
	}
	return s.fieldNames()
}

func (s *Service) execTerm(ctx context.Context, q TermQuery) (map[DocID]docAccum, error) {
	if q.Term == "" {
		return map[DocID]docAccum{}, nil
	}

	tokens := s.pipeline.Process(q.Term)
	if len(tokens) == 0 {
		return map[DocID]docAccum{}, nil
	}

	var hits map[DocID]docAccum

	for _, field := range s.resolveFields(q.Field) {
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
				return nil, fmt.Errorf("fts: term query: keygen: %w", err)
			}

			for _, key := range keys {
				if s.filter != nil && !s.filter.Contains([]byte(key)) {
					continue
				}
				docs, err := index.Search(key)
				if err != nil {
					return nil, fmt.Errorf("fts: term query field %q: %w", field, err)
				}
				if hits == nil {
					hits = make(map[DocID]docAccum, len(docs))
				}
				df := uint32(len(docs))
				for _, doc := range docs {
					accum := hits[doc.ID]
					accum.UniqueMatches++
					accum.TotalMatches += int(doc.Count)
					if s.scorer != nil {
						ts := TermStats{Field: field, Term: token, TF: doc.Count, DF: df}
						ds := DocStats{ID: doc.ID, Length: s.collection.DocLen(field, doc.ID)}
						accum.Score += s.scorer.Score(ts, ds, fieldStats)
					}
					hits[doc.ID] = accum
				}
			}
		}
	}
	if hits == nil {
		hits = map[DocID]docAccum{}
	}
	return hits, nil
}

func (s *Service) execPhrase(ctx context.Context, q PhraseQuery) (map[DocID]docAccum, error) {
	tokens := s.pipeline.Process(q.Phrase)
	if len(tokens) == 0 {
		return map[DocID]docAccum{}, nil
	}
	if len(tokens) == 1 {
		return s.execTerm(ctx, TermQuery{Field: q.Field, Term: tokens[0]})
	}

	phraseTerm := strings.Join(tokens, " ")
	hits := make(map[DocID]docAccum)

	for _, field := range s.resolveFields(q.Field) {
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
		skip := false
		for i, token := range tokens {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			keys, err := s.keyGen(token)
			if err != nil {
				return nil, fmt.Errorf("fts: phrase query: keygen: %w", err)
			}
			var merged map[DocID][]uint32
			if len(keys) == 1 {
				if s.filter != nil && !s.filter.Contains([]byte(keys[0])) {
					merged = nil
				} else {
					refs, err := positional.SearchPositional(keys[0])
					if err != nil {
						return nil, fmt.Errorf("fts: phrase query field %q: %w", field, err)
					}
					merged = make(map[DocID][]uint32, len(refs))
					for _, r := range refs {
						if len(r.Positions) > 0 {
							merged[r.ID] = r.Positions
						}
					}
				}
			} else {
				merged = make(map[DocID][]uint32)
				for _, key := range keys {
					if s.filter != nil && !s.filter.Contains([]byte(key)) {
						continue
					}
					refs, err := positional.SearchPositional(key)
					if err != nil {
						return nil, fmt.Errorf("fts: phrase query field %q: %w", field, err)
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
			}
			if len(merged) == 0 {
				skip = true
				break
			}
			tokenPostings[i] = merged
		}
		if skip {
			continue
		}

		driverIdx := 0
		for i := 1; i < len(tokenPostings); i++ {
			if len(tokenPostings[i]) < len(tokenPostings[driverIdx]) {
				driverIdx = i
			}
		}

		fieldCounts := make(map[DocID]uint32)
		for docID, driverPositions := range tokenPostings[driverIdx] {
			missing := false
			for i := range tokens {
				if i == driverIdx {
					continue
				}
				if _, ok := tokenPostings[i][docID]; !ok {
					missing = true
					break
				}
			}
			if missing {
				continue
			}
			matches := phraseAlign(tokenPostings, docID, driverIdx, driverPositions)
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
			accum := hits[docID]
			accum.UniqueMatches++
			accum.TotalMatches += int(cnt)
			if s.scorer != nil {
				ts := TermStats{Field: field, Term: phraseTerm, TF: cnt, DF: df}
				ds := DocStats{ID: docID, Length: s.collection.DocLen(field, docID)}
				accum.Score += s.scorer.Score(ts, ds, fieldStats)
			}
			hits[docID] = accum
		}
	}
	return hits, nil
}

func (s *Service) execPrefix(ctx context.Context, q PrefixQuery) (map[DocID]docAccum, error) {
	if q.Prefix == "" {
		return map[DocID]docAccum{}, nil
	}

	hits := make(map[DocID]docAccum)
	for _, field := range s.resolveFields(q.Field) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.RLock()
		index, ok := s.indexes[field]
		s.mu.RUnlock()
		if !ok {
			continue
		}
		prefixer, ok := index.(PrefixIndex)
		if !ok {
			continue
		}

		docs, err := prefixer.SearchPrefix(q.Prefix)
		if err != nil {
			return nil, fmt.Errorf("fts: prefix query field %q: %w", field, err)
		}
		if len(docs) == 0 {
			continue
		}

		var fieldStats FieldStats
		if s.scorer != nil {
			fieldStats = FieldStats{
				N:         s.collection.FieldDocCount(field),
				AvgLength: s.collection.AvgDocLen(field),
			}
		}
		df := uint32(len(docs))
		for _, doc := range docs {
			accum := hits[doc.ID]
			accum.UniqueMatches++
			accum.TotalMatches += int(doc.Count)
			if s.scorer != nil {
				ts := TermStats{Field: field, Term: q.Prefix + "*", TF: doc.Count, DF: df}
				ds := DocStats{ID: doc.ID, Length: s.collection.DocLen(field, doc.ID)}
				accum.Score += s.scorer.Score(ts, ds, fieldStats)
			}
			hits[doc.ID] = accum
		}
	}
	return hits, nil
}

func (s *Service) execBoolean(ctx context.Context, q *BooleanQuery, topK int) (map[DocID]docAccum, error) {

	if q == nil || len(q.Clauses) == 0 {
		return map[DocID]docAccum{}, nil
	}

	if res, ok, err := s.tryExecBooleanAndFast(ctx, q); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}

	if res, ok, err := s.execBooleanOrWand(ctx, q, topK); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}

	if res, ok, err := s.tryExecBooleanOrFast(ctx, q); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}

	var musts []map[DocID]docAccum
	var shoulds []map[DocID]docAccum
	exclude := make(map[DocID]struct{})

	for _, c := range q.Clauses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c.Query == nil {
			continue
		}
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, err
		}
		switch c.Occur {
		case Must:
			musts = append(musts, child)
		case Should:
			shoulds = append(shoulds, child)
		case MustNot:
			for id := range child {
				exclude[id] = struct{}{}
			}
		}
	}

	combined := make(map[DocID]docAccum)

	if len(musts) > 0 {
		sort.Slice(musts, func(i, j int) bool { return len(musts[i]) < len(musts[j]) })
		for id, h := range musts[0] {
			if _, skip := exclude[id]; skip {
				continue
			}
			accum := h
			ok := true
			for _, other := range musts[1:] {
				oh, found := other[id]
				if !found {
					ok = false
					break
				}
				accum = addAccum(accum, oh)
			}
			if ok {
				combined[id] = accum
			}
		}
		for _, sh := range shoulds {
			for id, h := range sh {
				if existing, ok := combined[id]; ok {
					combined[id] = addAccum(existing, h)
				}
			}
		}
	} else {
		for _, sh := range shoulds {
			for id, h := range sh {
				if _, skip := exclude[id]; skip {
					continue
				}
				combined[id] = addAccum(combined[id], h)
			}
		}
	}

	return combined, nil
}

func phraseAlign(tokenPostings []map[DocID][]uint32, docID DocID, driverIdx int, driverPositions []uint32) uint32 {
	n := len(tokenPostings)
	if n == 0 {
		return 0
	}
	others := make([][]uint32, n)
	ptrs := make([]int, n)
	for i := range n {
		if i == driverIdx {
			continue
		}
		others[i] = tokenPostings[i][docID]
		if len(others[i]) == 0 {
			return 0
		}
	}

	var matches uint32
outer:
	for _, p := range driverPositions {
		for i := range n {
			if i == driverIdx {
				continue
			}
			target := p + uint32(i) - uint32(driverIdx)
			if i < driverIdx && uint32(driverIdx-i) > p {
				continue outer
			}
			pos := others[i]
			j := ptrs[i]
			for j < len(pos) && pos[j] < target {
				j++
			}
			ptrs[i] = j
			if j >= len(pos) {
				return matches // this token exhausted; no more phrase hits possible
			}
			if pos[j] != target {
				continue outer
			}
		}
		matches++
	}
	return matches
}
