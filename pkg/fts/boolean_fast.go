package fts

import (
	"context"
	"fmt"
	"sort"
)

const lookupMapThreshold = 50

type termExpansion struct {
	field      string
	term       string
	df         uint32
	fieldStats FieldStats
	docs       []DocRef
	byDoc      map[DocID]uint32
}

func (e *termExpansion) buildMap() {
	if e.byDoc != nil {
		return
	}
	e.byDoc = make(map[DocID]uint32, len(e.docs))
	for _, d := range e.docs {
		e.byDoc[d.ID] = d.Count
	}
}

func (e *termExpansion) lookup(id DocID) (uint32, bool) {
	if e.byDoc != nil {
		tf, ok := e.byDoc[id]
		return tf, ok
	}
	for _, d := range e.docs {
		if d.ID == id {
			return d.Count, true
		}
	}
	return 0, false
}

type fastMust struct {
	expansions []termExpansion
	totalDocs  int // sum of len(expansions.docs), a size proxy for ordering
}

func (m *fastMust) contains(id DocID) bool {
	for i := range m.expansions {
		if _, ok := m.expansions[i].lookup(id); ok {
			return true
		}
	}
	return false
}

func (s *Service) tryExecBooleanAndFast(ctx context.Context, q *BooleanQuery) (map[DocID]docAccum, bool, error) {
	var mustTerms []TermQuery
	var shoulds []BoolClause
	var mustNots []BoolClause
	for _, c := range q.Clauses {
		if c.Query == nil {
			continue
		}
		switch c.Occur {
		case Must:
			tq, ok := termQueryOf(c.Query)
			if !ok {
				return nil, false, nil
			}
			mustTerms = append(mustTerms, tq)
		case Should:
			shoulds = append(shoulds, c)
		case MustNot:
			mustNots = append(mustNots, c)
		}
	}
	if len(mustTerms) == 0 {
		return nil, false, nil
	}

	musts := make([]fastMust, 0, len(mustTerms))
	for _, tq := range mustTerms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		fm, err := s.collectTermPostings(tq)
		if err != nil {
			return nil, false, err
		}
		if fm.totalDocs == 0 {
			return map[DocID]docAccum{}, true, nil
		}
		musts = append(musts, fm)
	}

	exclude := make(map[DocID]struct{})
	for _, c := range mustNots {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for id := range child {
			exclude[id] = struct{}{}
		}
	}

	sort.Slice(musts, func(i, j int) bool { return musts[i].totalDocs < musts[j].totalDocs })

	if allSingleExpansion(musts) {
		return s.execBooleanAndSortMerge(musts, shoulds, exclude, ctx)
	}

	driver := &musts[0]
	others := musts[1:]

	if driver.totalDocs >= lookupMapThreshold {
		for i := range others {
			for j := range others[i].expansions {
				others[i].expansions[j].buildMap()
			}
		}
	}

	combined := make(map[DocID]docAccum, driver.totalDocs)

	for di := range driver.expansions {
		de := &driver.expansions[di]
		for _, d := range de.docs {
			if _, already := combined[d.ID]; already {
				continue
			}
			if _, skip := exclude[d.ID]; skip {
				continue
			}
			survives := true
			for i := range others {
				if !others[i].contains(d.ID) {
					survives = false
					break
				}
			}
			if !survives {
				continue
			}

			var accum docAccum

			accum.TotalMatches += int(d.Count)
			if s.scorer != nil {
				ts := TermStats{Field: de.field, Term: de.term, TF: d.Count, DF: de.df}
				ds := DocStats{ID: d.ID, Length: s.collection.DocLen(de.field, d.ID)}
				accum.Score += s.scorer.Score(ts, ds, de.fieldStats)
			}
			for dj := range driver.expansions {
				if dj == di {
					continue
				}
				e2 := &driver.expansions[dj]
				if tf, ok := e2.lookup(d.ID); ok {
					accum.TotalMatches += int(tf)
					if s.scorer != nil {
						ts := TermStats{Field: e2.field, Term: e2.term, TF: tf, DF: e2.df}
						ds := DocStats{ID: d.ID, Length: s.collection.DocLen(e2.field, d.ID)}
						accum.Score += s.scorer.Score(ts, ds, e2.fieldStats)
					}
				}
			}
			accum.UniqueMatches++

			for i := range others {
				matchedAny := false
				for ej := range others[i].expansions {
					e := &others[i].expansions[ej]
					tf, ok := e.lookup(d.ID)
					if !ok {
						continue
					}
					matchedAny = true
					accum.TotalMatches += int(tf)
					if s.scorer != nil {
						ts := TermStats{Field: e.field, Term: e.term, TF: tf, DF: e.df}
						ds := DocStats{ID: d.ID, Length: s.collection.DocLen(e.field, d.ID)}
						accum.Score += s.scorer.Score(ts, ds, e.fieldStats)
					}
				}
				if matchedAny {
					accum.UniqueMatches++
				}
			}
			combined[d.ID] = accum
		}
	}

	for _, c := range shoulds {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for id, h := range child {
			if existing, ok := combined[id]; ok {
				combined[id] = addAccum(existing, h)
			}
		}
	}

	return combined, true, nil
}

func (s *Service) tryExecBooleanOrFast(ctx context.Context, q *BooleanQuery) (map[DocID]docAccum, bool, error) {
	var shouldTerms []TermQuery
	var mustNots []BoolClause
	for _, c := range q.Clauses {
		if c.Query == nil {
			continue
		}
		switch c.Occur {
		case Must:
			return nil, false, nil
		case Should:
			tq, ok := termQueryOf(c.Query)
			if !ok {
				return nil, false, nil
			}
			shouldTerms = append(shouldTerms, tq)
		case MustNot:
			mustNots = append(mustNots, c)
		}
	}
	if len(shouldTerms) == 0 {
		return nil, false, nil
	}

	exclude := make(map[DocID]struct{})
	for _, c := range mustNots {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for id := range child {
			exclude[id] = struct{}{}
		}
	}

	type resolved struct {
		field      string
		term       string
		df         uint32
		fieldStats FieldStats
		docs       []DocRef
		single     bool
	}
	plan := make([]resolved, 0, len(shouldTerms))
	totalCap := 0

	for _, tq := range shouldTerms {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if tq.Term == "" {
			continue
		}
		tokens := s.pipeline.Process(tq.Term)
		if len(tokens) == 0 {
			continue
		}
		fields := s.resolveFields(tq.Field)
		singleClause := len(tokens) == 1 && len(fields) == 1

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
				keys, err := s.keyGen(token)
				if err != nil {
					return nil, false, fmt.Errorf("fts: term query: keygen: %w", err)
				}
				isSingleKey := len(keys) == 1
				for _, key := range keys {
					if s.filter != nil && !s.filter.Contains([]byte(key)) {
						continue
					}
					docs, err := index.Search(key)
					if err != nil {
						return nil, false, fmt.Errorf("fts: term query field %q: %w", field, err)
					}
					if len(docs) == 0 {
						continue
					}
					plan = append(plan, resolved{
						field:      field,
						term:       token,
						df:         uint32(len(docs)),
						fieldStats: fieldStats,
						docs:       docs,
						single:     singleClause && isSingleKey,
					})
					totalCap += len(docs)
				}
			}
		}
	}

	if len(plan) == 0 {
		return map[DocID]docAccum{}, true, nil
	}

	combined := make(map[DocID]docAccum, totalCap)

	var seenInClause map[DocID]struct{}
	var prevIsSingle bool
	for i, p := range plan {
		needsSet := !p.single
		if needsSet && (i == 0 || prevIsSingle) {
			seenInClause = make(map[DocID]struct{}, len(p.docs))
		}
		prevIsSingle = p.single

		for _, d := range p.docs {
			if _, skip := exclude[d.ID]; skip {
				continue
			}
			accum := combined[d.ID]
			if p.single {
				accum.UniqueMatches++
			} else {
				if _, firstHitInClause := seenInClause[d.ID]; !firstHitInClause {
					accum.UniqueMatches++
					seenInClause[d.ID] = struct{}{}
				}
			}
			accum.TotalMatches += int(d.Count)
			if s.scorer != nil {
				ts := TermStats{Field: p.field, Term: p.term, TF: d.Count, DF: p.df}
				ds := DocStats{ID: d.ID, Length: s.collection.DocLen(p.field, d.ID)}
				accum.Score += s.scorer.Score(ts, ds, p.fieldStats)
			}
			combined[d.ID] = accum
		}
	}

	return combined, true, nil
}

func allSingleExpansion(musts []fastMust) bool {
	for i := range musts {
		if len(musts[i].expansions) != 1 {
			return false
		}
	}
	return true
}

func (s *Service) execBooleanAndSortMerge(
	musts []fastMust,
	shoulds []BoolClause,
	exclude map[DocID]struct{},
	ctx context.Context,
) (map[DocID]docAccum, bool, error) {
	k := len(musts)
	ptrs := make([]int, k)
	exps := make([]*termExpansion, k)
	for i := range musts {
		exps[i] = &musts[i].expansions[0]
		if len(exps[i].docs) == 0 {
			return map[DocID]docAccum{}, true, nil
		}
	}

	combined := make(map[DocID]docAccum, len(exps[0].docs))

	currentSeq := exps[0].docs[0].Seq
loop:
	for {

		for i := 0; i < k; i++ {
			docs := exps[i].docs
			for ptrs[i] < len(docs) && docs[ptrs[i]].Seq < currentSeq {
				ptrs[i]++
			}
			if ptrs[i] >= len(docs) {
				break loop
			}
			if docs[ptrs[i]].Seq > currentSeq {
				currentSeq = docs[ptrs[i]].Seq
				i = -1 // restart the advance sweep
				continue
			}
		}

		docID := exps[0].docs[ptrs[0]].ID
		if _, skip := exclude[docID]; !skip {
			var accum docAccum
			for i := range k {
				d := exps[i].docs[ptrs[i]]
				accum.UniqueMatches++
				accum.TotalMatches += int(d.Count)
				if s.scorer != nil {
					e := exps[i]
					ts := TermStats{Field: e.field, Term: e.term, TF: d.Count, DF: e.df}
					ds := DocStats{ID: docID, Length: s.collection.DocLen(e.field, docID)}
					accum.Score += s.scorer.Score(ts, ds, e.fieldStats)
				}
			}
			combined[docID] = accum
		}

		for i := range k {
			ptrs[i]++
			if ptrs[i] >= len(exps[i].docs) {
				break loop
			}
		}
		currentSeq = exps[0].docs[ptrs[0]].Seq
	}

	for _, c := range shoulds {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for id, h := range child {
			if existing, ok := combined[id]; ok {
				combined[id] = addAccum(existing, h)
			}
		}
	}

	return combined, true, nil
}

func termQueryOf(q Query) (TermQuery, bool) {
	switch v := q.(type) {
	case TermQuery:
		return v, true
	case *TermQuery:
		if v == nil {
			return TermQuery{}, false
		}
		return *v, true
	}
	return TermQuery{}, false
}

func (s *Service) collectTermPostings(q TermQuery) (fastMust, error) {
	var out fastMust
	if q.Term == "" {
		return out, nil
	}
	tokens := s.pipeline.Process(q.Term)
	if len(tokens) == 0 {
		return out, nil
	}

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
			keys, err := s.keyGen(token)
			if err != nil {
				return fastMust{}, fmt.Errorf("fts: term query: keygen: %w", err)
			}
			for _, key := range keys {
				if s.filter != nil && !s.filter.Contains([]byte(key)) {
					continue
				}
				docs, err := index.Search(key)
				if err != nil {
					return fastMust{}, fmt.Errorf("fts: term query field %q: %w", field, err)
				}
				if len(docs) == 0 {
					continue
				}
				out.expansions = append(out.expansions, termExpansion{
					field:      field,
					term:       token,
					df:         uint32(len(docs)),
					fieldStats: fieldStats,
					docs:       docs,
				})
				out.totalDocs += len(docs)
			}
		}
	}
	return out, nil
}
