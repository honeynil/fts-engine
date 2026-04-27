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
	postings   []Posting
	byOrd      map[DocOrd]uint32
}

func (e *termExpansion) buildMap() {
	if e.byOrd != nil {
		return
	}
	e.byOrd = make(map[DocOrd]uint32, len(e.postings))
	for _, p := range e.postings {
		e.byOrd[p.Ord] = p.Count
	}
}

func (e *termExpansion) lookup(ord DocOrd) (uint32, bool) {
	if e.byOrd != nil {
		tf, ok := e.byOrd[ord]
		return tf, ok
	}
	for _, p := range e.postings {
		if p.Ord == ord {
			return p.Count, true
		}
	}
	return 0, false
}

type fastMust struct {
	expansions []termExpansion
	totalDocs  int
}

func (m *fastMust) contains(ord DocOrd) bool {
	for i := range m.expansions {
		if _, ok := m.expansions[i].lookup(ord); ok {
			return true
		}
	}
	return false
}

func (s *Service) tryExecBooleanAndFast(ctx context.Context, q *BooleanQuery) (map[DocOrd]docAccum, bool, error) {
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
			return map[DocOrd]docAccum{}, true, nil
		}
		musts = append(musts, fm)
	}

	exclude := make(map[DocOrd]struct{})
	for _, c := range mustNots {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for ord := range child {
			exclude[ord] = struct{}{}
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

	combined := make(map[DocOrd]docAccum, driver.totalDocs)

	for di := range driver.expansions {
		de := &driver.expansions[di]
		for _, d := range de.postings {
			if _, already := combined[d.Ord]; already {
				continue
			}
			if _, skip := exclude[d.Ord]; skip {
				continue
			}
			survives := true
			for i := range others {
				if !others[i].contains(d.Ord) {
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
				ds := DocStats{Ord: d.Ord, Length: s.collection.DocLen(de.field, d.Ord)}
				accum.Score += s.scorer.Score(ts, ds, de.fieldStats)
			}
			for dj := range driver.expansions {
				if dj == di {
					continue
				}
				e2 := &driver.expansions[dj]
				if tf, ok := e2.lookup(d.Ord); ok {
					accum.TotalMatches += int(tf)
					if s.scorer != nil {
						ts := TermStats{Field: e2.field, Term: e2.term, TF: tf, DF: e2.df}
						ds := DocStats{Ord: d.Ord, Length: s.collection.DocLen(e2.field, d.Ord)}
						accum.Score += s.scorer.Score(ts, ds, e2.fieldStats)
					}
				}
			}
			accum.UniqueMatches++

			for i := range others {
				matchedAny := false
				for ej := range others[i].expansions {
					e := &others[i].expansions[ej]
					tf, ok := e.lookup(d.Ord)
					if !ok {
						continue
					}
					matchedAny = true
					accum.TotalMatches += int(tf)
					if s.scorer != nil {
						ts := TermStats{Field: e.field, Term: e.term, TF: tf, DF: e.df}
						ds := DocStats{Ord: d.Ord, Length: s.collection.DocLen(e.field, d.Ord)}
						accum.Score += s.scorer.Score(ts, ds, e.fieldStats)
					}
				}
				if matchedAny {
					accum.UniqueMatches++
				}
			}
			combined[d.Ord] = accum
		}
	}

	for _, c := range shoulds {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for ord, h := range child {
			if existing, ok := combined[ord]; ok {
				combined[ord] = addAccum(existing, h)
			}
		}
	}

	return combined, true, nil
}

func (s *Service) tryExecBooleanOrFast(ctx context.Context, q *BooleanQuery) (map[DocOrd]docAccum, bool, error) {
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

	exclude := make(map[DocOrd]struct{})
	for _, c := range mustNots {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for ord := range child {
			exclude[ord] = struct{}{}
		}
	}

	type resolved struct {
		field      string
		term       string
		df         uint32
		fieldStats FieldStats
		postings   []Posting
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
					postings, err := index.Search(key)
					if err != nil {
						return nil, false, fmt.Errorf("fts: term query field %q: %w", field, err)
					}
					postings = s.filterAlivePostings(postings)
					if len(postings) == 0 {
						continue
					}
					plan = append(plan, resolved{
						field:      field,
						term:       token,
						df:         uint32(len(postings)),
						fieldStats: fieldStats,
						postings:   postings,
						single:     singleClause && isSingleKey,
					})
					totalCap += len(postings)
				}
			}
		}
	}

	if len(plan) == 0 {
		return map[DocOrd]docAccum{}, true, nil
	}

	combined := make(map[DocOrd]docAccum, totalCap)

	var seenInClause map[DocOrd]struct{}
	var prevIsSingle bool
	for i, p := range plan {
		needsSet := !p.single
		if needsSet && (i == 0 || prevIsSingle) {
			seenInClause = make(map[DocOrd]struct{}, len(p.postings))
		}
		prevIsSingle = p.single

		for _, d := range p.postings {
			if _, skip := exclude[d.Ord]; skip {
				continue
			}
			accum := combined[d.Ord]
			if p.single {
				accum.UniqueMatches++
			} else {
				if _, firstHitInClause := seenInClause[d.Ord]; !firstHitInClause {
					accum.UniqueMatches++
					seenInClause[d.Ord] = struct{}{}
				}
			}
			accum.TotalMatches += int(d.Count)
			if s.scorer != nil {
				ts := TermStats{Field: p.field, Term: p.term, TF: d.Count, DF: p.df}
				ds := DocStats{Ord: d.Ord, Length: s.collection.DocLen(p.field, d.Ord)}
				accum.Score += s.scorer.Score(ts, ds, p.fieldStats)
			}
			combined[d.Ord] = accum
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
	exclude map[DocOrd]struct{},
	ctx context.Context,
) (map[DocOrd]docAccum, bool, error) {
	k := len(musts)
	ptrs := make([]int, k)
	exps := make([]*termExpansion, k)
	for i := range musts {
		exps[i] = &musts[i].expansions[0]
		if len(exps[i].postings) == 0 {
			return map[DocOrd]docAccum{}, true, nil
		}
	}

	combined := make(map[DocOrd]docAccum, len(exps[0].postings))

	currentOrd := exps[0].postings[0].Ord
loop:
	for {

		for i := 0; i < k; i++ {
			postings := exps[i].postings
			for ptrs[i] < len(postings) && postings[ptrs[i]].Ord < currentOrd {
				ptrs[i]++
			}
			if ptrs[i] >= len(postings) {
				break loop
			}
			if postings[ptrs[i]].Ord > currentOrd {
				currentOrd = postings[ptrs[i]].Ord
				i = -1
				continue
			}
		}

		ord := exps[0].postings[ptrs[0]].Ord
		if _, skip := exclude[ord]; !skip {
			var accum docAccum
			for i := range k {
				d := exps[i].postings[ptrs[i]]
				accum.UniqueMatches++
				accum.TotalMatches += int(d.Count)
				if s.scorer != nil {
					e := exps[i]
					ts := TermStats{Field: e.field, Term: e.term, TF: d.Count, DF: e.df}
					ds := DocStats{Ord: ord, Length: s.collection.DocLen(e.field, ord)}
					accum.Score += s.scorer.Score(ts, ds, e.fieldStats)
				}
			}
			combined[ord] = accum
		}

		for i := range k {
			ptrs[i]++
			if ptrs[i] >= len(exps[i].postings) {
				break loop
			}
		}
		currentOrd = exps[0].postings[ptrs[0]].Ord
	}

	for _, c := range shoulds {
		child, err := s.executeQuery(ctx, c.Query, 0)
		if err != nil {
			return nil, false, err
		}
		for ord, h := range child {
			if existing, ok := combined[ord]; ok {
				combined[ord] = addAccum(existing, h)
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
				postings, err := index.Search(key)
				if err != nil {
					return fastMust{}, fmt.Errorf("fts: term query field %q: %w", field, err)
				}
				postings = s.filterAlivePostings(postings)
				if len(postings) == 0 {
					continue
				}
				out.expansions = append(out.expansions, termExpansion{
					field:      field,
					term:       token,
					df:         uint32(len(postings)),
					fieldStats: fieldStats,
					postings:   postings,
				})
				out.totalDocs += len(postings)
			}
		}
	}
	return out, nil
}
