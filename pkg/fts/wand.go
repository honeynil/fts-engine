package fts

import (
	"container/heap"
	"context"
	"math"
	"sort"
)

func (s *Service) execBooleanOrWand(
	ctx context.Context,
	q *BooleanQuery,
	topK int,
) (map[DocOrd]docAccum, bool, error) {

	if topK <= 0 {
		return nil, false, nil
	}

	if s.scorer == nil {
		return nil, false, nil
	}

	var shouldTerms []TermQuery
	for _, c := range q.Clauses {
		if c.Query == nil {
			continue
		}
		switch c.Occur {
		case Should:
			tq, ok := termQueryOf(c.Query)
			if !ok {
				return nil, false, nil
			}
			shouldTerms = append(shouldTerms, tq)
		default:
			return nil, false, nil
		}
	}
	if len(shouldTerms) == 0 {
		return nil, false, nil
	}

	clauses := make([]*wandClause, 0, len(shouldTerms))
	for _, tq := range shouldTerms {
		fm, err := s.collectTermPostings(tq)
		if err != nil {
			return nil, false, err
		}
		if fm.totalDocs == 0 {
			continue
		}
		if len(fm.expansions) != 1 {
			return nil, false, nil
		}
		exp := &fm.expansions[0]
		clauses = append(clauses, &wandClause{
			exp:    exp,
			ub:     clauseUpperBound(exp, s),
			cursor: 0,
		})
	}
	if len(clauses) == 0 {
		return map[DocOrd]docAccum{}, true, nil
	}

	h := &topKHeap{}
	heap.Init(h)

	var theta float64

	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		clauses = compactClauses(clauses)
		if len(clauses) == 0 {
			break
		}

		sort.Slice(clauses, func(i, j int) bool {
			return clauses[i].currentOrd() < clauses[j].currentOrd()
		})

		pivot := -1
		var cum float64
		for i, c := range clauses {
			cum += c.ub
			if cum >= theta {
				pivot = i
				break
			}
		}
		if pivot < 0 {
			break
		}

		pivotOrd := clauses[pivot].currentOrd()

		if clauses[0].currentOrd() == pivotOrd {
			var accum docAccum
			matchedOrd := clauses[0].currentOrd()
			for _, c := range clauses {
				if c.currentOrd() != pivotOrd {
					continue
				}
				d := c.currentPosting()
				accum.UniqueMatches++
				accum.TotalMatches += int(d.Count)
				ts := TermStats{Field: c.exp.field, Term: c.exp.term, TF: d.Count, DF: c.exp.df}
				ds := DocStats{Ord: matchedOrd, Length: s.collection.DocLen(c.exp.field, matchedOrd)}
				accum.Score += s.scorer.Score(ts, ds, c.exp.fieldStats)
			}
			if accum.Score > theta || h.Len() < topK {
				heap.Push(h, wandHit{ord: matchedOrd, accum: accum})
				if h.Len() > topK {
					heap.Pop(h)
				}
				if h.Len() == topK {
					theta = (*h)[0].accum.Score
				}
			}
			for _, c := range clauses {
				if c.currentOrd() == pivotOrd {
					c.cursor++
				}
			}
		} else {

			for i := 0; i <= pivot; i++ {
				c := clauses[i]
				if c.currentOrd() < pivotOrd {
					for c.cursor < len(c.exp.postings) && c.exp.postings[c.cursor].Ord < pivotOrd {
						c.cursor++
					}
					break
				}
			}
		}
	}

	out := make(map[DocOrd]docAccum, h.Len())
	for _, hit := range *h {
		out[hit.ord] = hit.accum
	}
	return out, true, nil
}

type wandClause struct {
	exp    *termExpansion
	ub     float64
	cursor int
}

func (c *wandClause) currentPosting() Posting { return c.exp.postings[c.cursor] }
func (c *wandClause) currentOrd() DocOrd      { return c.exp.postings[c.cursor].Ord }
func (c *wandClause) exhausted() bool         { return c.cursor >= len(c.exp.postings) }

func compactClauses(cs []*wandClause) []*wandClause {
	out := cs[:0]
	for _, c := range cs {
		if !c.exhausted() {
			out = append(out, c)
		}
	}
	return out
}

func clauseUpperBound(exp *termExpansion, s *Service) float64 {
	if s.scorer == nil {
		return math.Inf(1)
	}
	var maxTF uint32
	for i := range exp.postings {
		if exp.postings[i].Count > maxTF {
			maxTF = exp.postings[i].Count
		}
	}
	if maxTF == 0 {
		return 0
	}

	ts := TermStats{Field: exp.field, Term: exp.term, TF: maxTF, DF: exp.df}
	ds := DocStats{Ord: 0, Length: 1}
	return s.scorer.Score(ts, ds, exp.fieldStats)
}

type wandHit struct {
	ord   DocOrd
	accum docAccum
}

type topKHeap []wandHit

func (h topKHeap) Len() int           { return len(h) }
func (h topKHeap) Less(i, j int) bool { return h[i].accum.Score < h[j].accum.Score }
func (h topKHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *topKHeap) Push(x any)        { *h = append(*h, x.(wandHit)) }
func (h *topKHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
