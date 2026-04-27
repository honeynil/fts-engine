package segment_test

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

// TestSegmentAsServiceIndex exercises the full pipeline: index docs into
// slicedradix -> freeze to segment bytes -> open segment as fts.Index ->
// queries return identical results to the in-memory original.
func TestSegmentAsServiceIndex(t *testing.T) {
	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "barack obamka gave a speech",
		"doc-b": "obamka speech today barack was there",
		"doc-c": "barack obamka said barack obobamkaama again",
		"doc-d": "no match here",
	}

	source := slicedradix.New()
	live := fts.New(source, fts.WordKeys, fts.WithScorer(fts.BM25()))
	for id, content := range docs {
		if err := live.IndexDocument(ctx, fts.DocID(id), content); err != nil {
			t.Fatalf("Index %s: %v", id, err)
		}
	}

	terms := exportSlicedRadix(source)
	blob, err := segment.Build(terms)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	r, err := segment.Open(blob)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	sealed := fts.New(r, fts.WordKeys,
		fts.WithRegistry(live.Registry()),
		fts.WithScorer(fts.BM25()),
	)

	for _, query := range []string{"barack", "obamka", "speech", "match"} {
		liveRes, _ := live.SearchDocuments(ctx, query, 10)
		sealedRes, _ := sealed.SearchDocuments(ctx, query, 10)
		if liveRes.TotalResultsCount != sealedRes.TotalResultsCount {
			t.Fatalf("query %q: live count %d != sealed count %d", query, liveRes.TotalResultsCount, sealedRes.TotalResultsCount)
		}
		liveIDs := map[fts.DocID]bool{}
		for _, r := range liveRes.Results {
			liveIDs[r.ID] = true
		}
		for _, r := range sealedRes.Results {
			if !liveIDs[r.ID] {
				t.Fatalf("query %q: sealed has %q not in live", query, r.ID)
			}
		}
	}

	livePhrase, _ := live.SearchPhrase(ctx, "barack obamka", 10)
	sealedPhrase, _ := sealed.SearchPhrase(ctx, "barack obamka", 10)
	if livePhrase.TotalResultsCount != sealedPhrase.TotalResultsCount {
		t.Fatalf("phrase: live %d != sealed %d", livePhrase.TotalResultsCount, sealedPhrase.TotalResultsCount)
	}
}

func exportSlicedRadix(idx *slicedradix.Index) []segment.TermPostings {
	var out []segment.TermPostings
	idx.Walk(func(term string, postings []fts.Posting, positions [][]uint32) bool {
		tp := segment.TermPostings{
			Term:     term,
			Postings: append([]fts.Posting(nil), postings...),
		}
		if len(positions) > 0 {
			tp.Positions = make([][]uint32, len(positions))
			for i, p := range positions {
				tp.Positions[i] = append([]uint32(nil), p...)
			}
		}
		out = append(out, tp)
		return true
	})
	return out
}
