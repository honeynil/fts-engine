package fts

import (
	"context"
	"sort"
	"testing"
)

type positionalMemoryIndex struct {
	postings  map[string][]Posting
	positions map[string]map[DocOrd][]uint32
}

func newPositionalMemoryIndex() *positionalMemoryIndex {
	return &positionalMemoryIndex{
		postings:  make(map[string][]Posting),
		positions: make(map[string]map[DocOrd][]uint32),
	}
}

func (p *positionalMemoryIndex) Insert(key string, ord DocOrd) error {
	p.bumpCount(key, ord)
	return nil
}

func (p *positionalMemoryIndex) InsertAt(key string, ord DocOrd, pos uint32) error {
	p.bumpCount(key, ord)
	if _, ok := p.positions[key]; !ok {
		p.positions[key] = make(map[DocOrd][]uint32)
	}
	ps := p.positions[key][ord]
	ps = append(ps, pos)
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	p.positions[key][ord] = ps
	return nil
}

func (p *positionalMemoryIndex) bumpCount(key string, ord DocOrd) {
	entries := p.postings[key]
	for i := range entries {
		if entries[i].Ord == ord {
			entries[i].Count++
			p.postings[key] = entries
			return
		}
	}
	p.postings[key] = append(entries, Posting{Ord: ord, Count: 1})
}

func (p *positionalMemoryIndex) Search(key string) ([]Posting, error) {
	return p.postings[key], nil
}

func (p *positionalMemoryIndex) SearchPositional(key string) ([]PositionalPosting, error) {
	entries := p.postings[key]
	out := make([]PositionalPosting, 0, len(entries))
	for _, e := range entries {
		var positions []uint32
		if ps, ok := p.positions[key][e.Ord]; ok {
			positions = append([]uint32(nil), ps...)
		}
		out = append(out, PositionalPosting{Ord: e.Ord, Positions: positions})
	}
	return out, nil
}

func TestSearchPhraseMatchesExactOrder(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "barack obamka gave a speech",
		"doc-b": "obamka speech today barack was there",
		"doc-c": "barack obamka said barack obamka again",
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}

	res, err := svc.SearchPhrase(ctx, "barack obamka", 10)
	if err != nil {
		t.Fatalf("SearchPhrase: %v", err)
	}

	hits := map[DocID]int{}
	for _, r := range res.Results {
		hits[r.ID] = r.TotalMatches
	}

	if _, ok := hits["doc-a"]; !ok {
		t.Fatalf("expected doc-a to match, got %+v", res.Results)
	}
	if _, ok := hits["doc-b"]; ok {
		t.Fatalf("doc-b should NOT match (tokens not adjacent), got %+v", res.Results)
	}
	if hits["doc-c"] != 2 {
		t.Fatalf("doc-c should match twice, got %d (results %+v)", hits["doc-c"], res.Results)
	}
}

func TestSearchPhraseSingleTokenFallsBackToSearch(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "hello world"); err != nil {
		t.Fatalf("index: %v", err)
	}

	res, err := svc.SearchPhrase(ctx, "hello", 10)
	if err != nil {
		t.Fatalf("SearchPhrase: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("single-token phrase should match like a normal query, got %+v", res.Results)
	}
}

func TestSearchPhraseEmptyQuery(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	_ = svc.IndexDocument(ctx, "doc-a", "hello world")

	res, err := svc.SearchPhrase(ctx, "   ", 10)
	if err != nil {
		t.Fatalf("SearchPhrase: %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("empty phrase should return no results, got %+v", res.Results)
	}
}

func TestSearchPhraseSkipsNonPositionalIndexes(t *testing.T) {
	factory := func(name string) (Index, error) { return newMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "barack obamka"); err != nil {
		t.Fatalf("index: %v", err)
	}

	res, err := svc.SearchPhrase(ctx, "barack obamka", 10)
	if err != nil {
		t.Fatalf("SearchPhrase: %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("non-positional index must yield no phrase matches, got %+v", res.Results)
	}
}

func TestSearchPhraseFieldRestrictsToOneField(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	err := svc.Index(ctx, Document{
		ID: "doc-a",
		Fields: map[string]Field{
			"title": {Value: "barack obamka"},
			"body":  {Value: "obamka barack speech"},
		},
	})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	res, err := svc.SearchPhraseField(ctx, "title", "barack obamka", 10)
	if err != nil {
		t.Fatalf("SearchPhraseField: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("title should match, got %+v", res.Results)
	}

	res, err = svc.SearchPhraseField(ctx, "body", "barack obamka", 10)
	if err != nil {
		t.Fatalf("SearchPhraseField: %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("body phrase should NOT match, got %+v", res.Results)
	}
}

func TestSearchPhraseWithBM25Scorer(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithScorer(BM25()))

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "barack obamka spoke",
		"doc-b": "barack obamka barack obamka repeats",
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}
	for i := 0; i < 5; i++ {
		_ = svc.IndexDocument(ctx, DocID("noise-"+string(rune('a'+i))), "unrelated content here")
	}

	res, err := svc.SearchPhrase(ctx, "barack obamka", 10)
	if err != nil {
		t.Fatalf("SearchPhrase: %v", err)
	}
	if len(res.Results) < 2 {
		t.Fatalf("want at least 2 results, got %+v", res.Results)
	}
	if res.Results[0].Score <= 0 {
		t.Fatalf("top result should have positive score, got %v", res.Results[0].Score)
	}
	if res.Results[0].Score < res.Results[1].Score {
		t.Fatalf("scores must be non-increasing: %v, %v", res.Results[0].Score, res.Results[1].Score)
	}
}
