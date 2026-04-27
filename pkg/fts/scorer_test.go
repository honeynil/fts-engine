package fts

import (
	"context"
	"math"
	"testing"
)

func TestBM25RareTermScoresHigherThanCommon(t *testing.T) {
	scorer := BM25()

	fieldStats := FieldStats{N: 1000, AvgLength: 20}
	doc := DocStats{Ord: 0, Length: 20}

	rare := scorer.Score(TermStats{Term: "rosa", TF: 1, DF: 3}, doc, fieldStats)
	common := scorer.Score(TermStats{Term: "the", TF: 1, DF: 900}, doc, fieldStats)

	if rare <= common {
		t.Fatalf("BM25: expected rare score > common score, got rare=%v common=%v", rare, common)
	}
}

func TestBM25LengthNormalization(t *testing.T) {
	scorer := BM25()
	fieldStats := FieldStats{N: 100, AvgLength: 50}
	term := TermStats{Term: "x", TF: 2, DF: 10}

	short := scorer.Score(term, DocStats{Ord: 0, Length: 10}, fieldStats)
	long := scorer.Score(term, DocStats{Ord: 1, Length: 200}, fieldStats)

	if short <= long {
		t.Fatalf("BM25: expected short doc > long doc (same TF), got short=%v long=%v", short, long)
	}
}

func TestBM25ZeroInputsSafe(t *testing.T) {
	scorer := BM25()
	if got := scorer.Score(TermStats{TF: 0}, DocStats{}, FieldStats{N: 10, AvgLength: 10}); got != 0 {
		t.Fatalf("TF=0: want 0, got %v", got)
	}
	if got := scorer.Score(TermStats{TF: 1, DF: 0}, DocStats{Length: 10}, FieldStats{N: 10, AvgLength: 10}); got != 0 {
		t.Fatalf("DF=0: want 0, got %v", got)
	}
	if got := scorer.Score(TermStats{TF: 1, DF: 1}, DocStats{Length: 10}, FieldStats{N: 0, AvgLength: 10}); got != 0 {
		t.Fatalf("N=0: want 0, got %v", got)
	}
}

func TestTFIDFMonotonicInTF(t *testing.T) {
	scorer := TFIDF()
	f := FieldStats{N: 100, AvgLength: 10}
	low := scorer.Score(TermStats{TF: 1, DF: 5}, DocStats{Length: 10}, f)
	high := scorer.Score(TermStats{TF: 10, DF: 5}, DocStats{Length: 10}, f)
	if !(high > low) {
		t.Fatalf("TFIDF should grow with TF: low=%v high=%v", low, high)
	}
}

func TestSearchWithBM25RanksByRareness(t *testing.T) {
	factory := func(name string) (Index, error) { return newSnapshotIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithScorer(BM25()))

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "rosa barge",
		"doc-b": "barge barge barge",
	}
	for _, id := range []string{"doc-c", "doc-d", "doc-e", "doc-f", "doc-g", "doc-h", "doc-i", "doc-j"} {
		docs[id] = "barge"
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("index %q: %v", id, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, "rosa barge", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	if len(res.Results) < 2 {
		t.Fatalf("too few results: %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("BM25: expected doc-a first, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("scores must be non-increasing: %v then %v", res.Results[0].Score, res.Results[1].Score)
	}
}

func TestSearchScoreIsZeroWithoutScorer(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	idx.entries["alpha"] = []Posting{docPosting(svc, "a", 1)}

	res, err := svc.SearchDocuments(context.Background(), "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 1 {
		t.Fatalf("want 1 result, got %d", len(res.Results))
	}
	if res.Results[0].Score != 0 {
		t.Fatalf("Score without scorer should stay 0, got %v", res.Results[0].Score)
	}
}

func TestCollectionStatsTracksPerFieldLength(t *testing.T) {

	factory := func(name string) (Index, error) { return newSnapshotIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithScorer(BM25()))

	ctx := context.Background()
	_ = svc.Index(ctx, Document{ID: "d1", Fields: map[string]Field{
		"title": {Value: "a b"},
		"body":  {Value: "x y z w v"},
	}})
	_ = svc.Index(ctx, Document{ID: "d2", Fields: map[string]Field{
		"title": {Value: "a"},
	}})

	if got := svc.collection.FieldDocCount("title"); got != 2 {
		t.Fatalf("title FieldDocCount: want 2, got %d", got)
	}
	if got := svc.collection.FieldDocCount("body"); got != 1 {
		t.Fatalf("body FieldDocCount: want 1, got %d", got)
	}

	wantAvgTitle := (2.0 + 1.0) / 2.0
	if got := svc.collection.AvgDocLen("title"); math.Abs(got-wantAvgTitle) > 1e-9 {
		t.Fatalf("AvgDocLen(title): want %v, got %v", wantAvgTitle, got)
	}
	d1Ord, _ := svc.Registry().Has("d1")
	if got := svc.collection.DocLen("title", d1Ord); got != 2 {
		t.Fatalf("DocLen(title, d1): want 2, got %d", got)
	}
	if got := svc.collection.TotalDocs(); got != 2 {
		t.Fatalf("TotalDocs: want 2, got %d", got)
	}
}

func TestCollectionStatsSkippedWithoutScorer(t *testing.T) {
	factory := func(name string) (Index, error) { return newSnapshotIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	_ = svc.Index(ctx, Document{ID: "d1", Fields: map[string]Field{
		"title": {Value: "a b c"},
	}})

	if got := svc.collection.TotalDocs(); got != 0 {
		t.Fatalf("TotalDocs without scorer should stay 0, got %d", got)
	}
	if got := svc.collection.FieldDocCount("title"); got != 0 {
		t.Fatalf("FieldDocCount without scorer should stay 0, got %d", got)
	}
}
