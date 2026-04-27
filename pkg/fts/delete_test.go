package fts

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"testing"
)

func TestDeleteRemovesFromSearch(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)
	ctx := context.Background()

	if err := svc.IndexDocument(ctx, "doc-1", "alpha beta"); err != nil {
		t.Fatalf("Index: %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-2", "alpha gamma"); err != nil {
		t.Fatalf("Index: %v", err)
	}

	res, _ := svc.SearchDocuments(ctx, "alpha", 10)
	if res.TotalResultsCount != 2 {
		t.Fatalf("before delete: TotalResultsCount = %d, want 2", res.TotalResultsCount)
	}

	if err := svc.Delete(ctx, "doc-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	res, _ = svc.SearchDocuments(ctx, "alpha", 10)
	if res.TotalResultsCount != 1 {
		t.Fatalf("after delete: TotalResultsCount = %d, want 1", res.TotalResultsCount)
	}
	if res.Results[0].ID != "doc-2" {
		t.Fatalf("after delete: Results[0].ID = %q, want doc-2", res.Results[0].ID)
	}
}

func TestDeleteOfMissingDocIsNoop(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	if err := svc.Delete(context.Background(), "nonexistent"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestDeleteFiltersAcrossQueryTypes(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithScorer(BM25()))
	ctx := context.Background()

	docs := map[string]string{
		// дельта, альфа, бета штрих
		"doc-a": "alpha beta gamma",
		"doc-b": "alpha delta",
		"doc-c": "beta gamma",
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("Index %s: %v", id, err)
		}
	}

	if err := svc.Delete(ctx, "doc-a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// TermQuery
	res, _ := svc.Search(ctx, TermQuery{Term: "alpha"}, 10)
	if len(res.Results) != 1 || res.Results[0].ID != "doc-b" {
		t.Fatalf("TermQuery after delete: %+v, want only doc-b", res.Results)
	}

	// PhraseQuery
	pres, _ := svc.SearchPhrase(ctx, "alpha beta", 10)
	if len(pres.Results) != 0 {
		t.Fatalf("PhraseQuery after delete: %+v, want no matches", pres.Results)
	}

	// BooleanQuery (AND)
	andRes, _ := svc.Search(ctx, &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "beta"}),
		MustClause(TermQuery{Term: "gamma"}),
	}}, 10)
	if len(andRes.Results) != 1 || andRes.Results[0].ID != "doc-c" {
		t.Fatalf("AND after delete: %+v, want only doc-c", andRes.Results)
	}

	// BooleanQuery (OR via WAND, scorer present)
	orRes, _ := svc.Search(ctx, &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "alpha"}),
		ShouldClause(TermQuery{Term: "beta"}),
	}}, 10)
	for _, r := range orRes.Results {
		if r.ID == "doc-a" {
			t.Fatalf("OR after delete leaked doc-a: %+v", orRes.Results)
		}
	}
}

func TestUpdateAssignsFreshOrdAndIsolatesNewContent(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)
	ctx := context.Background()

	if err := svc.IndexDocument(ctx, "doc-1", "old keyword"); err != nil {
		t.Fatalf("Index: %v", err)
	}
	oldOrd, _ := svc.Registry().Has("doc-1")

	if err := svc.UpdateDocument(ctx, "doc-1", "fresh keyword"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	newOrd, _ := svc.Registry().Has("doc-1")

	if oldOrd == newOrd {
		t.Fatalf("Update should assign new ord; got %d for both", oldOrd)
	}

	res, _ := svc.SearchDocuments(ctx, "old", 10)
	if res.TotalResultsCount != 0 {
		t.Fatalf("'old' should not match after update, got %+v", res.Results)
	}

	res, _ = svc.SearchDocuments(ctx, "fresh", 10)
	if res.TotalResultsCount != 1 || res.Results[0].ID != "doc-1" {
		t.Fatalf("'fresh' should match doc-1, got %+v", res.Results)
	}

	res, _ = svc.SearchDocuments(ctx, "keyword", 10)
	if res.TotalResultsCount != 1 || res.Results[0].ID != "doc-1" {
		t.Fatalf("'keyword' should match exactly once, got %+v", res.Results)
	}
}

func TestDeleteUpdatesCollectionStats(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithScorer(BM25()))
	ctx := context.Background()

	_ = svc.Index(ctx, Document{ID: "d1", Fields: map[string]Field{
		"title": {Value: "alpha beta"},
	}})
	_ = svc.Index(ctx, Document{ID: "d2", Fields: map[string]Field{
		"title": {Value: "alpha gamma delta"},
	}})

	if got := svc.collection.FieldDocCount("title"); got != 2 {
		t.Fatalf("before delete: FieldDocCount(title) = %d, want 2", got)
	}

	if err := svc.Delete(ctx, "d1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if got := svc.collection.FieldDocCount("title"); got != 1 {
		t.Fatalf("after delete: FieldDocCount(title) = %d, want 1", got)
	}
	if got := svc.collection.TotalDocs(); got != 1 {
		t.Fatalf("after delete: TotalDocs = %d, want 1", got)
	}
}

func TestTombstonesSurviveSnapshotRoundTrip(t *testing.T) {
	codec := fmt.Sprintf("test-tomb-%s", t.Name())
	if err := RegisterIndexSnapshotCodec(codec,
		func(idx Index, w io.Writer) error { return idx.(Serializable).Serialize(w) },
		func(r io.Reader) (Index, error) {
			out := newSnapshotIndex()
			if err := gob.NewDecoder(r).Decode(&out.data); err != nil {
				return nil, err
			}
			return out, nil
		},
	); err != nil {
		t.Fatalf("register: %v", err)
	}

	idx := newSnapshotIndex()
	svc := New(idx, WordKeys)
	ctx := context.Background()
	_ = svc.IndexDocument(ctx, "doc-1", "alpha")
	_ = svc.IndexDocument(ctx, "doc-2", "alpha")
	_ = svc.Delete(ctx, "doc-1")

	var buf bytes.Buffer
	if err := SaveIndexSnapshotWithTombstones(&buf, codec, idx, svc.Registry(), svc.Tombstones()); err != nil {
		t.Fatalf("SaveIndexSnapshotWithTombstones: %v", err)
	}

	loaded, err := LoadIndexSnapshot(&buf)
	if err != nil {
		t.Fatalf("LoadIndexSnapshot: %v", err)
	}
	if loaded.Tombstones == nil || !loaded.Tombstones.Any() {
		t.Fatalf("loaded tombstones empty; want at least one")
	}

	restored := New(loaded.Index, WordKeys, WithRegistry(loaded.Registry), WithTombstones(loaded.Tombstones))
	res, _ := restored.SearchDocuments(ctx, "alpha", 10)
	if res.TotalResultsCount != 1 || res.Results[0].ID != "doc-2" {
		t.Fatalf("after restore: %+v, want only doc-2", res.Results)
	}
}

func TestUpdateOnUnknownDocIsLikeIndex(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	ctx := context.Background()

	if err := svc.UpdateDocument(ctx, "fresh", "alpha beta"); err != nil {
		t.Fatalf("UpdateDocument: %v", err)
	}

	if _, ok := svc.Registry().Has("fresh"); !ok {
		t.Fatal("Registry missing 'fresh' after Update on unknown doc")
	}
}
