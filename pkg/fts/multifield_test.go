package fts

import (
	"context"
	"fmt"
	"testing"
)

func TestIndexMultiFieldPopulatesPerFieldIndex(t *testing.T) {
	factories := map[string]*memoryIndex{}
	factory := func(name string) (Index, error) {
		idx := newMemoryIndex()
		factories[name] = idx
		return idx, nil
	}

	svc := NewMultiField(factory, WordKeys)

	doc := Document{
		ID: "doc-1",
		Fields: map[string]Field{
			"title": {Value: "rosa"},
			"body":  {Value: "barge"},
		},
	}
	if err := svc.Index(context.Background(), doc); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	if got := len(factories["title"].inserts); got != 1 {
		t.Fatalf("title inserts = %d, want 1", got)
	}
	if got := len(factories["body"].inserts); got != 1 {
		t.Fatalf("body inserts = %d, want 1", got)
	}
	if factories["title"].inserts[0].key != "rosa" {
		t.Fatalf("title got key %q, want %q", factories["title"].inserts[0].key, "rosa")
	}
}

func TestIndexDocumentStillWorksAsSugar(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	if err := svc.IndexDocument(context.Background(), "doc-1", "alpha"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}
	if len(idx.inserts) != 1 || idx.inserts[0].key != "alpha" {
		t.Fatalf("legacy IndexDocument did not populate default field: %+v", idx.inserts)
	}

	fields := svc.Fields()
	if len(fields) != 1 || fields[0] != DefaultField {
		t.Fatalf("Fields() = %v, want [_default]", fields)
	}
}

func TestSingleFieldServiceRejectsOtherFields(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	err := svc.Index(context.Background(), Document{
		ID:     "doc-1",
		Fields: map[string]Field{"title": {Value: "oops"}},
	})
	if err == nil {
		t.Fatal("expected error for unknown field on single-field service")
	}
}

func TestSearchDocumentsORAcrossFields(t *testing.T) {
	titleIdx := newMemoryIndex()
	bodyIdx := newMemoryIndex()

	factory := func(name string) (Index, error) {
		switch name {
		case "title":
			return titleIdx, nil
		case "body":
			return bodyIdx, nil
		}
		return nil, fmt.Errorf("unexpected field %q", name)
	}
	svc := NewMultiField(factory, WordKeys)

	titleIdx.entries["alpha"] = []Posting{docPosting(svc, "a", 2)}
	bodyIdx.entries["beta"] = []Posting{docPosting(svc, "b", 3), docPosting(svc, "a", 1)}

	if err := svc.Index(context.Background(), Document{ID: "seed", Fields: map[string]Field{
		"title": {Value: "alpha"},
		"body":  {Value: "beta"},
	}}); err != nil {
		t.Fatalf("seed Index() error = %v", err)
	}

	res, err := svc.SearchDocuments(context.Background(), "alpha beta", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	if res.TotalResultsCount < 2 {
		t.Fatalf("expected matches across both fields, got %+v", res.Results)
	}

	ids := map[DocID]bool{}
	for _, r := range res.Results {
		ids[r.ID] = true
	}
	if !ids["a"] || !ids["b"] {
		t.Fatalf("expected both 'a' and 'b' in results, got %+v", res.Results)
	}
}

func TestSearchFieldRestrictsToOneField(t *testing.T) {
	titleIdx := newMemoryIndex()
	bodyIdx := newMemoryIndex()

	factory := func(name string) (Index, error) {
		switch name {
		case "title":
			return titleIdx, nil
		case "body":
			return bodyIdx, nil
		}
		return nil, fmt.Errorf("unexpected field %q", name)
	}
	svc := NewMultiField(factory, WordKeys)
	titleIdx.entries["alpha"] = []Posting{docPosting(svc, "a", 1)}
	bodyIdx.entries["alpha"] = []Posting{docPosting(svc, "b", 1)}
	_ = svc.Index(context.Background(), Document{ID: "seed", Fields: map[string]Field{
		"title": {Value: "alpha"},
		"body":  {Value: "alpha"},
	}})

	res, err := svc.SearchField(context.Background(), "title", "alpha", 10)
	if err != nil {
		t.Fatalf("SearchField() error = %v", err)
	}
	if res.TotalResultsCount != 1 || res.Results[0].ID != "a" {
		t.Fatalf("title-only search: want [a], got %+v", res.Results)
	}

	res, err = svc.SearchField(context.Background(), "nowhere", "alpha", 10)
	if err != nil {
		t.Fatalf("SearchField() on missing field error = %v", err)
	}
	if res.TotalResultsCount != 0 {
		t.Fatalf("missing field: want 0, got %d", res.TotalResultsCount)
	}
}

type uppercasePipeline struct{}

func (uppercasePipeline) Process(text string) []string {
	out := make([]rune, 0, len(text))
	for _, r := range text {
		if r >= 'a' && r <= 'z' {
			out = append(out, r-32)
		} else if r >= 'A' && r <= 'Z' {
			out = append(out, r)
		}
	}
	return []string{string(out)}
}

func TestPerFieldPipelineOverridesDefault(t *testing.T) {
	idx := newMemoryIndex()
	svc := NewMultiField(
		func(name string) (Index, error) { return idx, nil },
		WordKeys,
	)

	err := svc.Index(context.Background(), Document{
		ID: "doc-1",
		Fields: map[string]Field{
			"title": {Value: "abc", Pipeline: uppercasePipeline{}},
		},
	})
	if err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	if len(idx.inserts) != 1 || idx.inserts[0].key != "ABC" {
		t.Fatalf("per-field pipeline not applied: %+v", idx.inserts)
	}
}

func TestIndexEmptyDocumentRejected(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	if err := svc.Index(context.Background(), Document{ID: "x"}); err == nil {
		t.Fatal("expected error for document with no fields")
	}
	if err := svc.Index(context.Background(), Document{Fields: map[string]Field{DefaultField: {Value: "a"}}}); err == nil {
		t.Fatal("expected error for empty doc id")
	}
}

func TestNewMultiFieldFromIndexes(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()

	registry := NewDocRegistry()
	aOrd := registry.GetOrAssign("a")
	bOrd := registry.GetOrAssign("b")
	title.entries["alpha"] = []Posting{{Ord: aOrd, Count: 1}}
	body.entries["beta"] = []Posting{{Ord: bOrd, Count: 1}}

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithRegistry(registry))

	got := svc.Fields()
	if len(got) != 2 {
		t.Fatalf("Fields() = %v, want 2 entries", got)
	}

	res, err := svc.SearchField(context.Background(), "title", "alpha", 10)
	if err != nil || res.TotalResultsCount != 1 {
		t.Fatalf("SearchField title: err=%v, res=%+v", err, res)
	}

	err = svc.Index(context.Background(), Document{
		ID:     "x",
		Fields: map[string]Field{"new_field": {Value: "z"}},
	})
	if err == nil {
		t.Fatal("expected error indexing an unknown field on restored service")
	}
}
