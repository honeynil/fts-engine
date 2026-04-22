package slicedradix

import (
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestIndexInsertAndSearch(t *testing.T) {
	idx := New()

	if err := idx.Insert("hotel", "doc-1"); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].ID != "doc-1" {
		t.Fatalf("doc ID = %q, want %q", docs[0].ID, "doc-1")
	}
}

func TestIndexInsertSameDocIncrementsCount(t *testing.T) {
	idx := New()

	_ = idx.Insert("hotel", "doc-1")
	_ = idx.Insert("hotel", "doc-1")

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].Count != 2 {
		t.Fatalf("doc Count = %d, want 2", docs[0].Count)
	}
}

func TestIndexInsertDifferentDocs(t *testing.T) {
	idx := New()

	_ = idx.Insert("hotel", "doc-1")
	_ = idx.Insert("hotel", "doc-2")

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
}

func TestIndexSearchNotFound(t *testing.T) {
	idx := New()

	docs, err := idx.Search("unknown")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 0 {
		t.Fatalf("len(docs) = %d, want 0", len(docs))
	}
}

func TestIndexAnalyze(t *testing.T) {
	idx := New()
	_ = idx.Insert("hotel", "doc-1")

	stats := idx.Analyze()
	if stats.Nodes == 0 {
		t.Fatalf("stats.Nodes = %d, want > 0", stats.Nodes)
	}
}

func TestInsertAtAndSearchPositional(t *testing.T) {
	idx := New()

	if err := idx.InsertAt("hotel", "doc-1", 0); err != nil {
		t.Fatalf("InsertAt: %v", err)
	}
	if err := idx.InsertAt("hotel", "doc-1", 7); err != nil {
		t.Fatalf("InsertAt: %v", err)
	}
	if err := idx.InsertAt("hotel", "doc-2", 3); err != nil {
		t.Fatalf("InsertAt: %v", err)
	}

	refs, err := idx.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("SearchPositional: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("len(refs) = %d, want 2", len(refs))
	}

	got := map[string][]uint32{}
	for _, r := range refs {
		got[string(r.ID)] = r.Positions
	}
	if want := []uint32{0, 7}; !equalPositions(got["doc-1"], want) {
		t.Fatalf("doc-1 positions = %v, want %v", got["doc-1"], want)
	}
	if want := []uint32{3}; !equalPositions(got["doc-2"], want) {
		t.Fatalf("doc-2 positions = %v, want %v", got["doc-2"], want)
	}
}

func TestSearchPositionalEmptyForNonPositionalInserts(t *testing.T) {
	idx := New()
	_ = idx.Insert("hotel", "doc-1")

	refs, err := idx.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("SearchPositional: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("len(refs) = %d, want 1", len(refs))
	}
	if len(refs[0].Positions) != 0 {
		t.Fatalf("positions from non-positional insert should be empty, got %v", refs[0].Positions)
	}
}

func TestSearchPrefix(t *testing.T) {
	idx := New()
	inserts := map[string][]string{
		"barack": {"doc-a", "doc-a"},
		"banana": {"doc-b"},
		"barge":  {"doc-c"},
		"obama":  {"doc-a"},
		"russia": {"doc-d"},
	}
	for word, docs := range inserts {
		for _, d := range docs {
			if err := idx.Insert(word, fts.DocID(d)); err != nil {
				t.Fatalf("Insert %s %s: %v", word, d, err)
			}
		}
	}

	refs, err := idx.SearchPrefix("ba")
	if err != nil {
		t.Fatalf("SearchPrefix: %v", err)
	}

	got := map[fts.DocID]uint32{}
	for _, r := range refs {
		got[r.ID] = r.Count
	}
	if got["doc-a"] != 2 {
		t.Fatalf("doc-a should have count 2 (barack×2), got %d", got["doc-a"])
	}
	if got["doc-b"] != 1 {
		t.Fatalf("doc-b should have count 1 (banana), got %d", got["doc-b"])
	}
	if got["doc-c"] != 1 {
		t.Fatalf("doc-c should have count 1 (barge), got %d", got["doc-c"])
	}
	if _, ok := got["doc-d"]; ok {
		t.Fatalf("doc-d has no 'ba*' word, got %+v", got)
	}
}

func TestSearchPrefixNoMatch(t *testing.T) {
	idx := New()
	_ = idx.Insert("hello", "doc-1")
	refs, err := idx.SearchPrefix("zzz")
	if err != nil {
		t.Fatalf("SearchPrefix: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("want 0 matches, got %d", len(refs))
	}
}

func TestSearchPrefixExactKey(t *testing.T) {
	idx := New()
	_ = idx.Insert("barack", "doc-1")
	refs, err := idx.SearchPrefix("barack")
	if err != nil {
		t.Fatalf("SearchPrefix: %v", err)
	}
	if len(refs) != 1 || refs[0].ID != "doc-1" {
		t.Fatalf("exact-key prefix should match, got %+v", refs)
	}
}

func equalPositions(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
