package slicedradix

import (
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestIndexInsertAndSearch(t *testing.T) {
	idx := New()

	if err := idx.Insert("hotel", 1); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].Ord != 1 {
		t.Fatalf("doc Ord = %d, want 1", docs[0].Ord)
	}
}

func TestIndexInsertSameDocIncrementsCount(t *testing.T) {
	idx := New()

	_ = idx.Insert("hotel", 1)
	_ = idx.Insert("hotel", 1)

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

	_ = idx.Insert("hotel", 1)
	_ = idx.Insert("hotel", 2)

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
	_ = idx.Insert("hotel", 1)

	stats := idx.Analyze()
	if stats.Nodes == 0 {
		t.Fatalf("stats.Nodes = %d, want > 0", stats.Nodes)
	}
}

func TestInsertAtAndSearchPositional(t *testing.T) {
	idx := New()

	if err := idx.InsertAt("hotel", 1, 0); err != nil {
		t.Fatalf("InsertAt: %v", err)
	}
	if err := idx.InsertAt("hotel", 1, 7); err != nil {
		t.Fatalf("InsertAt: %v", err)
	}
	if err := idx.InsertAt("hotel", 2, 3); err != nil {
		t.Fatalf("InsertAt: %v", err)
	}

	refs, err := idx.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("SearchPositional: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("len(refs) = %d, want 2", len(refs))
	}

	got := map[fts.DocOrd][]uint32{}
	for _, r := range refs {
		got[r.Ord] = r.Positions
	}
	if want := []uint32{0, 7}; !equalPositions(got[1], want) {
		t.Fatalf("ord=1 positions = %v, want %v", got[1], want)
	}
	if want := []uint32{3}; !equalPositions(got[2], want) {
		t.Fatalf("ord=2 positions = %v, want %v", got[2], want)
	}
}

func TestSearchPositionalEmptyForNonPositionalInserts(t *testing.T) {
	idx := New()
	_ = idx.Insert("hotel", 1)

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
	inserts := map[string][]fts.DocOrd{
		"barack": {1, 1},
		"banana": {2},
		"barge":  {3},
		"obamka": {1},
		"russia": {4},
	}
	for word, ords := range inserts {
		for _, o := range ords {
			if err := idx.Insert(word, o); err != nil {
				t.Fatalf("Insert %s %d: %v", word, o, err)
			}
		}
	}

	refs, err := idx.SearchPrefix("ba")
	if err != nil {
		t.Fatalf("SearchPrefix: %v", err)
	}

	got := map[fts.DocOrd]uint32{}
	for _, r := range refs {
		got[r.Ord] = r.Count
	}
	if got[1] != 2 {
		t.Fatalf("ord=1 should have count 2 (barack×2), got %d", got[1])
	}
	if got[2] != 1 {
		t.Fatalf("ord=2 should have count 1 (banana), got %d", got[2])
	}
	if got[3] != 1 {
		t.Fatalf("ord=3 should have count 1 (barge), got %d", got[3])
	}
	if _, ok := got[4]; ok {
		t.Fatalf("ord=4 has no 'ba*' word, got %+v", got)
	}
}

func TestSearchPrefixNoMatch(t *testing.T) {
	idx := New()
	_ = idx.Insert("hello", 1)
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
	_ = idx.Insert("barack", 1)
	refs, err := idx.SearchPrefix("barack")
	if err != nil {
		t.Fatalf("SearchPrefix: %v", err)
	}
	if len(refs) != 1 || refs[0].Ord != 1 {
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
