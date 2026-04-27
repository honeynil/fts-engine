package slicedradix

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestOrdAssignedOnFirstInsertion(t *testing.T) {
	idx := New()
	_ = idx.Insert("x", 0)
	_ = idx.Insert("x", 1)
	_ = idx.Insert("x", 2)

	docs, _ := idx.Search("x")
	want := []fts.Posting{
		{Ord: 0, Count: 1},
		{Ord: 1, Count: 1},
		{Ord: 2, Count: 1},
	}
	for i, d := range docs {
		if d != want[i] {
			t.Fatalf("docs[%d] = %+v, want %+v", i, d, want[i])
		}
	}
}

func TestOrdStableAcrossTerms(t *testing.T) {
	idx := New()
	_ = idx.Insert("foo", 0)
	_ = idx.Insert("bar", 0)
	_ = idx.Insert("foo", 1)

	foo, _ := idx.Search("foo")
	bar, _ := idx.Search("bar")

	if foo[0].Ord != bar[0].Ord {
		t.Fatalf("ord-0 has Ord %d in foo but %d in bar", foo[0].Ord, bar[0].Ord)
	}
	if foo[0].Ord != 0 || foo[1].Ord != 1 {
		t.Fatalf("foo Ords = %d,%d, want 0,1", foo[0].Ord, foo[1].Ord)
	}
}

func TestOrdUnchangedByTailCheck(t *testing.T) {
	idx := New()
	_ = idx.Insert("hotel", 0)
	_ = idx.Insert("hotel", 0)
	_ = idx.Insert("hotel", 0)
	_ = idx.Insert("hotel", 1)

	docs, _ := idx.Search("hotel")
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].Count != 3 || docs[0].Ord != 0 {
		t.Fatalf("docs[0] = %+v, want Count=3 Ord=0", docs[0])
	}
	if docs[1].Ord != 1 {
		t.Fatalf("docs[1].Ord = %d, want 1", docs[1].Ord)
	}
}

func TestOrdUnchangedByColdPathReindex(t *testing.T) {
	idx := New()
	_ = idx.Insert("x", 0)
	_ = idx.Insert("x", 1)
	_ = idx.Insert("x", 0)

	docs, _ := idx.Search("x")
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].Ord != 0 || docs[0].Count != 2 {
		t.Fatalf("docs[0] = %+v, want {Ord:0 Count:2}", docs[0])
	}
	if docs[1].Ord != 1 {
		t.Fatalf("docs[1].Ord = %d, want 1", docs[1].Ord)
	}
}

func TestOrdMonotonicInPostings(t *testing.T) {
	idx := New()
	for o := range []fts.DocOrd{0, 1, 2, 3, 4} {
		_ = idx.Insert("t", fts.DocOrd(o))
	}
	docs, _ := idx.Search("t")
	var prev fts.DocOrd
	for i, d := range docs {
		if i > 0 && d.Ord <= prev {
			t.Fatalf("postings not Ord-sorted: docs[%d].Ord=%d, prev=%d", i, d.Ord, prev)
		}
		prev = d.Ord
	}
}

func TestOrdSurvivesSerializeLoad(t *testing.T) {
	idx := New()
	_ = idx.Insert("foo", 0)
	_ = idx.Insert("foo", 1)
	_ = idx.Insert("bar", 0)

	var buf bytes.Buffer
	if err := idx.Serialize(&buf); err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	loadedIdx := loaded.(*Index)
	foo, _ := loadedIdx.Search("foo")
	bar, _ := loadedIdx.Search("bar")
	if foo[0].Ord != 0 || foo[1].Ord != 1 {
		t.Fatalf("loaded foo Ords = %d,%d", foo[0].Ord, foo[1].Ord)
	}
	if bar[0].Ord != 0 {
		t.Fatalf("loaded bar[0].Ord = %d, want 0", bar[0].Ord)
	}

	if err := loadedIdx.Insert("foo", 2); err != nil {
		t.Fatalf("Insert post-load: %v", err)
	}
	foo2, _ := loadedIdx.Search("foo")
	if foo2[2].Ord != 2 {
		t.Fatalf("post-load Ord for new posting = %d, want 2", foo2[2].Ord)
	}
}

func TestConcurrentIndexingIsUnsafe(t *testing.T) {
	const (
		goroutines  = 8
		docsPerG    = 50
		termsPerDoc = 4
	)

	idx := New()
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for d := range docsPerG {
				ord := fts.DocOrd(g*docsPerG + d)
				for t := range termsPerDoc {
					term := fmt.Sprintf("term%d", t)
					_ = idx.Insert(term, ord)
				}
			}
		}(g)
	}
	wg.Wait()

	for term := range termsPerDoc {
		docs, err := idx.Search(fmt.Sprintf("term%d", term))
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		var prev fts.DocOrd
		for i, d := range docs {
			if i > 0 && d.Ord < prev {
				t.Logf("term%d posting is NOT Ord-sorted at index %d: Ord=%d (prev=%d). "+
					"Single-threaded indexing is the supported mode.", term, i, d.Ord, prev)
				return
			}
			prev = d.Ord
		}
	}
	t.Log("All postings happened to stay Ord-sorted this run.")
}

func TestSearchPositionalReturnsSharedSlice(t *testing.T) {
	idx := New()
	_ = idx.InsertAt("x", 1, 0)
	_ = idx.InsertAt("x", 1, 5)

	refs, err := idx.SearchPositional("x")
	if err != nil {
		t.Fatalf("SearchPositional: %v", err)
	}
	if len(refs) != 1 || len(refs[0].Positions) != 2 {
		t.Fatalf("refs = %+v, want 1 ref with 2 positions", refs)
	}

	firstCall := refs[0].Positions
	again, _ := idx.SearchPositional("x")
	secondCall := again[0].Positions

	if &firstCall[0] != &secondCall[0] {
		t.Fatalf("SearchPositional is copying positions — shared-slice contract violated")
	}
}
