package slicedradix

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestSeqAssignedOnFirstInsertion(t *testing.T) {
	idx := New()
	_ = idx.Insert("x", "doc-a")
	_ = idx.Insert("x", "doc-b")
	_ = idx.Insert("x", "doc-c")

	docs, _ := idx.Search("x")
	want := []fts.DocRef{
		{ID: "doc-a", Count: 1, Seq: 0},
		{ID: "doc-b", Count: 1, Seq: 1},
		{ID: "doc-c", Count: 1, Seq: 2},
	}
	for i, d := range docs {
		if d != want[i] {
			t.Fatalf("docs[%d] = %+v, want %+v", i, d, want[i])
		}
	}
}

func TestSeqStableAcrossTerms(t *testing.T) {
	idx := New()
	_ = idx.Insert("foo", "doc-a")
	_ = idx.Insert("bar", "doc-a")
	_ = idx.Insert("foo", "doc-b")

	foo, _ := idx.Search("foo")
	bar, _ := idx.Search("bar")

	if foo[0].Seq != bar[0].Seq {
		t.Fatalf("doc-a has Seq %d in foo but %d in bar", foo[0].Seq, bar[0].Seq)
	}
	if foo[0].Seq != 0 || foo[1].Seq != 1 {
		t.Fatalf("foo Seqs = %d,%d, want 0,1", foo[0].Seq, foo[1].Seq)
	}
}

// ordinals.
func TestSeqUnchangedByTailCheck(t *testing.T) {
	idx := New()
	_ = idx.Insert("hotel", "doc-a")
	_ = idx.Insert("hotel", "doc-a")
	_ = idx.Insert("hotel", "doc-a")
	_ = idx.Insert("hotel", "doc-b")

	docs, _ := idx.Search("hotel")
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].Count != 3 || docs[0].Seq != 0 {
		t.Fatalf("docs[0] = %+v, want Count=3 Seq=0", docs[0])
	}
	if docs[1].Seq != 1 {
		t.Fatalf("docs[1].Seq = %d, want 1", docs[1].Seq)
	}
}

func TestSeqUnchangedByColdPathReindex(t *testing.T) {
	idx := New()
	_ = idx.Insert("x", "doc-a")
	_ = idx.Insert("x", "doc-b")
	_ = idx.Insert("x", "doc-a")

	docs, _ := idx.Search("x")
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].ID != "doc-a" || docs[0].Count != 2 || docs[0].Seq != 0 {
		t.Fatalf("docs[0] = %+v, want {doc-a Count:2 Seq:0}", docs[0])
	}
	if docs[1].Seq != 1 {
		t.Fatalf("docs[1].Seq = %d, want 1", docs[1].Seq)
	}
}

func TestSeqMonotonicInPostings(t *testing.T) {
	idx := New()
	for i, id := range []fts.DocID{"a", "b", "c", "d", "e"} {
		_ = idx.Insert("t", id)
		_ = i
	}
	docs, _ := idx.Search("t")
	var prev uint32
	for i, d := range docs {
		if i > 0 && d.Seq <= prev {
			t.Fatalf("postings not Seq-sorted: docs[%d].Seq=%d, prev=%d", i, d.Seq, prev)
		}
		prev = d.Seq
	}
}

func TestSeqSurvivesSerializeLoad(t *testing.T) {
	idx := New()
	_ = idx.Insert("foo", "doc-a")
	_ = idx.Insert("foo", "doc-b")
	_ = idx.Insert("bar", "doc-a")

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
	if foo[0].Seq != 0 || foo[1].Seq != 1 {
		t.Fatalf("loaded foo Seqs = %d,%d", foo[0].Seq, foo[1].Seq)
	}
	if bar[0].Seq != 0 {
		t.Fatalf("loaded bar[0].Seq = %d, want 0", bar[0].Seq)
	}

	if err := loadedIdx.Insert("foo", "doc-c"); err != nil {
		t.Fatalf("Insert post-load: %v", err)
	}
	foo2, _ := loadedIdx.Search("foo")
	if foo2[2].Seq != 2 {
		t.Fatalf("post-load Seq for doc-c = %d, want 2", foo2[2].Seq)
	}
}

func TestSeqConcurrentIndexingIsUnsafe(t *testing.T) {
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
				docID := fts.DocID(fmt.Sprintf("g%d-d%d", g, d))
				for t := range termsPerDoc {
					term := fmt.Sprintf("term%d", t)
					_ = idx.Insert(term, docID)
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
		var prev uint32
		for i, d := range docs {
			if i > 0 && d.Seq < prev {
				t.Logf("term%d posting is NOT Seq-sorted at index %d: Seq=%d (prev=%d). "+
					"This is the expected outcome under concurrent indexing — "+
					"documented in DocRef.Seq godoc. Single-threaded indexing "+
					"is the supported mode.", term, i, d.Seq, prev)
				return
			}
			prev = d.Seq
		}
	}
	t.Log("All postings happened to stay Seq-sorted this run — race didn't " +
		"surface. Rerun with -count=N to reproduce the interleaving.")
}

func TestSearchPositionalReturnsSharedSlice(t *testing.T) {
	idx := New()
	_ = idx.InsertAt("x", "doc-a", 0)
	_ = idx.InsertAt("x", "doc-a", 5)

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
