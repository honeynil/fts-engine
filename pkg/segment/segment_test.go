package segment

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestRoundTripBasic(t *testing.T) {
	terms := []TermPostings{
		{
			Term:     "alpha",
			Postings: []fts.Posting{{Ord: 0, Count: 2}, {Ord: 5, Count: 1}, {Ord: 99, Count: 7}},
		},
		{
			Term:      "beta",
			Postings:  []fts.Posting{{Ord: 1, Count: 3}, {Ord: 4, Count: 1}},
			Positions: [][]uint32{{0, 5}, {12}},
		},
		{
			Term:     "gamma",
			Postings: []fts.Posting{{Ord: 7, Count: 1}},
		},
	}

	blob, err := Build(terms)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	r, err := Open(blob)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for _, in := range terms {
		got, err := r.Search(in.Term)
		if err != nil {
			t.Fatalf("Search(%q): %v", in.Term, err)
		}
		if !reflect.DeepEqual(got, in.Postings) {
			t.Fatalf("Search(%q) = %+v, want %+v", in.Term, got, in.Postings)
		}
	}

	miss, _ := r.Search("nonexistent")
	if miss != nil {
		t.Fatalf("missing term should return nil, got %+v", miss)
	}
}

func TestRoundTripPositions(t *testing.T) {
	terms := []TermPostings{
		{
			Term:      "obamka",
			Postings:  []fts.Posting{{Ord: 0, Count: 2}, {Ord: 3, Count: 1}, {Ord: 12, Count: 4}},
			Positions: [][]uint32{{1, 7}, {2}, {0, 5, 11, 50}},
		},
	}
	blob, _ := Build(terms)
	r, _ := Open(blob)

	got, err := r.SearchPositional("obamka")
	if err != nil {
		t.Fatalf("SearchPositional: %v", err)
	}
	want := []fts.PositionalPosting{
		{Ord: 0, Positions: []uint32{1, 7}},
		{Ord: 3, Positions: []uint32{2}},
		{Ord: 12, Positions: []uint32{0, 5, 11, 50}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchPositional = %+v, want %+v", got, want)
	}
}

func TestSearchPrefix(t *testing.T) {
	terms := []TermPostings{
		{Term: "banana", Postings: []fts.Posting{{Ord: 0, Count: 1}}},
		{Term: "barack", Postings: []fts.Posting{{Ord: 1, Count: 2}, {Ord: 3, Count: 1}}},
		{Term: "barge", Postings: []fts.Posting{{Ord: 2, Count: 1}}},
		{Term: "obamka", Postings: []fts.Posting{{Ord: 1, Count: 1}}},
	}
	blob, _ := Build(terms)
	r, _ := Open(blob)

	got, err := r.SearchPrefix("ba")
	if err != nil {
		t.Fatalf("SearchPrefix: %v", err)
	}
	gotMap := map[fts.DocOrd]uint32{}
	for _, p := range got {
		gotMap[p.Ord] = p.Count
	}
	wantMap := map[fts.DocOrd]uint32{
		0: 1, // banana
		1: 2, // barack
		2: 1, // barge
		3: 1, // barack
	}
	if !reflect.DeepEqual(gotMap, wantMap) {
		t.Fatalf("SearchPrefix(ba) = %+v, want %+v", gotMap, wantMap)
	}
}

func TestRejectsUnsortedPostings(t *testing.T) {
	_, err := Build([]TermPostings{
		{Term: "x", Postings: []fts.Posting{{Ord: 5}, {Ord: 2}}},
	})
	if err == nil {
		t.Fatal("Build should reject unsorted postings")
	}
}

func TestSegmentHeapVsRadix(t *testing.T) {
	if testing.Short() {
		t.Skip("heap test slow")
	}

	const (
		uniqueTerms   = 500
		docs          = 5000
		insertsPerDoc = 100
	)
	rng := rand.New(rand.NewSource(42))
	vocab := make([]string, uniqueTerms)
	for i := range vocab {
		vocab[i] = fmt.Sprintf("w%04d", i)
	}

	// Build via the same logic slicedradix would: term -> sorted postings.
	type acc struct {
		ord       fts.DocOrd
		count     uint32
		positions []uint32
	}
	byTerm := make(map[string]map[fts.DocOrd]*acc)
	for d := range docs {
		ord := fts.DocOrd(d)
		for pos := range insertsPerDoc {
			term := vocab[rng.Intn(uniqueTerms)]
			perTerm, ok := byTerm[term]
			if !ok {
				perTerm = make(map[fts.DocOrd]*acc)
				byTerm[term] = perTerm
			}
			a, ok := perTerm[ord]
			if !ok {
				a = &acc{ord: ord}
				perTerm[ord] = a
			}
			a.count++
			a.positions = append(a.positions, uint32(pos))
		}
	}

	terms := make([]TermPostings, 0, len(byTerm))
	for term, perTerm := range byTerm {
		ords := make([]fts.DocOrd, 0, len(perTerm))
		for o := range perTerm {
			ords = append(ords, o)
		}
		slices.Sort(ords)
		tp := TermPostings{Term: term, Postings: make([]fts.Posting, len(ords)), Positions: make([][]uint32, len(ords))}
		for i, o := range ords {
			a := perTerm[o]
			tp.Postings[i] = fts.Posting{Ord: o, Count: a.count}
			tp.Positions[i] = a.positions
		}
		terms = append(terms, tp)
	}

	blob, err := Build(terms)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	terms = nil
	byTerm = nil

	r, err := Open(blob)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(r)
	runtime.KeepAlive(blob)
	heapKB := ms.HeapAlloc / 1024
	blobKB := uint64(len(blob)) / 1024

	t.Logf("segment heap (with reader): %d KB", heapKB)
	t.Logf("segment blob size:          %d KB", blobKB)
	t.Logf("term count:                 %d", r.TermCount())
}
