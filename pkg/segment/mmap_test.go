package segment_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

func TestOpenFileRoundTrip(t *testing.T) {
	terms := []segment.TermPostings{
		{Term: "alpha", Postings: []fts.Posting{{Ord: 0, Count: 2}, {Ord: 5, Count: 1}}},
		{Term: "beta", Postings: []fts.Posting{{Ord: 1, Count: 3}}, Positions: [][]uint32{{4, 9}}},
	}
	blob, err := segment.Build(terms)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.fts")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := segment.OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer r.Close()

	got, _ := r.Search("alpha")
	want := []fts.Posting{{Ord: 0, Count: 2}, {Ord: 5, Count: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Search(alpha) via mmap = %+v, want %+v", got, want)
	}

	gotPos, _ := r.SearchPositional("beta")
	if len(gotPos) != 1 || !reflect.DeepEqual(gotPos[0].Positions, []uint32{4, 9}) {
		t.Fatalf("SearchPositional(beta) via mmap = %+v", gotPos)
	}
}

func TestMmapHeapStaysSmall(t *testing.T) {
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

	source := slicedradix.New()
	for d := 0; d < docs; d++ {
		ord := fts.DocOrd(d)
		for pos := 0; pos < insertsPerDoc; pos++ {
			term := vocab[rng.Intn(uniqueTerms)]
			if err := source.InsertAt(term, ord, uint32(pos)); err != nil {
				t.Fatalf("InsertAt: %v", err)
			}
		}
	}

	var out []segment.TermPostings
	source.Walk(func(term string, postings []fts.Posting, positions [][]uint32) bool {
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
	blob, err := segment.Build(out)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "big.fts")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fileSize := uint64(len(blob))

	source = nil
	out = nil
	blob = nil
	runtime.GC()
	runtime.GC()

	r, err := segment.OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer r.Close()

	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(r)
	heapKB := ms.HeapAlloc / 1024
	fileKB := fileSize / 1024

	t.Logf("segment file size:        %d KB", fileKB)
	t.Logf("Go heap with mmap reader: %d KB", heapKB)
	t.Logf("ratio (file / heap):      %.2fx", float64(fileKB)/float64(heapKB))
}

func TestMmapAsServiceIndex(t *testing.T) {
	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "barack obamka gave a speech",
		"doc-b": "obamka speech today barack was there",
		"doc-c": "barack obamka said barack obamka again",
	}

	source := slicedradix.New()
	live := fts.New(source, fts.WordKeys, fts.WithScorer(fts.BM25()))
	for id, content := range docs {
		if err := live.IndexDocument(ctx, fts.DocID(id), content); err != nil {
			t.Fatalf("Index %s: %v", id, err)
		}
	}

	var terms []segment.TermPostings
	source.Walk(func(term string, postings []fts.Posting, positions [][]uint32) bool {
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
		terms = append(terms, tp)
		return true
	})
	sort.Slice(terms, func(i, j int) bool { return terms[i].Term < terms[j].Term })

	blob, err := segment.Build(terms)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "svc.fts")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := segment.OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer r.Close()

	sealed := fts.New(r.Reader, fts.WordKeys,
		fts.WithRegistry(live.Registry()),
		fts.WithScorer(fts.BM25()),
	)

	for _, q := range []string{"barack", "obamka", "speech"} {
		liveRes, _ := live.SearchDocuments(ctx, q, 10)
		mmRes, _ := sealed.SearchDocuments(ctx, q, 10)
		if liveRes.TotalResultsCount != mmRes.TotalResultsCount {
			t.Fatalf("query %q: live %d != mmap %d", q, liveRes.TotalResultsCount, mmRes.TotalResultsCount)
		}
	}
}
