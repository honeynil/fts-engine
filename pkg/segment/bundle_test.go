package segment_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

func TestBundleSaveOpenRoundTrip(t *testing.T) {
	ctx := context.Background()

	source := slicedradix.New()
	live := fts.New(source, fts.WordKeys, fts.WithScorer(fts.BM25()))
	docs := map[string]string{
		"doc-a": "alpha beta gamma",
		"doc-b": "alpha delta",
		"doc-c": "beta gamma",
		"doc-d": "obsolete content",
	}
	for id, text := range docs {
		if err := live.IndexDocument(ctx, fts.DocID(id), text); err != nil {
			t.Fatalf("Index %s: %v", id, err)
		}
	}
	if err := live.Delete(ctx, "doc-d"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.fts")

	if err := segment.SaveBundle(path, source, live.Registry(), live.Tombstones()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	bundle, err := segment.OpenBundle(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer bundle.Close()

	restored := fts.New(bundle.Reader, fts.WordKeys,
		fts.WithRegistry(bundle.Registry),
		fts.WithTombstones(bundle.Tombstones),
		fts.WithScorer(fts.BM25()),
	)

	res, _ := restored.SearchDocuments(ctx, "obsolete", 10)
	if res.TotalResultsCount != 0 {
		t.Fatalf("tombstoned doc resurfaced: %+v", res.Results)
	}

	for _, q := range []string{"alpha", "beta", "gamma", "delta"} {
		liveRes, _ := live.SearchDocuments(ctx, q, 10)
		gotRes, _ := restored.SearchDocuments(ctx, q, 10)
		if liveRes.TotalResultsCount != gotRes.TotalResultsCount {
			t.Fatalf("query %q: live %d != restored %d", q, liveRes.TotalResultsCount, gotRes.TotalResultsCount)
		}
		liveIDs := map[fts.DocID]bool{}
		for _, r := range liveRes.Results {
			liveIDs[r.ID] = true
		}
		for _, r := range gotRes.Results {
			if !liveIDs[r.ID] {
				t.Fatalf("query %q: restored returned %q not in live", q, r.ID)
			}
		}
	}
}

func TestBundleAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.fts")

	source := slicedradix.New()
	if err := source.Insert("hello", 0); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	reg := fts.NewDocRegistry()
	reg.GetOrAssign("doc-0")
	tomb := fts.NewTombstones()

	if err := segment.SaveBundle(path, source, reg, tomb); err != nil {
		t.Fatalf("Save: %v", err)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("leftover tmp file: %s", e.Name())
		}
	}

	b, err := segment.OpenBundle(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer b.Close()

	got, _ := b.Reader.Search("hello")
	if len(got) != 1 || got[0].Ord != 0 {
		t.Fatalf("Search(hello) = %+v", got)
	}
}

func TestBundleEmptyRegistryAndTombstones(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.fts")

	source := slicedradix.New()
	_ = source.Insert("x", 0)

	if err := segment.SaveBundle(path, source, nil, nil); err != nil {
		t.Fatalf("Save: %v", err)
	}

	b, err := segment.OpenBundle(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer b.Close()

	if b.Registry == nil || b.Registry.Len() != 0 {
		t.Fatalf("Registry must be non-nil and empty when nil supplied; got %v", b.Registry)
	}
	if b.Tombstones == nil || b.Tombstones.Any() {
		t.Fatalf("Tombstones must be non-nil and empty; got %+v", b.Tombstones)
	}
}

func TestBundleRejectsMalformedFile(t *testing.T) {
	if _, err := segment.OpenBundleBytes([]byte("garbage")); err == nil {
		t.Fatal("expected error on too-short blob")
	}
	bad := make([]byte, 32)
	copy(bad[:4], "XXXX")
	if _, err := segment.OpenBundleBytes(bad); err == nil {
		t.Fatal("expected error on bad magic")
	}
}
