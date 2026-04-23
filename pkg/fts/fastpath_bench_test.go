package fts_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
)

var benchVocab = []string{
	"the", "of", "and", "to", "in", "a", "is", "that", "for", "it",
	"barack", "obama", "russia", "mars", "hotel", "barge", "paris",
	"rosa", "speech", "visit", "country", "space", "exploration", "satellite",
	"forest", "mountain", "river", "ocean", "city", "valley", "desert",
	"music", "dance", "film", "novel", "poem", "painting", "sculpture",
	"algorithm", "data", "query", "index", "search", "relevance", "score",
}

func benchBuildSvc(b *testing.B, nDocs, wordsPerDoc int) *fts.Service {
	b.Helper()
	svc := fts.New(slicedradix.New(), fts.WordKeys, fts.WithScorer(fts.BM25()))
	r := rand.New(rand.NewSource(42))
	ctx := context.Background()
	for i := range nDocs {
		buf := make([]byte, 0, wordsPerDoc*8)
		for j := range wordsPerDoc {
			if j > 0 {
				buf = append(buf, ' ')
			}
			buf = append(buf, benchVocab[r.Intn(len(benchVocab))]...)
		}
		id := fts.DocID(fmt.Sprintf("doc-%d", i))
		if err := svc.IndexDocument(ctx, id, string(buf)); err != nil {
			b.Fatalf("index: %v", err)
		}
	}
	return svc
}

func BenchmarkAndFastPathSortMerge(b *testing.B) {
	svc := benchBuildSvc(b, 10000, 30)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.MustClause(fts.TermQuery{Term: "the"}),
		fts.MustClause(fts.TermQuery{Term: "and"}),
	}}
	ctx := context.Background()

	for b.Loop() {
		if _, err := svc.Search(ctx, q, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAndFastPathRareCommon(b *testing.B) {
	svc := benchBuildSvc(b, 10000, 30)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.MustClause(fts.TermQuery{Term: "sculpture"}),
		fts.MustClause(fts.TermQuery{Term: "the"}),
	}}
	ctx := context.Background()

	for b.Loop() {
		if _, err := svc.Search(ctx, q, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrFastPath(b *testing.B) {
	svc := benchBuildSvc(b, 10000, 30)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.ShouldClause(fts.TermQuery{Term: "the"}),
		fts.ShouldClause(fts.TermQuery{Term: "and"}),
	}}
	ctx := context.Background()

	for b.Loop() {
		if _, err := svc.Search(ctx, q, 100000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrWandTopK10(b *testing.B) {
	svc := benchBuildSvc(b, 10000, 30)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.ShouldClause(fts.TermQuery{Term: "the"}),
		fts.ShouldClause(fts.TermQuery{Term: "and"}),
	}}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.Search(ctx, q, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTermSliced(b *testing.B) {
	svc := benchBuildSvc(b, 10000, 30)
	q := fts.TermQuery{Term: "the"}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.Search(ctx, q, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPhraseSliced(b *testing.B) {
	svc := benchBuildSvc(b, 10000, 30)
	q := fts.PhraseQuery{Phrase: "the and"}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.Search(ctx, q, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexingSliced(b *testing.B) {
	r := rand.New(rand.NewSource(42))
	ctx := context.Background()
	const nDocs = 2000
	const wordsPerDoc = 30

	docs := make([]string, nDocs)
	for i := range nDocs {
		buf := make([]byte, 0, wordsPerDoc*8)
		for j := range wordsPerDoc {
			if j > 0 {
				buf = append(buf, ' ')
			}
			buf = append(buf, benchVocab[r.Intn(len(benchVocab))]...)
		}
		docs[i] = string(buf)
	}

	for b.Loop() {
		svc := fts.New(slicedradix.New(), fts.WordKeys, fts.WithScorer(fts.BM25()))
		for j, text := range docs {
			id := fts.DocID(fmt.Sprintf("doc-%d", j))
			if err := svc.IndexDocument(ctx, id, text); err != nil {
				b.Fatal(err)
			}
		}
	}
}
