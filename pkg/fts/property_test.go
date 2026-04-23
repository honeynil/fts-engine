package fts_test

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
)

func TestPropertyAndFastPathMatchesSetSemantics(t *testing.T) {
	const iterations = 40
	base := rand.New(rand.NewSource(2026))
	vocab := propVocab(30)

	for it := range iterations {
		seed := base.Int63()
		corpus := genCorpus(seed, 200, vocab)
		svc := buildSvcFromCorpus(t, corpus)
		a, b := pickTwoPresentTerms(seed, corpus, vocab)

		q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
			fts.MustClause(fts.TermQuery{Term: a}),
			fts.MustClause(fts.TermQuery{Term: b}),
		}}
		res, err := svc.Search(context.Background(), q, 1000)
		if err != nil {
			t.Fatalf("iter=%d seed=%d Search: %v", it, seed, err)
		}
		got := idSet(res)

		want := map[fts.DocID]bool{}
		for id, tokens := range corpus {
			if contains(tokens, a) && contains(tokens, b) {
				want[id] = true
			}
		}
		if !setsEqual(got, want) {
			t.Fatalf("iter=%d seed=%d a=%q b=%q\n  got:  %v\n  want: %v",
				it, seed, a, b, sortedKeys(got), sortedKeys(want))
		}
	}
}

func TestPropertyOrFastPathMatchesSetSemantics(t *testing.T) {
	const iterations = 40
	base := rand.New(rand.NewSource(4077))
	vocab := propVocab(30)

	for it := range iterations {
		seed := base.Int63()
		corpus := genCorpus(seed, 200, vocab)
		svc := buildSvcFromCorpus(t, corpus)
		a, b := pickTwoPresentTerms(seed, corpus, vocab)

		q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
			fts.ShouldClause(fts.TermQuery{Term: a}),
			fts.ShouldClause(fts.TermQuery{Term: b}),
		}}
		res, err := svc.Search(context.Background(), q, 10000)
		if err != nil {
			t.Fatalf("iter=%d seed=%d Search: %v", it, seed, err)
		}
		got := idSet(res)

		want := map[fts.DocID]bool{}
		for id, tokens := range corpus {
			if contains(tokens, a) || contains(tokens, b) {
				want[id] = true
			}
		}
		if !setsEqual(got, want) {
			t.Fatalf("iter=%d seed=%d a=%q b=%q\n  got:  %v\n  want: %v",
				it, seed, a, b, sortedKeys(got), sortedKeys(want))
		}
	}
}

func TestPropertyWandSubsetOfFullOr(t *testing.T) {
	const iterations = 40
	base := rand.New(rand.NewSource(9001))
	vocab := propVocab(30)

	for it := range iterations {
		seed := base.Int63()
		corpus := genCorpus(seed, 200, vocab)
		svc := buildSvcFromCorpus(t, corpus)
		a, b := pickTwoPresentTerms(seed, corpus, vocab)

		q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
			fts.ShouldClause(fts.TermQuery{Term: a}),
			fts.ShouldClause(fts.TermQuery{Term: b}),
		}}

		full, err := svc.Search(context.Background(), q, 10000)
		if err != nil {
			t.Fatalf("Search full: %v", err)
		}
		fullSet := idSet(full)

		top, err := svc.Search(context.Background(), q, 5)
		if err != nil {
			t.Fatalf("Search wand: %v", err)
		}

		for _, r := range top.Results {
			if !fullSet[r.ID] {
				t.Fatalf("iter=%d seed=%d: WAND returned %s but full union doesn't contain it",
					it, seed, r.ID)
			}
		}
		if len(top.Results) > 5 {
			t.Fatalf("iter=%d seed=%d: WAND returned %d > topK=5", it, seed, len(top.Results))
		}
	}
}

func propVocab(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("t%03d", i)
	}
	return out
}

func genCorpus(seed int64, nDocs int, vocab []string) map[fts.DocID][]string {
	r := rand.New(rand.NewSource(seed))
	out := make(map[fts.DocID][]string, nDocs)
	for i := 0; i < nDocs; i++ {
		id := fts.DocID(fmt.Sprintf("d%04d", i))
		nTokens := 1 + r.Intn(10)
		seen := map[string]struct{}{}
		tokens := make([]string, 0, nTokens)
		for len(tokens) < nTokens {
			w := vocab[r.Intn(len(vocab))]
			if _, dup := seen[w]; dup {
				continue
			}
			seen[w] = struct{}{}
			tokens = append(tokens, w)
		}
		out[id] = tokens
	}
	return out
}

func buildSvcFromCorpus(t *testing.T, corpus map[fts.DocID][]string) *fts.Service {
	t.Helper()
	svc := fts.New(slicedradix.New(), fts.WordKeys, fts.WithScorer(fts.BM25()))
	ids := make([]fts.DocID, 0, len(corpus))
	for id := range corpus {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ctx := context.Background()
	for _, id := range ids {
		text := strings.Join(corpus[id], " ")
		if err := svc.IndexDocument(ctx, id, text); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}
	return svc
}

func pickTwoPresentTerms(seed int64, corpus map[fts.DocID][]string, vocab []string) (string, string) {
	seen := map[string]bool{}
	for _, toks := range corpus {
		for _, tk := range toks {
			seen[tk] = true
		}
	}
	present := make([]string, 0, len(seen))
	for w := range seen {
		present = append(present, w)
	}
	sort.Strings(present)
	r := rand.New(rand.NewSource(seed ^ 0xbeef))
	if len(present) < 2 {
		return vocab[0], vocab[1]
	}
	a := present[r.Intn(len(present))]
	b := present[r.Intn(len(present))]
	for b == a {
		b = present[r.Intn(len(present))]
	}
	return a, b
}

func contains(s []string, v string) bool {
	return slices.Contains(s, v)
}

func idSet(res *fts.SearchResult) map[fts.DocID]bool {
	out := make(map[fts.DocID]bool, len(res.Results))
	for _, r := range res.Results {
		out[r.ID] = true
	}
	return out
}

func setsEqual(a, b map[fts.DocID]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func sortedKeys(m map[fts.DocID]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, string(k))
	}
	sort.Strings(out)
	return out
}
