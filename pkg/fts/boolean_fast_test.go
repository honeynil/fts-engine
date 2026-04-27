package fts_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
)

func buildSvcSliced(t *testing.T, scored bool) *fts.Service {
	t.Helper()
	opts := []fts.Option{}
	if scored {
		opts = append(opts, fts.WithScorer(fts.BM25()))
	}
	svc := fts.New(slicedradix.New(), fts.WordKeys, opts...)
	ctx := context.Background()
	corpus := map[string]string{
		"doc-a": "barack obamka gave a speech at inauguration",
		"doc-b": "banana split dessert is barack tasty",
		"doc-c": "russia is a country barack visited it",
		"doc-d": "mars rover exploration barack likes space",
		"doc-e": "the quick brown fox jumps over lazy dogs",
		"doc-f": "obamka russia meeting in moscow photo op",
	}
	ids := make([]string, 0, len(corpus))
	for id := range corpus {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := svc.IndexDocument(ctx, fts.DocID(id), corpus[id]); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}
	return svc
}

func collectIDs(res *fts.SearchResult) []string {
	out := make([]string, 0, len(res.Results))
	for _, r := range res.Results {
		out = append(out, string(r.ID))
	}
	sort.Strings(out)
	return out
}

func TestAndFastPathMatchesEagerPath(t *testing.T) {
	svc := buildSvcSliced(t, true)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.MustClause(fts.TermQuery{Term: "barack"}),
		fts.MustClause(fts.TermQuery{Term: "russia"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	got := collectIDs(res)
	want := []string{"doc-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AND result = %v, want %v", got, want)
	}
}

func TestAndFastPathEmptyWhenAnyClauseEmpty(t *testing.T) {
	svc := buildSvcSliced(t, true)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.MustClause(fts.TermQuery{Term: "barack"}),
		fts.MustClause(fts.TermQuery{Term: "zzznonexistent"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("AND with empty clause should match nothing, got %d results", len(res.Results))
	}
}

func TestAndFastPathMustNotExcludes(t *testing.T) {
	svc := buildSvcSliced(t, true)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.MustClause(fts.TermQuery{Term: "barack"}),
		fts.MustNotClause(fts.TermQuery{Term: "russia"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	got := collectIDs(res)
	want := []string{"doc-a", "doc-b", "doc-d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MUST barack MUST_NOT russia = %v, want %v", got, want)
	}
}

func TestAndFastPathThreeClauseIntersection(t *testing.T) {

	svc := buildSvcSliced(t, true)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.MustClause(fts.TermQuery{Term: "barack"}),
		fts.MustClause(fts.TermQuery{Term: "russia"}),
		fts.MustClause(fts.TermQuery{Term: "visited"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	got := collectIDs(res)
	want := []string{"doc-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("3-clause AND = %v, want %v", got, want)
	}
}

func TestOrFastPathUnionsClauses(t *testing.T) {
	svc := buildSvcSliced(t, true)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.ShouldClause(fts.TermQuery{Term: "russia"}),
		fts.ShouldClause(fts.TermQuery{Term: "mars"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	got := collectIDs(res)
	want := []string{"doc-c", "doc-d", "doc-f"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OR result = %v, want %v", got, want)
	}
}

func TestWandReturnsTopK(t *testing.T) {

	svc := buildSvcSliced(t, true)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.ShouldClause(fts.TermQuery{Term: "barack"}),
		fts.ShouldClause(fts.TermQuery{Term: "russia"}),
	}}
	full, err := svc.Search(context.Background(), q, 100)
	if err != nil {
		t.Fatalf("Search(full): %v", err)
	}
	top2, err := svc.Search(context.Background(), q, 2)
	if err != nil {
		t.Fatalf("Search(top2): %v", err)
	}
	if len(top2.Results) != 2 {
		t.Fatalf("top-2 should return 2 results, got %d", len(top2.Results))
	}
	wantTop2 := []string{string(full.Results[0].ID), string(full.Results[1].ID)}
	sort.Strings(wantTop2)
	got := collectIDs(top2)
	if !reflect.DeepEqual(got, wantTop2) {
		t.Fatalf("WAND top-2 = %v, want %v (top-2 of full result %+v)", got, wantTop2, full.Results)
	}
}

func TestWandSkippedWhenScorerNil(t *testing.T) {
	svc := buildSvcSliced(t, false)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.ShouldClause(fts.TermQuery{Term: "russia"}),
		fts.ShouldClause(fts.TermQuery{Term: "mars"}),
	}}
	resAll, err := svc.Search(context.Background(), q, 100)
	if err != nil {
		t.Fatalf("Search all: %v", err)
	}
	if len(resAll.Results) != 3 {
		t.Fatalf("OR without scorer should still enumerate all matches, got %d (%+v)",
			len(resAll.Results), resAll.Results)
	}
}

func TestWandMatchesEagerScores(t *testing.T) {
	svc := buildSvcSliced(t, true)
	q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
		fts.ShouldClause(fts.TermQuery{Term: "barack"}),
		fts.ShouldClause(fts.TermQuery{Term: "obamka"}),
	}}
	full, err := svc.Search(context.Background(), q, 100)
	if err != nil {
		t.Fatalf("Search full: %v", err)
	}
	top, err := svc.Search(context.Background(), q, 2)
	if err != nil {
		t.Fatalf("Search top: %v", err)
	}
	if len(top.Results) != 2 {
		t.Fatalf("top-2 expected 2 results, got %d", len(top.Results))
	}
	wantScore := make(map[fts.DocID]float64)
	for _, r := range full.Results {
		wantScore[r.ID] = r.Score
	}
	for _, r := range top.Results {
		if r.Score != wantScore[r.ID] {
			t.Fatalf("doc %s: WAND score %.6f != eager %.6f", r.ID, r.Score, wantScore[r.ID])
		}
	}
}
