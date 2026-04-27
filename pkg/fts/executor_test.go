package fts

import (
	"context"
	"testing"
)

type prefixMemoryIndex struct {
	*positionalMemoryIndex
}

func newPrefixMemoryIndex() *prefixMemoryIndex {
	return &prefixMemoryIndex{positionalMemoryIndex: newPositionalMemoryIndex()}
}

func (p *prefixMemoryIndex) SearchPrefix(prefix string) ([]Posting, error) {
	aggregated := make(map[DocOrd]uint32)
	var order []DocOrd
	for key, refs := range p.postings {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		for _, r := range refs {
			if _, seen := aggregated[r.Ord]; !seen {
				order = append(order, r.Ord)
			}
			aggregated[r.Ord] += r.Count
		}
	}
	out := make([]Posting, 0, len(order))
	for _, ord := range order {
		out = append(out, Posting{Ord: ord, Count: aggregated[ord]})
	}
	return out, nil
}

func buildExecSvc(t *testing.T) *Service {
	t.Helper()
	factory := func(name string) (Index, error) { return newPrefixMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)
	ctx := context.Background()
	seed := map[string]map[string]string{
		"doc-a": {"title": "barack obama", "body": "speech at inauguration"},
		"doc-b": {"title": "banana split", "body": "barack said banana is tasty"},
		"doc-c": {"title": "russia", "body": "barack visited russia"},
		"doc-d": {"title": "mars rover", "body": "barack likes mars exploration"},
	}
	for id, fields := range seed {
		fs := map[string]Field{}
		for k, v := range fields {
			fs[k] = Field{Value: v}
		}
		if err := svc.Index(ctx, Document{ID: DocID(id), Fields: fs}); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}
	return svc
}

func TestSearchTermQuery(t *testing.T) {
	svc := buildExecSvc(t)
	res, err := svc.Search(context.Background(), TermQuery{Term: "barack"}, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Results) != 4 {
		t.Fatalf("all 4 docs mention 'barack', got %d", len(res.Results))
	}
}

func TestSearchTermQueryScopedToField(t *testing.T) {
	svc := buildExecSvc(t)
	res, err := svc.Search(context.Background(), TermQuery{Field: "title", Term: "barack"}, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("expected only doc-a matched in title, got %+v", res.Results)
	}
}

func TestSearchPhraseQueryViaAST(t *testing.T) {
	svc := buildExecSvc(t)
	res, err := svc.Search(context.Background(), PhraseQuery{Phrase: "barack obama"}, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("phrase 'barack obama' should only match doc-a, got %+v", res.Results)
	}
}

func TestSearchPrefixQuery(t *testing.T) {
	svc := buildExecSvc(t)
	res, err := svc.Search(context.Background(), PrefixQuery{Field: "title", Prefix: "ba"}, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	ids := map[DocID]bool{}
	for _, r := range res.Results {
		ids[r.ID] = true
	}
	if !ids["doc-a"] || !ids["doc-b"] {
		t.Fatalf("prefix 'ba' on title should match doc-a (barack) and doc-b (banana), got %+v", res.Results)
	}
	if ids["doc-c"] || ids["doc-d"] {
		t.Fatalf("prefix 'ba' on title should NOT match doc-c/doc-d, got %+v", res.Results)
	}
}

func TestSearchBooleanMustIntersects(t *testing.T) {
	svc := buildExecSvc(t)
	q := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "barack"}),
		MustClause(TermQuery{Term: "russia"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-c" {
		t.Fatalf("MUST barack AND russia should only match doc-c, got %+v", res.Results)
	}
}

func TestSearchBooleanMustNotExcludes(t *testing.T) {
	svc := buildExecSvc(t)
	q := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "barack"}),
		MustNotClause(TermQuery{Term: "russia"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	ids := map[DocID]bool{}
	for _, r := range res.Results {
		ids[r.ID] = true
	}
	if ids["doc-c"] {
		t.Fatalf("doc-c should be excluded, got %+v", res.Results)
	}
	if !ids["doc-a"] || !ids["doc-b"] || !ids["doc-d"] {
		t.Fatalf("expected doc-a/b/d to remain, got %+v", res.Results)
	}
}

func TestSearchBooleanShouldOnlyUnions(t *testing.T) {
	svc := buildExecSvc(t)
	q := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "obama"}),
		ShouldClause(TermQuery{Term: "mars"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	ids := map[DocID]bool{}
	for _, r := range res.Results {
		ids[r.ID] = true
	}
	if !ids["doc-a"] || !ids["doc-d"] {
		t.Fatalf("SHOULD obama OR mars should match doc-a + doc-d, got %+v", res.Results)
	}
}

func TestSearchBooleanShouldsBoostMusts(t *testing.T) {
	svc := buildExecSvc(t)
	q := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "barack"}),
		ShouldClause(TermQuery{Term: "obama"}),
	}}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Results) != 4 {
		t.Fatalf("MUST barack should still match all 4, got %d", len(res.Results))
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("doc-a should rank highest (boosted by 'obama'), got %+v", res.Results)
	}
}

func TestSearchNilQueryEmpty(t *testing.T) {
	svc := buildExecSvc(t)
	res, err := svc.Search(context.Background(), nil, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("nil query should return no results, got %+v", res.Results)
	}
}

func TestSearchViaParsedQuery(t *testing.T) {
	svc := buildExecSvc(t)
	q, err := ParseQuery(`+barack -russia "barack obama"`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Results) < 1 {
		t.Fatalf("want at least one match, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("doc-a (exact phrase + MUST barack) should rank first, got %+v", res.Results)
	}
	for _, r := range res.Results {
		if r.ID == "doc-c" {
			t.Fatalf("doc-c (russia) must be excluded, got %+v", res.Results)
		}
	}
}
