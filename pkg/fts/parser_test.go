package fts

import (
	"reflect"
	"testing"
)

func TestParseQuerySingleTerm(t *testing.T) {
	got, err := ParseQuery("hello")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	want := TermQuery{Term: "hello"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseQueryTwoTermsAreShouldClauses(t *testing.T) {
	got, err := ParseQuery("hello world")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	b, ok := got.(*BooleanQuery)
	if !ok {
		t.Fatalf("want BooleanQuery, got %T", got)
	}
	if len(b.Clauses) != 2 {
		t.Fatalf("want 2 clauses, got %d", len(b.Clauses))
	}
	for i, c := range b.Clauses {
		if c.Occur != Should {
			t.Fatalf("clause %d occur = %v, want Should", i, c.Occur)
		}
	}
}

func TestParseQueryMustAndMustNot(t *testing.T) {
	got, err := ParseQuery("+apple -banana cherry")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	b, ok := got.(*BooleanQuery)
	if !ok {
		t.Fatalf("want BooleanQuery, got %T", got)
	}
	if len(b.Clauses) != 3 {
		t.Fatalf("want 3 clauses, got %d", len(b.Clauses))
	}
	if b.Clauses[0].Occur != Must || b.Clauses[0].Query.(TermQuery).Term != "apple" {
		t.Fatalf("bad first clause: %+v", b.Clauses[0])
	}
	if b.Clauses[1].Occur != MustNot || b.Clauses[1].Query.(TermQuery).Term != "banana" {
		t.Fatalf("bad second clause: %+v", b.Clauses[1])
	}
	if b.Clauses[2].Occur != Should || b.Clauses[2].Query.(TermQuery).Term != "cherry" {
		t.Fatalf("bad third clause: %+v", b.Clauses[2])
	}
}

func TestParseQueryFieldScopedTerm(t *testing.T) {
	got, err := ParseQuery("title:hello")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	want := TermQuery{Field: "title", Term: "hello"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseQueryPhrase(t *testing.T) {
	got, err := ParseQuery(`"barack obama"`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	want := PhraseQuery{Phrase: "barack obama"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseQueryFieldScopedPhrase(t *testing.T) {
	got, err := ParseQuery(`title:"barack obama"`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	want := PhraseQuery{Field: "title", Phrase: "barack obama"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseQueryPrefix(t *testing.T) {
	got, err := ParseQuery("bar*")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	want := PrefixQuery{Prefix: "bar"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseQueryComplex(t *testing.T) {
	got, err := ParseQuery(`+title:"barack obama" -body:russia ba*`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	b, ok := got.(*BooleanQuery)
	if !ok {
		t.Fatalf("want BooleanQuery, got %T", got)
	}
	if len(b.Clauses) != 3 {
		t.Fatalf("want 3 clauses, got %d", len(b.Clauses))
	}
	if b.Clauses[0].Occur != Must {
		t.Fatalf("clause 0 occur %v", b.Clauses[0].Occur)
	}
	if _, ok := b.Clauses[0].Query.(PhraseQuery); !ok {
		t.Fatalf("clause 0 query type %T", b.Clauses[0].Query)
	}
	if b.Clauses[1].Occur != MustNot {
		t.Fatalf("clause 1 occur %v", b.Clauses[1].Occur)
	}
	if b.Clauses[2].Occur != Should {
		t.Fatalf("clause 2 occur %v", b.Clauses[2].Occur)
	}
	if _, ok := b.Clauses[2].Query.(PrefixQuery); !ok {
		t.Fatalf("clause 2 query type %T", b.Clauses[2].Query)
	}
}

func TestParseQueryUnterminatedQuoteFails(t *testing.T) {
	if _, err := ParseQuery(`"oops`); err == nil {
		t.Fatalf("expected error on unterminated quote")
	}
}

func TestParseQueryEmpty(t *testing.T) {
	got, err := ParseQuery("")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	b, ok := got.(*BooleanQuery)
	if !ok || len(b.Clauses) != 0 {
		t.Fatalf("empty input should give empty BooleanQuery, got %+v", got)
	}
}

func TestParseQueryLoneStarIsTerm(t *testing.T) {
	got, err := ParseQuery("*")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	if term, ok := got.(TermQuery); !ok || term.Term != "*" {
		t.Fatalf("lone '*' should be a TermQuery with Term=*, got %+v", got)
	}
}
