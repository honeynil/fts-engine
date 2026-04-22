package bench

import (
	"math"
	"testing"
	"time"
)

func approxEq(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestNDCGPerfect(t *testing.T) {
	rel := NewRelevanceSet([]string{"a", "b", "c"})
	got := NDCG([]string{"a", "b", "c", "x"}, rel, 10)
	if !approxEq(got, 1.0) {
		t.Fatalf("perfect ranking: want 1.0, got %v", got)
	}
}

func TestNDCGNoHits(t *testing.T) {
	rel := NewRelevanceSet([]string{"a", "b"})
	if got := NDCG([]string{"x", "y", "z"}, rel, 10); got != 0 {
		t.Fatalf("no hits: want 0, got %v", got)
	}
}

func TestNDCGPartial(t *testing.T) {
	// relevant = {a}, rank puts a at position 2
	// DCG  = 1/log2(3)
	// IDCG = 1/log2(2) = 1
	rel := NewRelevanceSet([]string{"a"})
	got := NDCG([]string{"x", "a", "y"}, rel, 10)
	want := 1.0 / math.Log2(3)
	if !approxEq(got, want) {
		t.Fatalf("partial ranking: want %v, got %v", want, got)
	}
}

func TestNDCGEmptyRelevant(t *testing.T) {
	if got := NDCG([]string{"a"}, NewRelevanceSet(nil), 10); got != 0 {
		t.Fatalf("empty relevant: want 0, got %v", got)
	}
}

func TestMRR(t *testing.T) {
	rel := NewRelevanceSet([]string{"b"})
	if got := MRR([]string{"a", "b", "c"}, rel); !approxEq(got, 0.5) {
		t.Fatalf("rank 2: want 0.5, got %v", got)
	}
	if got := MRR([]string{"a", "c"}, rel); got != 0 {
		t.Fatalf("no hit: want 0, got %v", got)
	}
	if got := MRR([]string{"b"}, rel); !approxEq(got, 1.0) {
		t.Fatalf("rank 1: want 1.0, got %v", got)
	}
}

func TestRecall(t *testing.T) {
	rel := NewRelevanceSet([]string{"a", "b", "c", "d"})
	if got := Recall([]string{"a", "b", "x"}, rel, 3); !approxEq(got, 0.5) {
		t.Fatalf("2 of 4 relevant in top-3: want 0.5, got %v", got)
	}
	if got := Recall([]string{"a"}, NewRelevanceSet(nil), 3); got != 0 {
		t.Fatalf("empty relevant: want 0, got %v", got)
	}
}

func TestPercentile(t *testing.T) {
	ds := []time.Duration{10, 20, 30, 40, 50}
	cases := []struct {
		p    float64
		want time.Duration
	}{
		{0.0, 10},
		{0.5, 30},
		{1.0, 50},
	}
	for _, c := range cases {
		if got := Percentile(ds, c.p); got != c.want {
			t.Fatalf("p=%v: want %v, got %v", c.p, c.want, got)
		}
	}

	if got := Percentile(ds, 0.25); got != 20 {
		t.Fatalf("p=0.25: want 20, got %v", got)
	}
	if got := Percentile(ds, 0.1); got != 14 {
		t.Fatalf("p=0.1: want 14, got %v", got)
	}
}

func TestPercentileEmpty(t *testing.T) {
	if got := Percentile(nil, 0.5); got != 0 {
		t.Fatalf("empty: want 0, got %v", got)
	}
}
