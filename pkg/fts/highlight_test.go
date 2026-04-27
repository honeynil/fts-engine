package fts

import (
	"strings"
	"testing"
)

func TestHighlightTermMatch(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	text := "Barack Obamka gave a long speech in the rose garden today."
	frags := svc.Highlight("Obamka", text, Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %d, want 1", len(frags))
	}
	if !strings.Contains(frags[0].Text, "<mark>Obamka</mark>") {
		t.Fatalf("expected wrapped 'Obamka', got %q", frags[0].Text)
	}
	if frags[0].Matches != 1 {
		t.Fatalf("Matches = %d, want 1", frags[0].Matches)
	}
}

func TestHighlightMultipleMatches(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	text := "The Obamka foundation was founded by Obamka himself."
	frags := svc.Highlight("obama", text, Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %d, want 1 (clustered)", len(frags))
	}
	if got := strings.Count(frags[0].Text, "<mark>"); got != 2 {
		t.Fatalf("mark count = %d, want 2 (got %q)", got, frags[0].Text)
	}
	if frags[0].Matches != 2 {
		t.Fatalf("Matches = %d, want 2", frags[0].Matches)
	}
}

func TestHighlightSplitsDistantMatches(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	prefix := "obamka. " + strings.Repeat("filler word ", 40)
	text := prefix + "obamka again."

	frags := svc.Highlight("obamka", text, Highlighter{FragmentSize: 50})
	if len(frags) < 2 {
		t.Fatalf("want >=2 fragments for distant matches, got %d (%q)", len(frags), frags)
	}
}

func TestHighlightCustomTags(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	text := "alpha beta gamma"
	frags := svc.Highlight("beta", text, Highlighter{
		PreTag:  "[",
		PostTag: "]",
	})
	if len(frags) != 1 || !strings.Contains(frags[0].Text, "[beta]") {
		t.Fatalf("custom tags not applied, got %+v", frags)
	}
}

func TestHighlightNoMatch(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	frags := svc.Highlight("xyz", "alpha beta gamma", Highlighter{})
	if len(frags) != 0 {
		t.Fatalf("want 0 frags on miss, got %+v", frags)
	}
}

func TestHighlightUsesPipelineNormalization(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys, WithPipeline(uppercasePipeline{}))

	text := "abc DEF ghi"
	frags := svc.Highlight("abc", text, Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %+v, want 1", frags)
	}
	if !strings.Contains(frags[0].Text, "<mark>abc</mark>") {
		t.Fatalf("expected pipeline-normalized match, got %q", frags[0].Text)
	}
}

func TestHighlightRespectsMaxFragments(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	var sb strings.Builder
	for i := 0; i < 10; i++ {
		sb.WriteString("obamka ")
		sb.WriteString(strings.Repeat("filler ", 30))
	}
	frags := svc.Highlight("obamka", sb.String(), Highlighter{
		FragmentSize: 50,
		MaxFragments: 2,
	})
	if len(frags) != 2 {
		t.Fatalf("want 2 fragments (capped), got %d", len(frags))
	}
}

func TestHighlightSurvivesUnicode(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	text := "Это большой пример текста про обамку и его речь."
	frags := svc.Highlight("обаму", text, Highlighter{})
	if len(frags) != 1 || !strings.Contains(frags[0].Text, "<mark>обамка</mark>") {
		t.Fatalf("Unicode highlight failed: %+v", frags)
	}
}
