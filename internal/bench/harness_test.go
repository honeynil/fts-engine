package bench

import (
	"context"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/internal/domain/models"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func mkDoc(id, title, abstract string) models.Document {
	return models.Document{
		DocumentBase: models.DocumentBase{Title: title, Abstract: abstract},
		ID:           id,
	}
}

func TestCorpusTitleIndex(t *testing.T) {
	c := Corpus{
		mkDoc("1", "  Rosa Barge  ", "french hotel barge"),
		mkDoc("2", "Obama Speech", "presidential address"),
		mkDoc("3", "", "no title"),
	}
	idx := c.TitleIndex()
	if got, want := idx["rosa barge"], "1"; got != want {
		t.Fatalf("rosa barge: want %q, got %q", want, got)
	}
	if _, ok := idx[""]; ok {
		t.Fatalf("empty title should not be indexed")
	}
}

func TestResolveRelevant(t *testing.T) {
	titleIdx := map[string]string{"rosa barge": "1", "obama speech": "2"}
	q := Query{
		RelevantIDs:    []string{"raw-id-3"},
		RelevantTitles: []string{"Rosa Barge", "Unknown Title"},
	}
	got := ResolveRelevant(q, titleIdx)
	want := []string{"raw-id-3", "1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestCountMissingTitles(t *testing.T) {
	titleIdx := map[string]string{"rosa barge": "1"}
	gt := &GroundTruth{Queries: []Query{
		{RelevantTitles: []string{"Rosa Barge", "Unknown"}},
		{RelevantTitles: []string{"Another Unknown"}},
	}}
	if got := CountMissingTitles(gt, titleIdx); got != 2 {
		t.Fatalf("want 2 missing, got %d", got)
	}
}

func TestIndexAndRunQueriesEndToEnd(t *testing.T) {
	ctx := context.Background()
	corpus := Corpus{
		mkDoc("1", "Rosa Barge", "Rosa is a French hotel barge on the Canal du Midi"),
		mkDoc("2", "Barack Obama", "Obama delivered a speech on climate change"),
		mkDoc("3", "Empty", "unrelated content about cats and dogs"),
	}

	svc := fts.New(radix.New(), keygen.Word)
	idxReport, err := IndexCorpus(ctx, svc, corpus, SelectAbstract)
	if err != nil {
		t.Fatalf("IndexCorpus: %v", err)
	}
	if idxReport.DocumentCount != 3 {
		t.Fatalf("want 3 indexed, got %d", idxReport.DocumentCount)
	}

	gt := &GroundTruth{Queries: []Query{
		{Query: "french hotel barge", RelevantTitles: []string{"Rosa Barge"}},
		{Query: "obama speech", RelevantTitles: []string{"Barack Obama"}},
	}}

	titleIdx := corpus.TitleIndex()
	qr, err := RunQueries(ctx, svc, gt, titleIdx, 10)
	if err != nil {
		t.Fatalf("RunQueries: %v", err)
	}
	if len(qr) != 2 {
		t.Fatalf("want 2 query reports, got %d", len(qr))
	}

	report := Aggregate(10, idxReport, qr)
	if report.MeanNDCG <= 0 {
		t.Fatalf("expected positive mean nDCG, got %v", report.MeanNDCG)
	}
	if report.MeanMRR <= 0 {
		t.Fatalf("expected positive mean MRR, got %v", report.MeanMRR)
	}
}
