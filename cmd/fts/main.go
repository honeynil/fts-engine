package main

import (
	"bytes"
	"context"
	"fmt"
	"fts-hw/internal/services/fts"
	radixtriesliced "fts-hw/internal/services/fts/slicedradix"
	"fts-hw/internal/utils"
	"log/slog"
	"os"
	"runtime"
)

var _ = runtime.NumCPU

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	ctx := context.Background()

	trie := radixtriesliced.New()
	svc := fts.NewSearchService(trie, fts.WordKeys)

	docs := []struct {
		id      uint64
		content string
	}{
		{1, "connection refused to database postgres port 5432"},
		{2, "error 404 not found api endpoint users"},
		{3, "connection timeout after 30 seconds host unreachable"},
		{4, "panic runtime error index out of range"},
		{5, "GET request 200 success api health check"},
	}

	log.Info("Indexing documents", "count", len(docs))

	startTime := utils.MeasureMemory(func() {
		for _, doc := range docs {
			if err := svc.IndexDocument(ctx, doc.id, doc.content); err != nil {
				log.Error("Failed to index document", "id", doc.id, "error", err)
			}
		}
	})

	log.Info("Indexed",
		"heapMB", startTime.HeapAlloc/1024/1024,
		"objects", startTime.HeapObjects,
	)

	results, err := svc.SearchDocuments(ctx, "connection refused", 10)
	if err != nil {
		log.Error("Search failed", "error", err)
		return
	}

	log.Info("Search results", "query", "connection refused", "total", results.TotalResultsCount)
	for _, r := range results.Results {
		fmt.Printf("  docID=%d uniqueMatches=%d totalMatches=%d\n",
			r.ID, r.UniqueMatches, r.TotalMatches)
	}

	log.Info("Testing persistence...")

	var buf bytes.Buffer
	if err := svc.Persist(&buf); err != nil {
		log.Error("Persist failed", "error", err)
		return
	}
	log.Info("Serialized", "bytes", buf.Len())

	svc2, err := fts.NewSearchServiceFromReader(&buf, radixtriesliced.LoadAsIndex, fts.WordKeys)
	if err != nil {
		log.Error("Load failed", "error", err)
		return
	}

	results2, err := svc2.SearchDocuments(ctx, "connection refused", 10)
	if err != nil {
		log.Error("Search after load failed", "error", err)
		return
	}
	log.Info("Search after load", "total", results2.TotalResultsCount)

	analyzeIndex(log, svc)
}

func analyzeIndex(log *slog.Logger, svc *fts.SearchService) {
	stats := svc.Analyse()
	log.Info("Index analysis",
		"nodes", stats.Nodes,
		"leaves", stats.Leaves,
		"maxDepth", stats.MaxDepth,
		"avgDepth", fmt.Sprintf("%.2f", stats.AvgDepth),
		"totalDocs", stats.TotalDocs,
		"totalChildren", stats.TotalChildren,
	)
	for level, avg := range stats.AvgChildrenPerLevel {
		log.Info(fmt.Sprintf("Level %d: avg children = %.2f", level, avg))
	}
}

func setupLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
}
