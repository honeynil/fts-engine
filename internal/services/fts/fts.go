package fts

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"
	"unicode/utf8"

	"fts-hw/internal/utils"

	snowballeng "github.com/kljensen/snowball/english"
)

// ── Types ────────────────────────────────────────────────────────────────────

type Document struct {
	ID    uint64
	Count uint32
}

type ResultData struct {
	ID            uint64
	UniqueMatches int
	TotalMatches  int
}

type SearchResult struct {
	Results           []ResultData
	TotalResultsCount int
	Timings           map[string]string
}

// ── Index Interface ──────────────────────────────────────────────────────────

type Index interface {
	Search(key string) ([]Document, error)
	Insert(key string, id uint64) error
	Serialize(w io.Writer) error
	Analyze() utils.TrieStats
}

type IndexLoader func(r io.Reader) (Index, error)

// ── KeyGenerator ─────────────────────────────────────────────────────────────

type KeyGenerator func(token string) ([]string, error)

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}

// ── SearchService ────────────────────────────────────────────────────────────

type SearchService struct {
	index  Index
	keyGen KeyGenerator
}

func NewSearchService(index Index, keyGen KeyGenerator) *SearchService {
	return &SearchService{
		index:  index,
		keyGen: keyGen,
	}
}

func NewSearchServiceFromReader(r io.Reader, loader IndexLoader, keyGen KeyGenerator) (*SearchService, error) {
	index, err := loader(r)
	if err != nil {
		return nil, fmt.Errorf("fts: load index: %w", err)
	}
	return NewSearchService(index, keyGen), nil
}

// ── Tokenizer ────────────────────────────────────────────────────────────────
func isAlphanumeric(char rune) bool {
	return (char >= 'A' && char <= 'Z') ||
		(char >= 'a' && char <= 'z') ||
		(char >= '0' && char <= '9')
}

func isNumeric(token string) bool {
	if len(token) == 0 {
		return false
	}
	for _, c := range token {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// "error 404"         → ["error", "404"]
// "192.168.1.1"       → ["192", "168", "1", "1"]
// "/api/v1/users"     → ["api", "v1", "users"]
// "connection_refused"→ ["connection", "refused"]
// "GET /health 200"   → ["GET", "health", "200"]
func Tokenize(content string) []string {
	lastSplit := 0
	tokens := make([]string, 0)

	for i, char := range content {
		if isAlphanumeric(char) {
			continue
		}

		if i-lastSplit != 0 {
			tokens = append(tokens, content[lastSplit:i])
		}

		charBytes := utf8.RuneLen(char)
		lastSplit = i + charBytes
	}

	if len(content) > lastSplit {
		tokens = append(tokens, content[lastSplit:])
	}

	return tokens
}

// ── IndexDocument ────────────────────────────────────────────────────────────

func (s *SearchService) IndexDocument(
	ctx context.Context,
	docID uint64,
	content string,
) error {
	tokens := Tokenize(content)

	for _, token := range tokens {
		if len(token) < 3 {
			continue
		}

		if !isNumeric(token) {
			if snowballeng.IsStopWord(token) {
				continue
			}
			token = snowballeng.Stem(token, false)
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return fmt.Errorf("fts: index document: keygen: %w", err)
		}

		for _, key := range keys {
			if err := s.index.Insert(key, docID); err != nil {
				return fmt.Errorf("fts: index document: insert: %w", err)
			}
		}
	}

	return nil
}

// ── SearchDocuments ──────────────────────────────────────────────────────────

func (s *SearchService) SearchDocuments(
	ctx context.Context,
	query string,
	maxResults int,
) (*SearchResult, error) {
	startTime := time.Now()
	timings := make(map[string]string)

	preprocessStart := time.Now()
	tokens := Tokenize(query)
	timings["preprocess"] = formatDuration(time.Since(preprocessStart))

	searchStart := time.Now()

	docUniqueMatches := make(map[uint64]int)
	docTotalMatches := make(map[uint64]int)

	for _, token := range tokens {
		if len(token) < 3 {
			continue
		}

		if !isNumeric(token) {
			if snowballeng.IsStopWord(token) {
				continue
			}
			token = snowballeng.Stem(token, false)
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return nil, fmt.Errorf("fts: search: keygen: %w", err)
		}

		for _, key := range keys {
			docs, err := s.index.Search(key)
			if err != nil {
				return nil, fmt.Errorf("fts: search: index search: %w", err)
			}

			for _, doc := range docs {
				docUniqueMatches[doc.ID]++
				docTotalMatches[doc.ID] += int(doc.Count)
			}
		}
	}

	timings["search_tokens"] = formatDuration(time.Since(searchStart))

	totalFound := len(docUniqueMatches)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}

	results := make([]ResultData, 0, totalFound)
	for docID, uniqueMatches := range docUniqueMatches {
		results = append(results, ResultData{
			ID:            docID,
			UniqueMatches: uniqueMatches,
			TotalMatches:  docTotalMatches[docID],
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].UniqueMatches == results[j].UniqueMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].UniqueMatches > results[j].UniqueMatches
	})

	timings["total"] = formatDuration(time.Since(startTime))

	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
		Timings:           timings,
	}, nil
}

// ── Persist ──────────────────────────────────────────────────────────────────

func (s *SearchService) Persist(w io.Writer) error {
	return s.index.Serialize(w)
}

// ── Analyze ──────────────────────────────────────────────────────────────────

func (s *SearchService) Analyse() utils.TrieStats {
	return s.index.Analyze()
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}
