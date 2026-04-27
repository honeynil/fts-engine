package fts

import (
	"context"
	"io"
)

type DocID string
type DocOrd uint32

type Posting struct {
	Ord   DocOrd
	Count uint32
}

type PositionalPosting struct {
	Ord       DocOrd
	Positions []uint32
}

type Result struct {
	ID            DocID
	UniqueMatches int
	TotalMatches  int
	Score         float64
}

type SearchResult struct {
	Results           []Result
	TotalResultsCount int
	Timings           map[string]string
}

const DefaultField = "_default"

type Document struct {
	ID     DocID
	Fields map[string]Field
}

type Field struct {
	Value    string
	Pipeline Pipeline
}

type Index interface {
	Insert(key string, ord DocOrd) error
	Search(key string) ([]Posting, error)
}

type PositionalIndex interface {
	Index
	InsertAt(key string, ord DocOrd, position uint32) error
	SearchPositional(key string) ([]PositionalPosting, error)
}

type PrefixIndex interface {
	Index
	SearchPrefix(prefix string) ([]Posting, error)
}

type IndexFactory func(fieldName string) (Index, error)

type Analyzer interface {
	Analyze() Stats
}

type Serializable interface {
	Serialize(w io.Writer) error
}

type IndexLoader func(r io.Reader) (Index, error)

type KeyGenerator func(token string) ([]string, error)

type Pipeline interface {
	Process(text string) []string
}

type Filter interface {
	Add(item []byte) bool
	Contains(item []byte) bool
}

type BuildableFilter interface {
	Build() error
}

type StaticFilter interface {
	BuildFromKeyStream(stream func(func([]byte) bool) error) error
	Contains(item []byte) bool
}

type RetryableStaticFilter interface {
	StaticFilter
	BuildWithRetriesFromKeyStream(stream func(func([]byte) bool) error, maxAttempts uint32) error
}

type Engine interface {
	IndexDocument(ctx context.Context, docID DocID, content string) error
	SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error)
}

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}
