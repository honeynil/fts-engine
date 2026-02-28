package radixtriesliced

import (
	"bytes"
	"fts-hw/internal/services/fts"
	"testing"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func mustInsert(t *testing.T, trie *Trie, word string, docID uint64) {
	t.Helper()
	if err := trie.Insert(word, docID); err != nil {
		t.Fatalf("Insert(%q, %d): unexpected error: %v", word, docID, err)
	}
}

func mustSearch(t *testing.T, trie *Trie, word string) []fts.Document {
	t.Helper()
	docs, err := trie.Search(word)
	if err != nil {
		t.Fatalf("Search(%q): unexpected error: %v", word, err)
	}
	return docs
}

func docCount(docs []fts.Document, id uint64) uint32 {
	for _, d := range docs {
		if d.ID == id {
			return d.Count
		}
	}
	return 0
}

// ── Insert & Search ──────────────────────────────────────────────────────────

func TestInsertAndSearch_BasicWord(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "connection", 1)

	docs := mustSearch(t, trie, "connection")
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
	if docs[0].ID != 1 {
		t.Errorf("expected ID=1, got %d", docs[0].ID)
	}
	if docs[0].Count != 1 {
		t.Errorf("expected Count=1, got %d", docs[0].Count)
	}
}

func TestInsertAndSearch_NotFound(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "connection", 1)

	docs := mustSearch(t, trie, "timeout")
	if len(docs) != 0 {
		t.Errorf("expected 0 docs, got %d", len(docs))
	}
}

func TestInsertAndSearch_EmptyTrie(t *testing.T) {
	trie := New()
	docs := mustSearch(t, trie, "anything")
	if len(docs) != 0 {
		t.Errorf("expected 0 docs on empty trie, got %d", len(docs))
	}
}

func TestInsert_SameWordSameDoc_IncrementsCount(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "error", 42)
	mustInsert(t, trie, "error", 42)
	mustInsert(t, trie, "error", 42)

	docs := mustSearch(t, trie, "error")
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
	if docs[0].Count != 3 {
		t.Errorf("expected Count=3, got %d", docs[0].Count)
	}
}

func TestInsert_SameWordDifferentDocs(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "error", 1)
	mustInsert(t, trie, "error", 2)
	mustInsert(t, trie, "error", 3)

	docs := mustSearch(t, trie, "error")
	if len(docs) != 3 {
		t.Fatalf("expected 3 docs, got %d", len(docs))
	}

	for _, id := range []uint64{1, 2, 3} {
		if docCount(docs, id) != 1 {
			t.Errorf("expected Count=1 for docID=%d", id)
		}
	}
}

func TestInsert_MultipleWords(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "connection", 1)
	mustInsert(t, trie, "timeout", 2)
	mustInsert(t, trie, "refused", 3)

	for _, tc := range []struct {
		word  string
		docID uint64
	}{
		{"connection", 1},
		{"timeout", 2},
		{"refused", 3},
	} {
		docs := mustSearch(t, trie, tc.word)
		if len(docs) == 0 {
			t.Errorf("word %q not found", tc.word)
			continue
		}
		if docs[0].ID != tc.docID {
			t.Errorf("word %q: expected docID=%d, got %d", tc.word, tc.docID, docs[0].ID)
		}
	}
}

// ── Numbers (новое поведение) ────────────────────────────────────────────────

func TestInsert_NumericWord(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "404", 10)

	docs := mustSearch(t, trie, "404")
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc for numeric word '404', got %d", len(docs))
	}
	if docs[0].ID != 10 {
		t.Errorf("expected ID=10, got %d", docs[0].ID)
	}
}

func TestInsert_NumericStatusCodes(t *testing.T) {
	trie := New()
	codes := []struct {
		code  string
		docID uint64
	}{
		{"200", 1},
		{"404", 2},
		{"500", 3},
		{"503", 4},
	}

	for _, c := range codes {
		mustInsert(t, trie, c.code, c.docID)
	}

	for _, c := range codes {
		docs := mustSearch(t, trie, c.code)
		if len(docs) == 0 {
			t.Errorf("status code %q not found", c.code)
			continue
		}
		if docs[0].ID != c.docID {
			t.Errorf("code %q: expected ID=%d, got %d", c.code, c.docID, docs[0].ID)
		}
	}
}

func TestInsert_AlphanumericWord(t *testing.T) {
	trie := New()
	// v1, v2
	mustInsert(t, trie, "v1", 100)
	mustInsert(t, trie, "v2", 200)

	docs := mustSearch(t, trie, "v1")
	if len(docs) != 1 || docs[0].ID != 100 {
		t.Errorf("expected docID=100 for 'v1'")
	}

	docs = mustSearch(t, trie, "v2")
	if len(docs) != 1 || docs[0].ID != 200 {
		t.Errorf("expected docID=200 for 'v2'")
	}
}

// ── Prefix splits (radix логика) ─────────────────────────────────────────────

func TestInsert_CommonPrefix_Split(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "connect", 1)
	mustInsert(t, trie, "connection", 2)
	mustInsert(t, trie, "connected", 3)

	cases := []struct {
		word  string
		docID uint64
	}{
		{"connect", 1},
		{"connection", 2},
		{"connected", 3},
	}

	for _, c := range cases {
		docs := mustSearch(t, trie, c.word)
		if len(docs) == 0 {
			t.Errorf("word %q not found after split", c.word)
			continue
		}
		if docs[0].ID != c.docID {
			t.Errorf("word %q: expected ID=%d, got %d", c.word, c.docID, docs[0].ID)
		}
	}
}

func TestSearch_PrefixNotMatchesLongerWord(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "connection", 1)

	docs := mustSearch(t, trie, "connect")
	if len(docs) != 0 {
		t.Errorf("prefix 'connect' should not match word 'connection', got %d docs", len(docs))
	}
}

// ── Serialize / Load ─────────────────────────────────────────────────────────

func TestSerialize_EmptyTrie(t *testing.T) {
	trie := New()

	var buf bytes.Buffer
	if err := trie.Serialize(&buf); err != nil {
		t.Fatalf("Serialize empty trie: %v", err)
	}

	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load empty trie: %v", err)
	}

	docs := mustSearch(t, loaded, "anything")
	if len(docs) != 0 {
		t.Errorf("expected 0 docs in empty loaded trie, got %d", len(docs))
	}
}

func TestSerialize_RoundTrip_SingleDoc(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "error", 42)

	var buf bytes.Buffer
	if err := trie.Serialize(&buf); err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	docs := mustSearch(t, loaded, "error")
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
	if docs[0].ID != 42 {
		t.Errorf("expected ID=42, got %d", docs[0].ID)
	}
	if docs[0].Count != 1 {
		t.Errorf("expected Count=1, got %d", docs[0].Count)
	}
}

func TestSerialize_RoundTrip_MultipleDocs(t *testing.T) {
	trie := New()

	words := []struct {
		word  string
		docID uint64
		times int
	}{
		{"connection", 1, 3},
		{"connection", 2, 1},
		{"refused", 1, 2},
		{"timeout", 3, 1},
		{"404", 10, 2},
		{"500", 11, 1},
	}

	for _, w := range words {
		for range w.times {
			mustInsert(t, trie, w.word, w.docID)
		}
	}

	var buf bytes.Buffer
	if err := trie.Serialize(&buf); err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	checkCases := []struct {
		word      string
		docID     uint64
		wantCount uint32
	}{
		{"connection", 1, 3},
		{"connection", 2, 1},
		{"refused", 1, 2},
		{"timeout", 3, 1},
		{"404", 10, 2},
		{"500", 11, 1},
	}

	for _, c := range checkCases {
		original := mustSearch(t, trie, c.word)
		restored := mustSearch(t, loaded, c.word)

		origCount := docCount(original, c.docID)
		restCount := docCount(restored, c.docID)

		if origCount != restCount {
			t.Errorf("word=%q docID=%d: original count=%d, restored count=%d",
				c.word, c.docID, origCount, restCount)
		}
		if restCount != c.wantCount {
			t.Errorf("word=%q docID=%d: expected count=%d, got %d",
				c.word, c.docID, c.wantCount, restCount)
		}
	}
}

func TestSerialize_RoundTrip_NotFoundAfterLoad(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "error", 1)

	var buf bytes.Buffer
	if err := trie.Serialize(&buf); err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	docs := mustSearch(t, loaded, "timeout")
	if len(docs) != 0 {
		t.Errorf("expected 0 docs for non-indexed word, got %d", len(docs))
	}
}

func TestLoad_InvalidMagic(t *testing.T) {
	buf := bytes.NewBufferString("INVALID_MAGIC_BYTES_XXXXXXXXXX")
	_, err := Load(buf)
	if err == nil {
		t.Error("expected error for invalid magic bytes, got nil")
	}
}

func TestLoad_EmptyReader(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	_, err := Load(buf)
	if err == nil {
		t.Error("expected error for empty reader, got nil")
	}
}

// ── LoadAsIndex ──────────────────────────────────────────────────────────────

func TestLoadAsIndex_ImplementsInterface(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "test", 1)

	var buf bytes.Buffer
	if err := trie.Serialize(&buf); err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	index, err := LoadAsIndex(&buf)
	if err != nil {
		t.Fatalf("LoadAsIndex: %v", err)
	}

	var _ fts.Index = index

	docs, err := index.Search("test")
	if err != nil {
		t.Fatalf("Search via Index interface: %v", err)
	}
	if len(docs) != 1 || docs[0].ID != 1 {
		t.Errorf("expected doc ID=1, got %+v", docs)
	}
}

// ── Concurrent access ────────────────────────────────────────────────────────

func TestConcurrentInsert(t *testing.T) {
	trie := New()
	done := make(chan struct{})

	for i := range 10 {
		go func(id uint64) {
			mustInsert(t, trie, "concurrent", id)
			done <- struct{}{}
		}(uint64(i))
	}

	for range 10 {
		<-done
	}

	docs := mustSearch(t, trie, "concurrent")
	if len(docs) != 10 {
		t.Errorf("expected 10 docs after concurrent inserts, got %d", len(docs))
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	trie := New()
	mustInsert(t, trie, "base", 0)

	done := make(chan struct{})

	for i := range 5 {
		go func(id uint64) {
			mustInsert(t, trie, "concurrent", id)
			done <- struct{}{}
		}(uint64(i + 1))

		go func() {
			mustSearch(t, trie, "base")
			done <- struct{}{}
		}()
	}

	for range 10 {
		<-done
	}
}

// ── Large dataset ────────────────────────────────────────────────────────────

func TestSerialize_LargeDataset(t *testing.T) {
	trie := New()

	logTokens := []string{
		"error", "warn", "info", "debug", "fatal",
		"connection", "refused", "timeout", "panic",
		"database", "postgres", "redis", "kafka",
		"request", "response", "handler", "middleware",
		"404", "500", "200", "503", "401",
	}

	for docID := uint64(1); docID <= 1000; docID++ {
		token := logTokens[docID%uint64(len(logTokens))]
		mustInsert(t, trie, token, docID)
	}

	var buf bytes.Buffer
	if err := trie.Serialize(&buf); err != nil {
		t.Fatalf("Serialize large dataset: %v", err)
	}

	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load large dataset: %v", err)
	}

	for _, token := range []string{"error", "404", "connection"} {
		original := mustSearch(t, trie, token)
		restored := mustSearch(t, loaded, token)

		if len(original) != len(restored) {
			t.Errorf("token=%q: original len=%d, restored len=%d",
				token, len(original), len(restored))
		}
	}
}

// ── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkInsert(b *testing.B) {
	trie := New()
	words := []string{"error", "connection", "refused", "timeout", "database", "404", "500"}

	for i := 0; b.Loop(); i++ {
		word := words[i%len(words)]
		_ = trie.Insert(word, uint64(i))
	}
}

func BenchmarkSearch(b *testing.B) {
	trie := New()
	words := []string{"error", "connection", "refused", "timeout", "database", "404", "500"}

	for i, w := range words {
		_ = trie.Insert(w, uint64(i))
	}

	for i := 0; b.Loop(); i++ {
		word := words[i%len(words)]
		_, _ = trie.Search(word)
	}
}

func BenchmarkSerialize(b *testing.B) {
	trie := New()
	for i := range uint64(1000) {
		_ = trie.Insert("connection", i)
		_ = trie.Insert("error", i)
		_ = trie.Insert("timeout", i)
	}

	for b.Loop() {
		var buf bytes.Buffer
		_ = trie.Serialize(&buf)
	}
}

func BenchmarkLoad(b *testing.B) {
	trie := New()
	for i := range uint64(1000) {
		_ = trie.Insert("connection", i)
		_ = trie.Insert("error", i)
		_ = trie.Insert("timeout", i)
	}

	var buf bytes.Buffer
	_ = trie.Serialize(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Load(bytes.NewReader(data))
	}
}
