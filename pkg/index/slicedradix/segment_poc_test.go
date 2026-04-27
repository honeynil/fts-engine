package slicedradix

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestSegmentHeapPOC(t *testing.T) {
	if testing.Short() {
		t.Skip("heap POC is slow")
	}

	const (
		docs         = 5000
		tokensPerDoc = 100
		vocabSize    = 500
	)

	rng := rand.New(rand.NewSource(42))
	vocab := make([]string, vocabSize)
	for i := range vocab {
		vocab[i] = fmt.Sprintf("w%04d", i)
	}

	idx := New()
	for d := range docs {
		ord := fts.DocOrd(d)
		for pos := range tokensPerDoc {
			token := vocab[rng.Intn(vocabSize)]
			if err := idx.InsertAt(token, ord, uint32(pos)); err != nil {
				t.Fatalf("InsertAt: %v", err)
			}
		}
	}
	runtime.GC()
	runtime.GC()
	var inMemStats runtime.MemStats
	runtime.ReadMemStats(&inMemStats)
	runtime.KeepAlive(idx)
	inMemKB := inMemStats.HeapAlloc / 1024

	var buf bytes.Buffer
	if err := idx.Serialize(&buf); err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	blob := buf.Bytes()
	blobKB := uint64(len(blob)) / 1024

	idx = nil
	runtime.GC()
	runtime.GC()
	var blobOnlyStats runtime.MemStats
	runtime.ReadMemStats(&blobOnlyStats)
	runtime.KeepAlive(blob)
	blobOnlyHeapKB := blobOnlyStats.HeapAlloc / 1024

	t.Logf("slicedradix in-memory heap: %d KB", inMemKB)
	t.Logf("serialized blob size:       %d KB", blobKB)
	t.Logf("heap when only blob alive:  %d KB", blobOnlyHeapKB)
	t.Logf("ratio (in-mem / blob-only): %.2fx", float64(inMemKB)/float64(blobOnlyHeapKB))
}
