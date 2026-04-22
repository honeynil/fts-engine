package slicedradix

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func benchKeys(n int) []string {
	rng := rand.New(rand.NewSource(1))
	letters := []byte("abcdefghijklmnopqrstuvwxyz")
	keys := make([]string, n)
	for i := range keys {
		size := 4 + rng.Intn(6)
		buf := make([]byte, size)
		for j := range buf {
			buf[j] = letters[rng.Intn(len(letters))]
		}
		keys[i] = string(buf)
	}
	return keys
}

func BenchmarkInsert(b *testing.B) {
	keys := benchKeys(5000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := New()
		for k, word := range keys {
			_ = idx.Insert(word, fts.DocID(fmt.Sprintf("d-%d", k%500)))
		}
	}
}

func BenchmarkSearchHit(b *testing.B) {
	keys := benchKeys(5000)
	idx := New()
	for k, word := range keys {
		_ = idx.Insert(word, fts.DocID(fmt.Sprintf("d-%d", k%500)))
	}
	target := keys[len(keys)/2]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = idx.Search(target)
	}
}

func BenchmarkSearchMiss(b *testing.B) {
	keys := benchKeys(5000)
	idx := New()
	for k, word := range keys {
		_ = idx.Insert(word, fts.DocID(fmt.Sprintf("d-%d", k%500)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = idx.Search("zzzzmissing")
	}
}
