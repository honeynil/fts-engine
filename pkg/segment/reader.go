package segment

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// Reader queries a sealed segment by holding the file bytes and an
// in-memory term lookup table. It implements fts.Index, fts.PositionalIndex,
// and fts.PrefixIndex.
//
// Reader is read-only; Insert returns an error.
type Reader struct {
	bytes []byte

	// terms is sorted lexicographically — supports both exact lookup (binary
	// search) and prefix scans.
	terms []termEntry

	postingsBase  uint64
	positionsBase uint64
}

type termEntry struct {
	term         string
	postingsOff  uint64
	postingsLen  uint64
	positionsOff uint64
	positionsLen uint64
	hasPositions bool
}

// MappedReader is a Reader backed by a memory-mapped file. Close
// unmaps the region; the embedded *Reader must not be used afterwards.
type MappedReader struct {
	*Reader
	closer func() error
}

// Close unmaps the underlying file region.
func (m *MappedReader) Close() error {
	if m == nil || m.closer == nil {
		return nil
	}
	return m.closer()
}

// OpenFile mmaps the segment file at path (Linux/macOS/FreeBSD; falls
// back to ReadFile on other platforms) and parses its term index. The
// returned MappedReader must be Closed when no longer needed.
//
// On Linux/Darwin, the actual posting and position bytes stay in the OS
// page cache rather than the Go heap — only the term dictionary and
// per-query decoded postings live on heap. For large segments this is
// the difference between gigabytes-of-heap and megabytes-of-heap.
func OpenFile(path string) (*MappedReader, error) {
	data, closer, err := openMmap(path)
	if err != nil {
		return nil, err
	}
	r, err := Open(data)
	if err != nil {
		_ = closer()
		return nil, err
	}
	return &MappedReader{Reader: r, closer: closer}, nil
}

// Open parses the segment headers and term index from the given byte
// slice. The slice must remain valid for the lifetime of the Reader; it
// is referenced, not copied.
func Open(data []byte) (*Reader, error) {
	if len(data) < headerLen+footerLen {
		return nil, fmt.Errorf("segment: too short (%d bytes)", len(data))
	}
	if string(data[:4]) != magic {
		return nil, fmt.Errorf("segment: bad header magic")
	}
	if v := binary.LittleEndian.Uint16(data[4:6]); v != version {
		return nil, fmt.Errorf("segment: unsupported version %d", v)
	}

	footer := data[len(data)-footerLen:]
	if string(footer[16:20]) != magic {
		return nil, fmt.Errorf("segment: bad footer magic")
	}
	indexOff := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	if indexOff+indexLen > uint64(len(data)-footerLen) {
		return nil, fmt.Errorf("segment: index range out of bounds")
	}

	idx := data[indexOff : indexOff+indexLen]
	count, n := binary.Uvarint(idx)
	if n <= 0 {
		return nil, fmt.Errorf("segment: bad term count")
	}
	pos := n
	terms := make([]termEntry, 0, count)
	for range count {
		termLen, m := binary.Uvarint(idx[pos:])
		if m <= 0 {
			return nil, fmt.Errorf("segment: bad termLen")
		}
		pos += m
		if pos+int(termLen) > len(idx) {
			return nil, fmt.Errorf("segment: term bytes overflow")
		}
		term := string(idx[pos : pos+int(termLen)])
		pos += int(termLen)

		postingsOff, m1 := binary.Uvarint(idx[pos:])
		pos += m1
		postingsLen, m2 := binary.Uvarint(idx[pos:])
		pos += m2
		positionsOff, m3 := binary.Uvarint(idx[pos:])
		pos += m3
		positionsLen, m4 := binary.Uvarint(idx[pos:])
		pos += m4
		if m1 <= 0 || m2 <= 0 || m3 <= 0 || m4 <= 0 {
			return nil, fmt.Errorf("segment: bad term entry")
		}
		hasPositions := idx[pos] != 0
		pos++

		terms = append(terms, termEntry{
			term:         term,
			postingsOff:  postingsOff,
			postingsLen:  postingsLen,
			positionsOff: positionsOff,
			positionsLen: positionsLen,
			hasPositions: hasPositions,
		})
	}

	r := &Reader{
		bytes:         data,
		terms:         terms,
		postingsBase:  uint64(headerLen),
		positionsBase: 0,
	}
	// Positions blob starts immediately after the postings blob;
	// locate its base by finding the largest postings end.
	var postingsEnd uint64
	for _, t := range terms {
		end := t.postingsOff + t.postingsLen
		if end > postingsEnd {
			postingsEnd = end
		}
	}
	r.positionsBase = uint64(headerLen) + postingsEnd
	return r, nil
}

// (read-only).
func (r *Reader) Bytes() []byte { return r.bytes }

// TermCount returns the number of terms in the segment.
func (r *Reader) TermCount() int { return len(r.terms) }

func (r *Reader) findTerm(term string) (termEntry, bool) {
	i := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].term >= term })
	if i < len(r.terms) && r.terms[i].term == term {
		return r.terms[i], true
	}
	return termEntry{}, false
}

// Insert is unsupported on sealed segments.
func (r *Reader) Insert(key string, ord fts.DocOrd) error {
	return fmt.Errorf("segment: read-only (Insert called on sealed segment)")
}

// InsertAt is unsupported on sealed segments.
func (r *Reader) InsertAt(key string, ord fts.DocOrd, position uint32) error {
	return fmt.Errorf("segment: read-only (InsertAt called on sealed segment)")
}

// Search returns the postings for a term, decoding from bytes on demand.
func (r *Reader) Search(key string) ([]fts.Posting, error) {
	e, ok := r.findTerm(key)
	if !ok {
		return nil, nil
	}
	return decodePostings(r.bytes[r.postingsBase+e.postingsOff : r.postingsBase+e.postingsOff+e.postingsLen]), nil
}

// SearchPositional returns postings + positions for a term.
func (r *Reader) SearchPositional(key string) ([]fts.PositionalPosting, error) {
	e, ok := r.findTerm(key)
	if !ok {
		return nil, nil
	}
	postings := decodePostings(r.bytes[r.postingsBase+e.postingsOff : r.postingsBase+e.postingsOff+e.postingsLen])
	out := make([]fts.PositionalPosting, len(postings))
	if !e.hasPositions {
		for i, p := range postings {
			out[i] = fts.PositionalPosting{Ord: p.Ord}
		}
		return out, nil
	}
	posBuf := r.bytes[r.positionsBase+e.positionsOff : r.positionsBase+e.positionsOff+e.positionsLen]
	cur := 0
	for i, p := range postings {
		count, n := binary.Uvarint(posBuf[cur:])
		if n <= 0 {
			return nil, fmt.Errorf("segment: bad position count for term %q", key)
		}
		cur += n
		positions := make([]uint32, 0, count)
		var prev uint32
		for range count {
			delta, m := binary.Uvarint(posBuf[cur:])
			if m <= 0 {
				return nil, fmt.Errorf("segment: bad position delta for term %q", key)
			}
			cur += m
			prev += uint32(delta)
			positions = append(positions, prev)
		}
		out[i] = fts.PositionalPosting{Ord: p.Ord, Positions: positions}
	}
	return out, nil
}

// SearchPrefix returns aggregated postings for all terms with the given
// prefix. Counts are summed per ord across matching terms.
func (r *Reader) SearchPrefix(prefix string) ([]fts.Posting, error) {
	if prefix == "" {
		return nil, nil
	}
	start := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].term >= prefix })
	aggregated := make(map[fts.DocOrd]uint32)
	var order []fts.DocOrd
	for i := start; i < len(r.terms); i++ {
		t := r.terms[i].term
		if len(t) < len(prefix) || t[:len(prefix)] != prefix {
			break
		}
		postings := decodePostings(r.bytes[r.postingsBase+r.terms[i].postingsOff : r.postingsBase+r.terms[i].postingsOff+r.terms[i].postingsLen])
		for _, p := range postings {
			if _, seen := aggregated[p.Ord]; !seen {
				order = append(order, p.Ord)
			}
			aggregated[p.Ord] += p.Count
		}
	}
	out := make([]fts.Posting, 0, len(order))
	for _, ord := range order {
		out = append(out, fts.Posting{Ord: ord, Count: aggregated[ord]})
	}
	return out, nil
}

func decodePostings(blob []byte) []fts.Posting {
	if len(blob) == 0 {
		return nil
	}
	count, n := binary.Uvarint(blob)
	if n <= 0 || count == 0 {
		return nil
	}
	pos := n
	out := make([]fts.Posting, 0, count)
	var ord fts.DocOrd
	for range count {
		delta, m := binary.Uvarint(blob[pos:])
		if m <= 0 {
			return out
		}
		pos += m
		count2, m2 := binary.Uvarint(blob[pos:])
		if m2 <= 0 {
			return out
		}
		pos += m2
		ord += fts.DocOrd(delta)
		out = append(out, fts.Posting{Ord: ord, Count: uint32(count2)})
	}
	return out
}

var (
	_ fts.Index           = (*Reader)(nil)
	_ fts.PositionalIndex = (*Reader)(nil)
	_ fts.PrefixIndex     = (*Reader)(nil)
)
