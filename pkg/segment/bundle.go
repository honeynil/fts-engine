package segment

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// Bundle is the atomic on-disk packaging of a sealed segment together
// with the DocID↔DocOrd registry and tombstone bitmap needed to
// resurrect a fully-functional fts.Service.
//
// Layout:
//
//	[Header 8B]      magic "FTSB" + version u16 + flags u16
//	[Segment block]  uvarint length + segment bytes (see segment.go)
//	[Registry block] uvarint length + DocID list (uvarint count, then per
//	                 entry: uvarint len + bytes)
//	[Tombstone block] uvarint length + raw little-endian uint64 words
//	[Trailer 4B]     magic "FTSB"
//
// Numbers are little-endian.
type Bundle struct {
	Reader     *Reader
	Registry   *fts.DocRegistry
	Tombstones *fts.Tombstones

	closer func() error
}

// Close releases the mmap (if any) backing the segment reader. Idempotent.
func (b *Bundle) Close() error {
	if b == nil || b.closer == nil {
		return nil
	}
	err := b.closer()
	b.closer = nil
	return err
}

const (
	bundleMagic   = "FTSB"
	bundleVersion = uint16(1)
)

// SegmentSource is anything that can yield its terms — typically
// *slicedradix.Index. The bundle Save path walks it once.
type SegmentSource interface {
	Walk(visit func(term string, postings []fts.Posting, positions [][]uint32) bool)
}

// SaveBundle writes a bundle file atomically: data is staged at
// path+".tmp", fsynced, then renamed over path.
func SaveBundle(path string, source SegmentSource, registry *fts.DocRegistry, tombstones *fts.Tombstones) error {
	if path == "" {
		return fmt.Errorf("bundle: empty path")
	}
	if source == nil {
		return fmt.Errorf("bundle: nil segment source")
	}

	terms := walkToTerms(source)
	segBlob, err := Build(terms)
	if err != nil {
		return fmt.Errorf("bundle: build segment: %w", err)
	}
	regBlob := encodeRegistry(registry)
	tombBlob := encodeTombstones(tombstones)

	body := assemble(segBlob, regBlob, tombBlob)
	return writeAtomic(path, body)
}

// OpenBundle mmaps the bundle at path and returns a ready-to-use Bundle.
// Call Close when done.
func OpenBundle(path string) (*Bundle, error) {
	data, closer, err := openMmap(path)
	if err != nil {
		return nil, err
	}
	b, parseErr := parseBundle(data)
	if parseErr != nil {
		_ = closer()
		return nil, parseErr
	}
	b.closer = closer
	return b, nil
}

// OpenBundleBytes parses an in-memory bundle (no mmap).
// Useful for tests or when bytes already live in process memory.
func OpenBundleBytes(data []byte) (*Bundle, error) {
	return parseBundle(data)
}

func walkToTerms(source SegmentSource) []TermPostings {
	var out []TermPostings
	source.Walk(func(term string, postings []fts.Posting, positions [][]uint32) bool {
		tp := TermPostings{
			Term:     term,
			Postings: append([]fts.Posting(nil), postings...),
		}
		if len(positions) > 0 {
			tp.Positions = make([][]uint32, len(positions))
			for i, p := range positions {
				tp.Positions[i] = append([]uint32(nil), p...)
			}
		}
		out = append(out, tp)
		return true
	})
	return out
}

func encodeRegistry(r *fts.DocRegistry) []byte {
	if r == nil {
		var buf []byte
		buf = binary.AppendUvarint(buf, 0)
		return buf
	}
	ids := r.Snapshot()
	var buf []byte
	buf = binary.AppendUvarint(buf, uint64(len(ids)))
	for _, id := range ids {
		buf = binary.AppendUvarint(buf, uint64(len(id)))
		buf = append(buf, id...)
	}
	return buf
}

func decodeRegistry(blob []byte) (*fts.DocRegistry, error) {
	if len(blob) == 0 {
		return fts.NewDocRegistry(), nil
	}
	count, n := binary.Uvarint(blob)
	if n <= 0 {
		return nil, fmt.Errorf("bundle: bad registry count")
	}
	pos := n
	ids := make([]fts.DocID, 0, count)
	for range count {
		l, m := binary.Uvarint(blob[pos:])
		if m <= 0 {
			return nil, fmt.Errorf("bundle: bad DocID length")
		}
		pos += m
		if pos+int(l) > len(blob) {
			return nil, fmt.Errorf("bundle: DocID overflow")
		}
		ids = append(ids, fts.DocID(blob[pos:pos+int(l)]))
		pos += int(l)
	}
	return fts.RestoreDocRegistry(ids), nil
}

func encodeTombstones(t *fts.Tombstones) []byte {
	if t == nil || !t.Any() {
		var buf []byte
		buf = binary.AppendUvarint(buf, 0)
		return buf
	}
	words := t.Snapshot()
	var buf []byte
	buf = binary.AppendUvarint(buf, uint64(len(words)))
	for _, w := range words {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], w)
		buf = append(buf, b[:]...)
	}
	return buf
}

func decodeTombstones(blob []byte) (*fts.Tombstones, error) {
	if len(blob) == 0 {
		return fts.NewTombstones(), nil
	}
	count, n := binary.Uvarint(blob)
	if n <= 0 {
		return nil, fmt.Errorf("bundle: bad tombstones count")
	}
	pos := n
	if count == 0 {
		return fts.NewTombstones(), nil
	}
	if pos+int(count)*8 > len(blob) {
		return nil, fmt.Errorf("bundle: tombstones overflow")
	}
	words := make([]uint64, count)
	for i := range count {
		words[i] = binary.LittleEndian.Uint64(blob[pos:])
		pos += 8
	}
	return fts.RestoreTombstones(words), nil
}

func assemble(segBlob, regBlob, tombBlob []byte) []byte {
	out := make([]byte, 0, 8+len(segBlob)+len(regBlob)+len(tombBlob)+30)
	// Header
	out = append(out, bundleMagic...)
	var v [4]byte
	binary.LittleEndian.PutUint16(v[0:2], bundleVersion)
	binary.LittleEndian.PutUint16(v[2:4], 0)
	out = append(out, v[:]...)
	// Sections
	out = binary.AppendUvarint(out, uint64(len(segBlob)))
	out = append(out, segBlob...)
	out = binary.AppendUvarint(out, uint64(len(regBlob)))
	out = append(out, regBlob...)
	out = binary.AppendUvarint(out, uint64(len(tombBlob)))
	out = append(out, tombBlob...)
	// Trailer
	out = append(out, bundleMagic...)
	return out
}

func parseBundle(data []byte) (*Bundle, error) {
	if len(data) < 8+4 {
		return nil, fmt.Errorf("bundle: too short (%d bytes)", len(data))
	}
	if string(data[:4]) != bundleMagic {
		return nil, fmt.Errorf("bundle: bad header magic")
	}
	v := binary.LittleEndian.Uint16(data[4:6])
	if v != bundleVersion {
		return nil, fmt.Errorf("bundle: unsupported version %d", v)
	}
	if string(data[len(data)-4:]) != bundleMagic {
		return nil, fmt.Errorf("bundle: bad trailer magic")
	}

	pos := 8
	segLen, n := binary.Uvarint(data[pos:])
	if n <= 0 || pos+n+int(segLen) > len(data)-4 {
		return nil, fmt.Errorf("bundle: bad segment length")
	}
	pos += n
	segBlob := data[pos : pos+int(segLen)]
	pos += int(segLen)

	regLen, n := binary.Uvarint(data[pos:])
	if n <= 0 || pos+n+int(regLen) > len(data)-4 {
		return nil, fmt.Errorf("bundle: bad registry length")
	}
	pos += n
	regBlob := data[pos : pos+int(regLen)]
	pos += int(regLen)

	tombLen, n := binary.Uvarint(data[pos:])
	if n <= 0 || pos+n+int(tombLen) > len(data)-4 {
		return nil, fmt.Errorf("bundle: bad tombstones length")
	}
	pos += n
	tombBlob := data[pos : pos+int(tombLen)]

	r, err := Open(segBlob)
	if err != nil {
		return nil, fmt.Errorf("bundle: parse segment: %w", err)
	}
	reg, err := decodeRegistry(regBlob)
	if err != nil {
		return nil, err
	}
	tomb, err := decodeTombstones(tombBlob)
	if err != nil {
		return nil, err
	}

	return &Bundle{Reader: r, Registry: reg, Tombstones: tomb}, nil
}

// writeAtomic writes data to path via a tmp file + fsync + rename.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".bundle-*.tmp")
	if err != nil {
		return fmt.Errorf("bundle: create tmp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("bundle: write tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("bundle: fsync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("bundle: close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return fmt.Errorf("bundle: rename: %w", err)
	}
	return nil
}
