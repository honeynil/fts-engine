package fts

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sort"
	"sync"
)

const snapshotVersion uint16 = 2
const multiIndexSnapshotVersion uint16 = 3

type IndexSnapshotSaver func(index Index, w io.Writer) error
type IndexSnapshotLoader func(r io.Reader) (Index, error)

type FilterSnapshotSaver func(filter Filter, w io.Writer) error
type FilterSnapshotLoader func(r io.Reader) (Filter, error)

type LoadedIndexSnapshot struct {
	IndexName  string
	Index      Index
	Registry   *DocRegistry
	Tombstones *Tombstones
}

type LoadedMultiIndexSnapshot struct {
	Fields     map[string]LoadedIndexSnapshot
	Registry   *DocRegistry
	Tombstones *Tombstones
}

type LoadedFilterSnapshot struct {
	FilterName string
	Filter     Filter
}

type indexEnvelope struct {
	Version      uint16
	IndexName    string
	IndexPayload []byte
	Registry     []DocID
	Tombstones   []uint64
}

type multiIndexField struct {
	FieldName string
	IndexName string
	Payload   []byte
}

type multiIndexEnvelope struct {
	Version    uint16
	Fields     []multiIndexField
	Registry   []DocID
	Tombstones []uint64
}

type filterEnvelope struct {
	Version       uint16
	FilterName    string
	FilterPayload []byte
}

var (
	snapshotRegistryMu   sync.RWMutex
	indexSnapshotCodecs  = make(map[string]indexSnapshotCodec)
	filterSnapshotCodecs = make(map[string]filterSnapshotCodec)
)

type indexSnapshotCodec struct {
	save IndexSnapshotSaver
	load IndexSnapshotLoader
}

type filterSnapshotCodec struct {
	save FilterSnapshotSaver
	load FilterSnapshotLoader
}

func RegisterIndexSnapshotCodec(name string, saver IndexSnapshotSaver, loader IndexSnapshotLoader) error {
	if name == "" {
		return fmt.Errorf("fts: register index snapshot codec: empty name")
	}
	if saver == nil {
		return fmt.Errorf("fts: register index snapshot codec: nil saver")
	}
	if loader == nil {
		return fmt.Errorf("fts: register index snapshot codec: nil loader")
	}

	snapshotRegistryMu.Lock()
	defer snapshotRegistryMu.Unlock()

	if _, exists := indexSnapshotCodecs[name]; exists {
		return fmt.Errorf("fts: register index snapshot codec: duplicate name %q", name)
	}

	indexSnapshotCodecs[name] = indexSnapshotCodec{save: saver, load: loader}
	return nil
}

func RegisterFilterSnapshotCodec(name string, saver FilterSnapshotSaver, loader FilterSnapshotLoader) error {
	if name == "" {
		return fmt.Errorf("fts: register filter snapshot codec: empty name")
	}
	if saver == nil {
		return fmt.Errorf("fts: register filter snapshot codec: nil saver")
	}
	if loader == nil {
		return fmt.Errorf("fts: register filter snapshot codec: nil loader")
	}

	snapshotRegistryMu.Lock()
	defer snapshotRegistryMu.Unlock()

	if _, exists := filterSnapshotCodecs[name]; exists {
		return fmt.Errorf("fts: register filter snapshot codec: duplicate name %q", name)
	}

	filterSnapshotCodecs[name] = filterSnapshotCodec{save: saver, load: loader}
	return nil
}

func SaveIndexSnapshot(w io.Writer, indexName string, index Index, registry *DocRegistry) error {
	return SaveIndexSnapshotWithTombstones(w, indexName, index, registry, nil)
}

func SaveIndexSnapshotWithTombstones(w io.Writer, indexName string, index Index, registry *DocRegistry, tombstones *Tombstones) error {
	if w == nil {
		return fmt.Errorf("fts: save index snapshot: nil writer")
	}
	if index == nil {
		return fmt.Errorf("fts: save index snapshot: nil index")
	}
	if indexName == "" {
		return fmt.Errorf("fts: save index snapshot: empty index name")
	}

	indexCodec, ok := indexCodecByName(indexName)
	if !ok {
		return fmt.Errorf("fts: save index snapshot: unknown index codec %q", indexName)
	}

	var indexPayload bytes.Buffer
	if err := indexCodec.save(index, &indexPayload); err != nil {
		return fmt.Errorf("fts: save index snapshot: encode index %q: %w", indexName, err)
	}

	envelope := indexEnvelope{
		Version:      snapshotVersion,
		IndexName:    indexName,
		IndexPayload: indexPayload.Bytes(),
	}
	if registry != nil {
		envelope.Registry = registry.Snapshot()
	}
	if tombstones != nil && tombstones.Any() {
		envelope.Tombstones = tombstones.Snapshot()
	}

	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("fts: save index snapshot: encode envelope: %w", err)
	}

	return nil
}

func LoadIndexSnapshot(r io.Reader) (*LoadedIndexSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("fts: load index snapshot: nil reader")
	}

	var envelope indexEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("fts: load index snapshot: decode envelope: %w", err)
	}

	if envelope.Version != snapshotVersion {
		return nil, fmt.Errorf("fts: load index snapshot: unsupported version %d", envelope.Version)
	}
	if envelope.IndexName == "" {
		return nil, fmt.Errorf("fts: load index snapshot: empty index name")
	}

	indexCodec, ok := indexCodecByName(envelope.IndexName)
	if !ok {
		return nil, fmt.Errorf("fts: load index snapshot: unknown index codec %q", envelope.IndexName)
	}

	index, err := indexCodec.load(bytes.NewReader(envelope.IndexPayload))
	if err != nil {
		return nil, fmt.Errorf("fts: load index snapshot: decode index %q: %w", envelope.IndexName, err)
	}

	out := &LoadedIndexSnapshot{IndexName: envelope.IndexName, Index: index}
	if len(envelope.Registry) > 0 {
		out.Registry = RestoreDocRegistry(envelope.Registry)
	}
	if len(envelope.Tombstones) > 0 {
		out.Tombstones = RestoreTombstones(envelope.Tombstones)
	}
	return out, nil
}

func SaveMultiIndexSnapshot(w io.Writer, fieldCodecs map[string]string, indexes map[string]Index, registry *DocRegistry) error {
	return SaveMultiIndexSnapshotWithTombstones(w, fieldCodecs, indexes, registry, nil)
}

func SaveMultiIndexSnapshotWithTombstones(w io.Writer, fieldCodecs map[string]string, indexes map[string]Index, registry *DocRegistry, tombstones *Tombstones) error {
	if w == nil {
		return fmt.Errorf("fts: save multi-index snapshot: nil writer")
	}
	if len(indexes) == 0 {
		return fmt.Errorf("fts: save multi-index snapshot: no indexes")
	}

	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)

	fields := make([]multiIndexField, 0, len(indexes))
	for _, name := range names {
		idx := indexes[name]
		if idx == nil {
			return fmt.Errorf("fts: save multi-index snapshot: nil index for field %q", name)
		}

		codecName, ok := fieldCodecs[name]
		if !ok || codecName == "" {
			return fmt.Errorf("fts: save multi-index snapshot: no codec configured for field %q", name)
		}

		indexCodec, ok := indexCodecByName(codecName)
		if !ok {
			return fmt.Errorf("fts: save multi-index snapshot: unknown index codec %q for field %q", codecName, name)
		}

		var payload bytes.Buffer
		if err := indexCodec.save(idx, &payload); err != nil {
			return fmt.Errorf("fts: save multi-index snapshot: encode field %q with codec %q: %w", name, codecName, err)
		}

		fields = append(fields, multiIndexField{
			FieldName: name,
			IndexName: codecName,
			Payload:   payload.Bytes(),
		})
	}

	envelope := multiIndexEnvelope{
		Version: multiIndexSnapshotVersion,
		Fields:  fields,
	}
	if registry != nil {
		envelope.Registry = registry.Snapshot()
	}
	if tombstones != nil && tombstones.Any() {
		envelope.Tombstones = tombstones.Snapshot()
	}

	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("fts: save multi-index snapshot: encode envelope: %w", err)
	}
	return nil
}

func LoadMultiIndexSnapshot(r io.Reader) (*LoadedMultiIndexSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("fts: load multi-index snapshot: nil reader")
	}

	var envelope multiIndexEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("fts: load multi-index snapshot: decode envelope: %w", err)
	}

	if envelope.Version != multiIndexSnapshotVersion {
		return nil, fmt.Errorf("fts: load multi-index snapshot: unsupported version %d", envelope.Version)
	}

	out := &LoadedMultiIndexSnapshot{Fields: make(map[string]LoadedIndexSnapshot, len(envelope.Fields))}
	for _, f := range envelope.Fields {
		if f.FieldName == "" {
			return nil, fmt.Errorf("fts: load multi-index snapshot: empty field name")
		}
		if f.IndexName == "" {
			return nil, fmt.Errorf("fts: load multi-index snapshot: empty index codec name for field %q", f.FieldName)
		}

		indexCodec, ok := indexCodecByName(f.IndexName)
		if !ok {
			return nil, fmt.Errorf("fts: load multi-index snapshot: unknown index codec %q for field %q", f.IndexName, f.FieldName)
		}

		idx, err := indexCodec.load(bytes.NewReader(f.Payload))
		if err != nil {
			return nil, fmt.Errorf("fts: load multi-index snapshot: decode field %q with codec %q: %w", f.FieldName, f.IndexName, err)
		}

		out.Fields[f.FieldName] = LoadedIndexSnapshot{IndexName: f.IndexName, Index: idx}
	}
	if len(envelope.Registry) > 0 {
		out.Registry = RestoreDocRegistry(envelope.Registry)
	}
	if len(envelope.Tombstones) > 0 {
		out.Tombstones = RestoreTombstones(envelope.Tombstones)
	}
	return out, nil
}

func SaveFilterSnapshot(w io.Writer, filterName string, filter Filter) error {
	if w == nil {
		return fmt.Errorf("fts: save filter snapshot: nil writer")
	}
	if filter == nil {
		return fmt.Errorf("fts: save filter snapshot: nil filter")
	}
	if filterName == "" {
		return fmt.Errorf("fts: save filter snapshot: empty filter name")
	}

	if buildable, ok := filter.(BuildableFilter); ok {
		if err := buildable.Build(); err != nil {
			return fmt.Errorf("fts: save filter snapshot: build filter %q: %w", filterName, err)
		}
	}

	filterCodec, ok := filterCodecByName(filterName)
	if !ok {
		return fmt.Errorf("fts: save filter snapshot: unknown filter codec %q", filterName)
	}

	var filterPayload bytes.Buffer
	if err := filterCodec.save(filter, &filterPayload); err != nil {
		return fmt.Errorf("fts: save filter snapshot: encode filter %q: %w", filterName, err)
	}

	envelope := filterEnvelope{
		Version:       snapshotVersion,
		FilterName:    filterName,
		FilterPayload: filterPayload.Bytes(),
	}

	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("fts: save filter snapshot: encode envelope: %w", err)
	}

	return nil
}

func LoadFilterSnapshot(r io.Reader) (*LoadedFilterSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("fts: load filter snapshot: nil reader")
	}

	var envelope filterEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("fts: load filter snapshot: decode envelope: %w", err)
	}

	if envelope.Version != snapshotVersion {
		return nil, fmt.Errorf("fts: load filter snapshot: unsupported version %d", envelope.Version)
	}
	if envelope.FilterName == "" {
		return nil, fmt.Errorf("fts: load filter snapshot: empty filter name")
	}

	filterCodec, ok := filterCodecByName(envelope.FilterName)
	if !ok {
		return nil, fmt.Errorf("fts: load filter snapshot: unknown filter codec %q", envelope.FilterName)
	}

	filter, err := filterCodec.load(bytes.NewReader(envelope.FilterPayload))
	if err != nil {
		return nil, fmt.Errorf("fts: load filter snapshot: decode filter %q: %w", envelope.FilterName, err)
	}

	return &LoadedFilterSnapshot{FilterName: envelope.FilterName, Filter: filter}, nil
}

func indexCodecByName(name string) (indexSnapshotCodec, bool) {
	snapshotRegistryMu.RLock()
	codec, ok := indexSnapshotCodecs[name]
	snapshotRegistryMu.RUnlock()
	return codec, ok
}

func filterCodecByName(name string) (filterSnapshotCodec, bool) {
	snapshotRegistryMu.RLock()
	codec, ok := filterSnapshotCodecs[name]
	snapshotRegistryMu.RUnlock()
	return codec, ok
}
