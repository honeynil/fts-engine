package radixtriesliced

import (
	"encoding/binary"
	"fmt"
	"fts-hw/internal/services/fts"
	"fts-hw/internal/utils"
	"io"
	"sync"
)

var magic = [4]byte{'S', 'R', 'F', 'X'}

const serializeVersion uint16 = 1

type Node struct {
	prefix   string
	children []int
	docs     []fts.Document
}

func (t *Trie) newNode(prefix string) int {
	t.nodes = append(t.nodes, Node{
		prefix: prefix,
	})
	return len(t.nodes) - 1
}

type Trie struct {
	root  int
	nodes []Node
	mu    sync.RWMutex
}

func New() *Trie {
	var t Trie
	t.nodes = make([]Node, 0)
	t.root = t.newNode("")
	return &t
}

func lcp(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func (t *Trie) Insert(word string, docID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.root
	rest := word

	var newNodeIdx int

	for {
		for i, child := range t.nodes[current].children {
			p := lcp(rest, t.nodes[child].prefix)

			if p == 0 {
				continue
			}

			if p == len(t.nodes[child].prefix) {
				current = child
				rest = rest[p:]

				if rest == "" {
					t.addDoc(current, docID)
					return nil
				}

				goto NEXT
			}

			common := t.nodes[child].prefix[:p]
			childSuffix := t.nodes[child].prefix[p:]
			newSuffix := rest[p:]

			middle := t.newNode(common)

			t.nodes[child].prefix = childSuffix
			t.nodes[middle].children = append(t.nodes[middle].children, child)
			t.nodes[current].children[i] = middle

			if newSuffix != "" {
				newNodeIdx = t.newNode(newSuffix)
				t.addDoc(newNodeIdx, docID)
				t.nodes[middle].children = append(t.nodes[middle].children, newNodeIdx)
				return nil
			}

			t.addDoc(middle, docID)
			return nil
		}

		newNodeIdx = t.newNode(rest)
		t.addDoc(newNodeIdx, docID)
		t.nodes[current].children = append(t.nodes[current].children, newNodeIdx)
		return nil

	NEXT:
	}
}

func (t *Trie) addDoc(nodeIdx int, docID uint64) {
	node := &t.nodes[nodeIdx]

	for i := range node.docs {
		if node.docs[i].ID == docID {
			node.docs[i].Count++
			return
		}
	}

	node.docs = append(node.docs, fts.Document{
		ID:    docID,
		Count: 1,
	})
}

func (t *Trie) Search(word string) ([]fts.Document, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	currentIdx := t.root
	rest := word

	for {
		nextNodeIdx, nextRest, matched, exact := t.next(currentIdx, rest)

		if nextNodeIdx == 0 {
			return nil, nil
		}

		if !matched {
			return nil, nil
		}

		if exact {
			return t.nodes[nextNodeIdx].docs, nil
		}

		currentIdx = nextNodeIdx
		rest = nextRest
	}
}

func (t *Trie) next(current int, rest string) (int, string, bool, bool) {
	for _, child := range t.nodes[current].children {
		p := lcp(rest, t.nodes[child].prefix)

		if p == 0 {
			continue
		}

		if p == len(rest) {

			if p == len(t.nodes[child].prefix) && t.nodes[child].IsTerminal() {
				return child, "", true, true
			}
			return 0, "", false, false
		}

		if p == len(t.nodes[child].prefix) {
			return child, rest[p:], true, false
		}

		return 0, "", false, false
	}

	return 0, "", false, false
}

// ── Serialization ────────────────────────────────────────────────────────────

// Serialize сохраняет весь Trie в writer в бинарном формате.
//
// Формат файла (.fidx):
//
//	[4]  magic bytes "SRFX"
//	[2]  version uint16
//	[4]  node_count uint32
//	--- для каждой ноды ---
//	[2]  prefix_len uint16
//	[N]  prefix bytes
//	[4]  children_count uint32
//	[4]  children[i] int32 (индекс в t.nodes)
//	[4]  docs_count uint32
//	--- для каждого документа ---
//	[8]  doc_id uint64
//	[4]  count uint32
func (t *Trie) Serialize(w io.Writer) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if _, err := w.Write(magic[:]); err != nil {
		return fmt.Errorf("slicedradix: serialize: write magic: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, serializeVersion); err != nil {
		return fmt.Errorf("slicedradix: serialize: write version: %w", err)
	}

	nodeCount := uint32(len(t.nodes))
	if err := binary.Write(w, binary.LittleEndian, nodeCount); err != nil {
		return fmt.Errorf("slicedradix: serialize: write node count: %w", err)
	}

	for i, node := range t.nodes {
		prefixBytes := []byte(node.prefix)
		prefixLen := uint16(len(prefixBytes))
		if err := binary.Write(w, binary.LittleEndian, prefixLen); err != nil {
			return fmt.Errorf("slicedradix: serialize: node %d: write prefix len: %w", i, err)
		}
		if prefixLen > 0 {
			if _, err := w.Write(prefixBytes); err != nil {
				return fmt.Errorf("slicedradix: serialize: node %d: write prefix: %w", i, err)
			}
		}

		childCount := uint32(len(node.children))
		if err := binary.Write(w, binary.LittleEndian, childCount); err != nil {
			return fmt.Errorf("slicedradix: serialize: node %d: write children count: %w", i, err)
		}
		for _, child := range node.children {
			if err := binary.Write(w, binary.LittleEndian, int32(child)); err != nil {
				return fmt.Errorf("slicedradix: serialize: node %d: write child: %w", i, err)
			}
		}

		docsCount := uint32(len(node.docs))
		if err := binary.Write(w, binary.LittleEndian, docsCount); err != nil {
			return fmt.Errorf("slicedradix: serialize: node %d: write docs count: %w", i, err)
		}
		for _, doc := range node.docs {
			if err := binary.Write(w, binary.LittleEndian, doc.ID); err != nil {
				return fmt.Errorf("slicedradix: serialize: node %d: write doc id: %w", i, err)
			}
			if err := binary.Write(w, binary.LittleEndian, doc.Count); err != nil {
				return fmt.Errorf("slicedradix: serialize: node %d: write doc count: %w", i, err)
			}
		}
	}

	return nil
}

func Load(r io.Reader) (*Trie, error) {
	var readMagic [4]byte
	if _, err := io.ReadFull(r, readMagic[:]); err != nil {
		return nil, fmt.Errorf("slicedradix: load: read magic: %w", err)
	}
	if readMagic != magic {
		return nil, fmt.Errorf("slicedradix: load: invalid magic bytes, got %v", readMagic)
	}

	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("slicedradix: load: read version: %w", err)
	}
	if version != serializeVersion {
		return nil, fmt.Errorf("slicedradix: load: unsupported version %d, expected %d", version, serializeVersion)
	}

	var nodeCount uint32
	if err := binary.Read(r, binary.LittleEndian, &nodeCount); err != nil {
		return nil, fmt.Errorf("slicedradix: load: read node count: %w", err)
	}

	nodes := make([]Node, nodeCount)

	for i := uint32(0); i < nodeCount; i++ {

		var prefixLen uint16
		if err := binary.Read(r, binary.LittleEndian, &prefixLen); err != nil {
			return nil, fmt.Errorf("slicedradix: load: node %d: read prefix len: %w", i, err)
		}
		if prefixLen > 0 {
			prefixBytes := make([]byte, prefixLen)
			if _, err := io.ReadFull(r, prefixBytes); err != nil {
				return nil, fmt.Errorf("slicedradix: load: node %d: read prefix: %w", i, err)
			}
			nodes[i].prefix = string(prefixBytes)
		}

		var childCount uint32
		if err := binary.Read(r, binary.LittleEndian, &childCount); err != nil {
			return nil, fmt.Errorf("slicedradix: load: node %d: read children count: %w", i, err)
		}
		if childCount > 0 {
			nodes[i].children = make([]int, childCount)
			for j := uint32(0); j < childCount; j++ {
				var child int32
				if err := binary.Read(r, binary.LittleEndian, &child); err != nil {
					return nil, fmt.Errorf("slicedradix: load: node %d: read child %d: %w", i, j, err)
				}
				nodes[i].children[j] = int(child)
			}
		}

		var docsCount uint32
		if err := binary.Read(r, binary.LittleEndian, &docsCount); err != nil {
			return nil, fmt.Errorf("slicedradix: load: node %d: read docs count: %w", i, err)
		}
		if docsCount > 0 {
			nodes[i].docs = make([]fts.Document, docsCount)
			for j := uint32(0); j < docsCount; j++ {
				if err := binary.Read(r, binary.LittleEndian, &nodes[i].docs[j].ID); err != nil {
					return nil, fmt.Errorf("slicedradix: load: node %d: doc %d: read id: %w", i, j, err)
				}
				if err := binary.Read(r, binary.LittleEndian, &nodes[i].docs[j].Count); err != nil {
					return nil, fmt.Errorf("slicedradix: load: node %d: doc %d: read count: %w", i, j, err)
				}
			}
		}
	}

	return &Trie{
		root:  0,
		nodes: nodes,
	}, nil
}

func LoadAsIndex(r io.Reader) (fts.Index, error) {
	return Load(r)
}

// ── Analyze ──────────────────────────────────────────────────────────────────

func (t *Trie) Analyze() utils.TrieStats {
	var s utils.TrieStats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var dfs func(n int, depth int)
	dfs = func(n int, depth int) {
		s.Nodes++
		totalDepth += depth

		if t.nodes[n].IsTerminal() {
			s.Leaves++
		}
		if depth > s.MaxDepth {
			s.MaxDepth = depth
		}
		s.TotalDocs += len(t.nodes[n].docs)

		numChildren := len(t.nodes[n].children)
		s.TotalChildren += numChildren
		levelChildrenSum[depth] += numChildren
		levelNodeCount[depth]++

		for _, c := range t.nodes[n].children {
			dfs(c, depth+1)
		}
	}

	dfs(t.root, 0)

	if s.Nodes > 0 {
		s.AvgDepth = float64(totalDepth) / float64(s.Nodes)
	}

	for depth := 0; depth <= 3; depth++ {
		if levelNodeCount[depth] > 0 {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel,
				float64(levelChildrenSum[depth])/float64(levelNodeCount[depth]))
		} else {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel, 0)
		}
	}

	return s
}

func (n *Node) IsTerminal() bool {
	return len(n.docs) > 0
}
