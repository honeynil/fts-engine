package slicedradix

import (
	"encoding/gob"
	"fmt"
	"io"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

type node struct {
	prefix    string
	children  []int
	postings  []fts.Posting
	positions [][]uint32
}

type Index struct {
	root  int
	nodes []node
	mu    sync.RWMutex
}

type snapshotNode struct {
	Prefix    string
	Children  []int
	Postings  []fts.Posting
	Positions [][]uint32
}

type snapshotIndex struct {
	Root  int
	Nodes []snapshotNode
}

func New() *Index {
	var t Index
	t.root = t.newNode("")
	return &t
}

func (t *Index) Serialize(w io.Writer) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	snap := snapshotIndex{
		Root:  t.root,
		Nodes: make([]snapshotNode, 0, len(t.nodes)),
	}

	for i := range t.nodes {
		n := t.nodes[i]
		var positions [][]uint32
		if len(n.positions) > 0 {
			positions = make([][]uint32, len(n.positions))
			for j, p := range n.positions {
				positions[j] = append([]uint32(nil), p...)
			}
		}
		snap.Nodes = append(snap.Nodes, snapshotNode{
			Prefix:    n.prefix,
			Children:  append([]int(nil), n.children...),
			Postings:  append([]fts.Posting(nil), n.postings...),
			Positions: positions,
		})
	}

	if err := gob.NewEncoder(w).Encode(snap); err != nil {
		return fmt.Errorf("slicedradix: serialize: %w", err)
	}

	return nil
}

func Load(r io.Reader) (fts.Index, error) {
	var snap snapshotIndex
	if err := gob.NewDecoder(r).Decode(&snap); err != nil {
		return nil, fmt.Errorf("slicedradix: load: %w", err)
	}

	idx := &Index{
		root:  snap.Root,
		nodes: make([]node, 0, len(snap.Nodes)),
	}

	for i := range snap.Nodes {
		s := snap.Nodes[i]
		var positions [][]uint32
		if len(s.Positions) > 0 {
			positions = make([][]uint32, len(s.Positions))
			for j, p := range s.Positions {
				positions[j] = append([]uint32(nil), p...)
			}
		}
		idx.nodes = append(idx.nodes, node{
			prefix:    s.Prefix,
			children:  append([]int(nil), s.Children...),
			postings:  append([]fts.Posting(nil), s.Postings...),
			positions: positions,
		})
	}

	return idx, nil
}

func (t *Index) newNode(prefix string) int {
	t.nodes = append(t.nodes, node{prefix: prefix})
	return len(t.nodes) - 1
}

func lcp(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func (t *Index) Insert(word string, ord fts.DocOrd) error {
	return t.insert(word, ord, false, 0)
}

func (t *Index) InsertAt(word string, ord fts.DocOrd, position uint32) error {
	return t.insert(word, ord, true, position)
}

func (t *Index) insert(word string, ord fts.DocOrd, hasPos bool, pos uint32) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.root
	rest := word

	for {
		advanced := false
		for i, child := range t.nodes[current].children {
			p := lcp(rest, t.nodes[child].prefix)
			if p == 0 {
				continue
			}

			if p == len(t.nodes[child].prefix) {
				current = child
				rest = rest[p:]
				if rest == "" {
					t.recordDoc(current, ord, hasPos, pos)
					return nil
				}
				advanced = true
				break
			}

			common := t.nodes[child].prefix[:p]
			childSuffix := t.nodes[child].prefix[p:]
			newSuffix := rest[p:]

			middle := t.newNode(common)
			t.nodes[child].prefix = childSuffix
			t.nodes[middle].children = append(t.nodes[middle].children, child)
			t.nodes[current].children[i] = middle

			if newSuffix != "" {
				newIdx := t.newNode(newSuffix)
				t.recordDoc(newIdx, ord, hasPos, pos)
				t.nodes[middle].children = append(t.nodes[middle].children, newIdx)
				return nil
			}

			t.recordDoc(middle, ord, hasPos, pos)
			return nil
		}

		if advanced {
			continue
		}

		newIdx := t.newNode(rest)
		t.recordDoc(newIdx, ord, hasPos, pos)
		t.nodes[current].children = append(t.nodes[current].children, newIdx)
		return nil
	}
}

func (t *Index) recordDoc(nodeIdx int, ord fts.DocOrd, hasPos bool, pos uint32) {
	n := &t.nodes[nodeIdx]
	if last := len(n.postings) - 1; last >= 0 && n.postings[last].Ord == ord {
		n.postings[last].Count++
		if hasPos {
			t.growPositions(nodeIdx, len(n.postings))
			n.positions[last] = append(n.positions[last], pos)
		}
		return
	}

	for i := range n.postings {
		if n.postings[i].Ord == ord {
			n.postings[i].Count++
			if hasPos {
				t.growPositions(nodeIdx, len(n.postings))
				n.positions[i] = append(n.positions[i], pos)
			}
			return
		}
	}
	n.postings = append(n.postings, fts.Posting{Ord: ord, Count: 1})
	if hasPos {
		t.growPositions(nodeIdx, len(n.postings))
		last := len(n.postings) - 1
		n.positions[last] = append(n.positions[last], pos)
	}
}

func (t *Index) growPositions(nodeIdx int, want int) {
	n := &t.nodes[nodeIdx]
	for len(n.positions) < want {
		n.positions = append(n.positions, nil)
	}
}

func (t *Index) Search(word string) ([]fts.Posting, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.root
	rest := word

	for {
		nextNode, nextRest, matched, exact := t.next(current, rest)
		if nextNode == 0 || !matched {
			return nil, nil
		}
		if exact {
			return t.nodes[nextNode].postings, nil
		}
		current = nextNode
		rest = nextRest
	}
}

func (t *Index) SearchPositional(word string) ([]fts.PositionalPosting, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.root
	rest := word

	for {
		nextNode, nextRest, matched, exact := t.next(current, rest)
		if nextNode == 0 || !matched {
			return nil, nil
		}
		if exact {
			n := &t.nodes[nextNode]
			out := make([]fts.PositionalPosting, 0, len(n.postings))
			for i := range n.postings {
				var positions []uint32
				if i < len(n.positions) {
					positions = n.positions[i]
				}
				out = append(out, fts.PositionalPosting{
					Ord:       n.postings[i].Ord,
					Positions: positions,
				})
			}
			return out, nil
		}
		current = nextNode
		rest = nextRest
	}
}

func (t *Index) SearchPrefix(prefix string) ([]fts.Posting, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if prefix == "" {
		return t.collectSubtree(t.root), nil
	}

	current := t.root
	rest := prefix
	for {
		nextNode, nextRest, matched, spansPrefix := t.prefixDescend(current, rest)
		if !matched {
			return nil, nil
		}
		if spansPrefix {
			return t.collectSubtree(nextNode), nil
		}
		current = nextNode
		rest = nextRest
	}
}

func (t *Index) prefixDescend(current int, rest string) (int, string, bool, bool) {
	for _, child := range t.nodes[current].children {
		childPrefix := t.nodes[child].prefix
		p := lcp(rest, childPrefix)
		if p == 0 {
			continue
		}
		if p == len(rest) {
			return child, "", true, true
		}
		if p == len(childPrefix) {
			return child, rest[p:], true, false
		}
		return 0, "", false, false
	}
	return 0, "", false, false
}

func (t *Index) collectSubtree(start int) []fts.Posting {
	aggregated := make(map[fts.DocOrd]uint32)
	var order []fts.DocOrd

	var dfs func(n int)
	dfs = func(n int) {
		for _, p := range t.nodes[n].postings {
			if _, seen := aggregated[p.Ord]; !seen {
				order = append(order, p.Ord)
			}
			aggregated[p.Ord] += p.Count
		}
		for _, c := range t.nodes[n].children {
			dfs(c)
		}
	}
	dfs(start)

	out := make([]fts.Posting, 0, len(order))
	for _, ord := range order {
		out = append(out, fts.Posting{Ord: ord, Count: aggregated[ord]})
	}
	return out
}

func (t *Index) next(current int, rest string) (int, string, bool, bool) {
	for _, child := range t.nodes[current].children {
		p := lcp(rest, t.nodes[child].prefix)
		if p == 0 {
			continue
		}
		if p == len(rest) {
			if p == len(t.nodes[child].prefix) && t.nodes[child].isTerminal() {
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

func (n *node) isTerminal() bool {
	return len(n.postings) > 0
}

func (t *Index) Analyze() fts.Stats {
	var s fts.Stats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var dfs func(n int, depth int)
	dfs = func(n int, depth int) {
		s.Nodes++
		totalDepth += depth
		if t.nodes[n].isTerminal() {
			s.Leaves++
		}
		if depth > s.MaxDepth {
			s.MaxDepth = depth
		}
		s.TotalDocs += len(t.nodes[n].postings)

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

func (t *Index) Walk(visit func(term string, postings []fts.Posting, positions [][]uint32) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.walkFrom(t.root, "", visit)
}

func (t *Index) walkFrom(nodeIdx int, prefix string, visit func(string, []fts.Posting, [][]uint32) bool) bool {
	n := &t.nodes[nodeIdx]
	term := prefix + n.prefix
	if n.isTerminal() {
		if !visit(term, n.postings, n.positions) {
			return false
		}
	}
	for _, c := range n.children {
		if !t.walkFrom(c, term, visit) {
			return false
		}
	}
	return true
}

var (
	_ fts.Index           = (*Index)(nil)
	_ fts.PositionalIndex = (*Index)(nil)
	_ fts.PrefixIndex     = (*Index)(nil)
)
