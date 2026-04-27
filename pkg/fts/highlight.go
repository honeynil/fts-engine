package fts

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Highlighter struct {
	PreTag       string
	PostTag      string
	MaxFragments int
	FragmentSize int
	Separator    string
	Pipeline     Pipeline
	KeyGen       KeyGenerator
}

type Fragment struct {
	Text    string
	Matches int
}

func (h Highlighter) Highlight(query, text string, pipeline Pipeline, keyGen KeyGenerator) []Fragment {
	if text == "" || query == "" {
		return nil
	}

	pre, post, maxFrag, fragSize, sep := h.PreTag, h.PostTag, h.MaxFragments, h.FragmentSize, h.Separator
	if pre == "" {
		pre = "<mark>"
	}
	if post == "" {
		post = "</mark>"
	}
	if maxFrag <= 0 {
		maxFrag = 3
	}
	if fragSize <= 0 {
		fragSize = 150
	}
	if sep == "" {
		sep = " … "
	}
	if h.Pipeline != nil {
		pipeline = h.Pipeline
	}
	if h.KeyGen != nil {
		keyGen = h.KeyGen
	}
	if pipeline == nil {
		pipeline = defaultPipeline{}
	}
	if keyGen == nil {
		keyGen = WordKeys
	}

	wantedKeys := buildKeySet(query, pipeline, keyGen)
	if len(wantedKeys) == 0 {
		return nil
	}

	matches := findHighlightMatches(text, pipeline, keyGen, wantedKeys)
	if len(matches) == 0 {
		return nil
	}

	clusters := clusterMatches(matches, fragSize)
	sort.SliceStable(clusters, func(i, j int) bool { return len(clusters[i]) > len(clusters[j]) })
	if len(clusters) > maxFrag {
		clusters = clusters[:maxFrag]
	}
	sort.SliceStable(clusters, func(i, j int) bool { return clusters[i][0].start < clusters[j][0].start })

	out := make([]Fragment, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, renderFragment(text, c, fragSize, pre, post, sep))
	}
	return out
}

func (s *Service) Highlight(query, text string, h Highlighter) []Fragment {
	return h.Highlight(query, text, s.pipeline, s.keyGen)
}

func buildKeySet(query string, pipeline Pipeline, keyGen KeyGenerator) map[string]struct{} {
	tokens := pipeline.Process(query)
	if len(tokens) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		keys, err := keyGen(t)
		if err != nil {
			continue
		}
		for _, k := range keys {
			out[k] = struct{}{}
		}
	}
	return out
}

type highlightMatch struct {
	start int // byte offset, inclusive
	end   int // byte offset, exclusive
}

func findHighlightMatches(text string, pipeline Pipeline, keyGen KeyGenerator, want map[string]struct{}) []highlightMatch {
	var matches []highlightMatch
	wordStart := -1
	emit := func(end int) {
		if wordStart < 0 {
			return
		}
		word := text[wordStart:end]
		if isNormalizedHit(word, pipeline, keyGen, want) {
			matches = append(matches, highlightMatch{start: wordStart, end: end})
		}
		wordStart = -1
	}

	for i, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if wordStart < 0 {
				wordStart = i
			}
			continue
		}
		emit(i)
	}
	emit(len(text))
	return matches
}

func isNormalizedHit(word string, pipeline Pipeline, keyGen KeyGenerator, want map[string]struct{}) bool {
	tokens := pipeline.Process(word)
	for _, t := range tokens {
		keys, err := keyGen(t)
		if err != nil {
			continue
		}
		for _, k := range keys {
			if _, ok := want[k]; ok {
				return true
			}
		}
	}
	return false
}

func clusterMatches(matches []highlightMatch, fragSize int) [][]highlightMatch {
	var clusters [][]highlightMatch
	var current []highlightMatch
	for _, m := range matches {
		if len(current) == 0 {
			current = []highlightMatch{m}
			continue
		}
		first := current[0].start
		if m.end-first <= fragSize {
			current = append(current, m)
		} else {
			clusters = append(clusters, current)
			current = []highlightMatch{m}
		}
	}
	if len(current) > 0 {
		clusters = append(clusters, current)
	}
	return clusters
}

func renderFragment(text string, cluster []highlightMatch, fragSize int, pre, post, sep string) Fragment {
	first, last := cluster[0].start, cluster[len(cluster)-1].end
	span := last - first
	pad := (fragSize - span) / 2
	if pad < 0 {
		pad = 0
	}
	left := first - pad
	right := last + pad
	if left < 0 {
		left = 0
	}
	if right > len(text) {
		right = len(text)
	}
	left = expandToWordBoundary(text, left, -1)
	right = expandToWordBoundary(text, right, +1)

	var b strings.Builder
	b.Grow(right - left + len(cluster)*(len(pre)+len(post)) + len(sep)*2)
	if left > 0 {
		b.WriteString(sep)
	}
	cursor := left
	for _, m := range cluster {
		if m.start < cursor {
			continue
		}
		b.WriteString(text[cursor:m.start])
		b.WriteString(pre)
		b.WriteString(text[m.start:m.end])
		b.WriteString(post)
		cursor = m.end
	}
	b.WriteString(text[cursor:right])
	if right < len(text) {
		b.WriteString(sep)
	}
	return Fragment{Text: b.String(), Matches: len(cluster)}
}

func expandToWordBoundary(text string, pos, dir int) int {
	if dir < 0 {
		for pos > 0 {
			r, size := utf8.DecodeLastRuneInString(text[:pos])
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
				break
			}
			pos -= size
		}
		return pos
	}
	for pos < len(text) {
		r, size := utf8.DecodeRuneInString(text[pos:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			break
		}
		pos += size
	}
	return pos
}
