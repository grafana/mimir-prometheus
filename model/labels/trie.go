package labels

import (
	"sort"
)

// trie is a prefix tree
type trie struct {
	prefix   byte
	terminal bool
	// children are sorted by prefix
	children []*trie
}

// add adds a name to the trie, splitting nodes as necessary
func (t *trie) add(name string) {
	if len(name) == 0 {
		t.terminal = true
		return
	}

	l := name[0]

	childIndex := sort.Search(len(t.children), func(i int) bool {
		return t.children[i].prefix >= l
	})

	if childIndex < len(t.children) && t.children[childIndex].prefix == l {
		t.children[childIndex].add(name[1:])
		return
	}

	newChild := &trie{
		prefix:   l,
		terminal: false,
		children: []*trie{},
	}

	t.children = append(t.children, nil)
	copy(t.children[childIndex+1:], t.children[childIndex:])
	t.children[childIndex] = newChild

	newChild.add(name[1:])
}

func (t *trie) Matches(name string) bool {
	if len(name) == 0 {
		return t.terminal
	}

	l := name[0]

	childIndex := sort.Search(len(t.children), func(i int) bool {
		return t.children[i].prefix >= l
	})

	if childIndex < len(t.children) && t.children[childIndex].prefix == l {
		return t.children[childIndex].Matches(name[1:])
	}

	return false
}
