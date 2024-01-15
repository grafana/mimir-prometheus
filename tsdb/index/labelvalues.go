package index

import (
	"container/heap"
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/util/annotations"
)

// LabelValuesFor returns LabelValues for the given label name in the series referred to by postings.
func (r *Reader) LabelValuesFor(postings Postings, name string) storage.LabelValues {
	return r.labelValuesFor(postings, name, false)
}

// LabelValuesNotFor returns LabelValues for the given label name in the series *not* referred to by postings.
func (r *Reader) LabelValuesNotFor(postings Postings, name string) storage.LabelValues {
	return r.labelValuesFor(postings, name, true)
}

func (r *Reader) labelValuesFor(postings Postings, name string, inverted bool) storage.LabelValues {
	if r.version == FormatV1 {
		return r.labelValuesForV1(postings, name, inverted)
	}

	e := r.postings[name]
	if len(e) == 0 {
		return storage.EmptyLabelValues()
	}

	d := encoding.NewDecbufAt(r.b, int(r.toc.PostingsTable), nil)
	// Skip to start
	d.Skip(e[0].off)
	lastVal := e[len(e)-1].value

	return &intersectLabelValues{
		d:        &d,
		b:        r.b,
		dec:      r.dec,
		lastVal:  lastVal,
		postings: NewPostingsCloner(postings),
		inverted: inverted,
	}
}

func (r *Reader) labelValuesForV1(postings Postings, name string, inverted bool) storage.LabelValues {
	e := r.postingsV1[name]
	if len(e) == 0 {
		return storage.EmptyLabelValues()
	}
	vals := make([]string, 0, len(e))
	for v := range e {
		vals = append(vals, v)
	}
	slices.Sort(vals)
	return &intersectLabelValuesV1{
		seriesRefs: e,
		values:     vals,
		postings:   NewPostingsCloner(postings),
		b:          r.b,
		dec:        r.dec,
		inverted:   inverted,
	}
}

type intersectLabelValuesV1 struct {
	seriesRefs map[string]uint64
	values     []string
	postings   *PostingsCloner
	b          ByteSlice
	dec        *Decoder
	cur        string
	err        error
	inverted   bool
}

func (it *intersectLabelValuesV1) Next() bool {
	if it.err != nil {
		return false
	}

	// Look for a value with intersecting postings
	for len(it.values) > 0 {
		val := it.values[0]
		it.values = it.values[1:]

		postingsOff := it.seriesRefs[val]
		// Read from the postings table.
		d := encoding.NewDecbufAt(it.b, int(postingsOff), castagnoliTable)
		_, curPostings, err := it.dec.Postings(d.Get())
		if err != nil {
			it.err = fmt.Errorf("decode postings: %w", err)
			return false
		}

		isMatch := intersect(curPostings, it.postings.Clone())
		if it.inverted {
			isMatch = !isMatch
		}
		if isMatch {
			it.cur = val
			return true
		}
	}

	return false
}

func (it *intersectLabelValuesV1) At() string {
	return it.cur
}

func (it *intersectLabelValuesV1) Err() error {
	return it.err
}

func (it *intersectLabelValuesV1) Warnings() annotations.Annotations {
	return nil
}

func (it *intersectLabelValuesV1) Close() error {
	return nil
}

type intersectLabelValues struct {
	d         *encoding.Decbuf
	b         ByteSlice
	dec       *Decoder
	postings  *PostingsCloner
	lastVal   string
	skip      int
	cur       string
	exhausted bool
	err       error
	inverted  bool
}

func (it *intersectLabelValues) Next() bool {
	if it.exhausted || it.err != nil {
		return false
	}

	for !it.exhausted && it.d.Err() == nil {
		if it.skip == 0 {
			// These are always the same number of bytes,
			// and it's faster to skip than to parse.
			it.skip = it.d.Len()
			// Key count
			it.d.Uvarint()
			// Label name
			it.d.UvarintBytes()
			it.skip -= it.d.Len()
		} else {
			it.d.Skip(it.skip)
		}

		// Label value
		vb := it.d.UvarintBytes()
		v := yoloString(vb)

		postingsOff := int(it.d.Uvarint64())
		// Read from the postings table
		postingsDec := encoding.NewDecbufAt(it.b, postingsOff, castagnoliTable)
		_, curPostings, err := it.dec.Postings(postingsDec.Get())
		if err != nil {
			it.err = fmt.Errorf("decode postings: %w", err)
			return false
		}

		it.exhausted = v == it.lastVal

		isMatch := intersect(curPostings, it.postings.Clone())
		if it.inverted {
			isMatch = !isMatch
		}
		if isMatch {
			// Make sure to allocate a new string
			it.cur = string(vb)
			return true
		}
	}
	if it.d.Err() != nil {
		it.err = fmt.Errorf("get postings offset entry: %w", it.d.Err())
	}

	return false
}

func (it *intersectLabelValues) At() string {
	return it.cur
}

func (it *intersectLabelValues) Err() error {
	return it.err
}

func (it *intersectLabelValues) Warnings() annotations.Annotations {
	return nil
}

func (it *intersectLabelValues) Close() error {
	return nil
}

// LabelValuesFor returns LabelValues for the given label name in the series referred to by postings.
func (p *MemPostings) LabelValuesFor(postings Postings, name string) storage.LabelValues {
	return p.labelValuesFor(postings, name, false)
}

// LabelValuesNotFor returns LabelValues for the given label name in the series *not* referred to by postings.
func (p *MemPostings) LabelValuesNotFor(postings Postings, name string) storage.LabelValues {
	return p.labelValuesFor(postings, name, true)
}

func (p *MemPostings) labelValuesFor(postings Postings, name string, inverted bool) storage.LabelValues {
	p.mtx.RLock()

	e := p.m[name]
	if len(e) == 0 {
		p.mtx.RUnlock()
		return storage.EmptyLabelValues()
	}

	// With thread safety in mind and due to random key ordering in map, we have to construct the array in memory
	vals := make([]string, 0, len(e))
	candidates := make([]Postings, 0, len(e))
	for val, srs := range e {
		vals = append(vals, val)
		candidates = append(candidates, NewListPostings(srs))
	}

	if !inverted {
		indexes, err := FindIntersectingPostings(postings, candidates)
		p.mtx.RUnlock()
		if err != nil {
			return storage.ErrLabelValues(err)
		}

		// Filter the values, keeping only those with intersecting postings
		if len(vals) != len(indexes) {
			slices.Sort(indexes)
			for i, index := range indexes {
				vals[i] = vals[index]
			}
			vals = vals[:len(indexes)]
		}
	} else {
		// TODO: Implement findNonIntersectingPostings instead
		indexes, err := findIntersectingPostingsMap(postings, candidates)
		p.mtx.RUnlock()
		if err != nil {
			return storage.ErrLabelValues(err)
		}

		// Filter the values, keeping only those without intersecting postings
		keep := make([]string, 0, len(vals)-len(indexes))
		for i, val := range vals {
			if _, ok := indexes[i]; !ok {
				keep = append(keep, val)
			}
		}
		vals = keep
	}

	slices.Sort(vals)
	return storage.NewListLabelValues(vals)
}

func findIntersectingPostingsMap(p Postings, candidates []Postings) (indexes map[int]struct{}, err error) {
	h := make(postingsWithIndexHeap, 0, len(candidates))
	for idx, it := range candidates {
		switch {
		case it.Next():
			h = append(h, postingsWithIndex{index: idx, p: it})
		case it.Err() != nil:
			return nil, it.Err()
		}
	}
	if h.empty() {
		return nil, nil
	}
	heap.Init(&h)

	indexes = map[int]struct{}{}
	for !h.empty() {
		if !p.Seek(h.at()) {
			return indexes, p.Err()
		}
		if p.At() == h.at() {
			indexes[h.popIndex()] = struct{}{}
		} else if err := h.next(); err != nil {
			return nil, err
		}
	}

	return indexes, nil
}

// intersect returns whether p1 and p2 have at least one series in common.
func intersect(p1, p2 Postings) bool {
	if !p1.Next() || !p2.Next() {
		return false
	}

	cur := p1.At()
	if p2.At() > cur {
		cur = p2.At()
	}

	for {
		if !p1.Seek(cur) {
			break
		}
		if p1.At() > cur {
			cur = p1.At()
		}
		if !p2.Seek(cur) {
			break
		}
		if p2.At() > cur {
			cur = p2.At()
			continue
		}

		return true
	}

	return false
}
