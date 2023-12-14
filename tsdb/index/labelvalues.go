package index

import (
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/util/annotations"
)

// LabelValuesFor returns LabelValues for the given label name in the series referred to by postings.
func (r *Reader) LabelValuesFor(postings Postings, name string) storage.LabelValues {
	if r.version == FormatV1 {
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
			e:      e,
			values: vals,
			cloner: NewPostingsCloner(postings),
			b:      r.b,
			dec:    r.dec,
		}
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
		d:       &d,
		b:       r.b,
		dec:     r.dec,
		lastVal: lastVal,
		cloner:  NewPostingsCloner(postings),
	}
}

type intersectLabelValuesV1 struct {
	e      map[string]uint64
	values []string
	cloner *PostingsCloner
	b      ByteSlice
	dec    *Decoder
	cur    string
	err    error
}

func (it *intersectLabelValuesV1) Next() bool {
	// Look for a value with intersecting postings
	for len(it.values) > 0 {
		val := it.values[0]
		it.values = it.values[1:]

		postingsOff := it.e[val]
		// Read from the postings table.
		d := encoding.NewDecbufAt(it.b, int(postingsOff), castagnoliTable)
		_, curPostings, err := it.dec.Postings(d.Get())
		if err != nil {
			it.err = fmt.Errorf("decode postings: %w", err)
			return false
		}

		postings := it.cloner.Clone()
		if checkIntersection(curPostings, postings) {
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
	cloner    *PostingsCloner
	lastVal   string
	skip      int
	cur       string
	exhausted bool
	err       error
}

func (it *intersectLabelValues) Next() bool {
	if it.exhausted {
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
		v := yoloString(it.d.UvarintBytes())

		postingsOff := it.d.Uvarint64()
		// Read from the postings table
		d2 := encoding.NewDecbufAt(it.b, int(postingsOff), castagnoliTable)
		_, curPostings, err := it.dec.Postings(d2.Get())
		if err != nil {
			it.err = fmt.Errorf("decode postings: %w", err)
			return false
		}

		it.exhausted = v == it.lastVal

		postings := it.cloner.Clone()
		if checkIntersection(curPostings, postings) {
			it.cur = v
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

	slices.Sort(vals)
	return storage.NewListLabelValues(vals)
}

// checkIntersection returns whether p1 and p2 have at least one series in common.
func checkIntersection(p1, p2 Postings) bool {
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
