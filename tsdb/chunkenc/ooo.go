package chunkenc

import (
	"sort"
)

type Sample struct {
	T int64
	V float64
}

// OOOChunk maintains Samples in time-ascending order.
// Inserts for timestamps already seen, are dropped.
// Samples are stored uncompressed to allow easy sorting.
// Perhaps we can be more efficient later.
type OOOChunk struct {
	Samples []Sample
}

func NewOOOChunk(capacity int) *OOOChunk {
	return &OOOChunk{Samples: make([]Sample, 0, capacity)}
}

// Insert inserts the Sample such that order is maintained.
// Returns false if insert was not possible due to the same timestamp already existing.
func (o *OOOChunk) Insert(t int64, v float64) bool {
	// find index of Sample we should replace
	i := sort.Search(len(o.Samples), func(i int) bool { return o.Samples[i].T >= t })

	if i >= len(o.Samples) {
		// none found. append it at the end
		o.Samples = append(o.Samples, Sample{t, v})
		return true
	}

	if o.Samples[i].T == t {
		return false
	}

	// expand length by 1 to make room. use a zero Sample, we will overwrite it anyway
	o.Samples = append(o.Samples, Sample{})
	copy(o.Samples[i+1:], o.Samples[i:])
	o.Samples[i] = Sample{t, v}

	return true
}

func (o *OOOChunk) NumSamples() int {
	return len(o.Samples)
}

func (o *OOOChunk) ToXor() (*XORChunk, error) {
	x := NewXORChunk()
	app, err := x.Appender()
	if err != nil {
		return nil, err
	}
	for _, s := range o.Samples {
		app.Append(s.T, s.V)
	}
	return x, nil
}

// TODO: to support querying, implement Iterator

// func (c *OOOChunk) Iterator(it Iterator) Iterator {
// func (c *OOOChunk) NumSamples() int {
// func (c *OOOChunk) Compact() {

// func (it *oooIterator) Seek(T int64) bool {
// func (it *oooIterator) At() (int64, float64) {
// func (it *oooIterator) Err() error {
// func (it *oooIterator) Next() bool {
