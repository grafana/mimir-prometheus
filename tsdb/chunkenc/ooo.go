package chunkenc

import (
	"sort"
)

type sample struct {
	t int64
	v float64
}

// OOOChunk maintains samples in time-ascending order.
// Inserts for timestamps already seen, are dropped.
// Samples are stored uncompressed to allow easy sorting.
// Perhaps we can be more efficient later.
type OOOChunk struct {
	samples []sample
}

func NewOOOChunk(capacity int) *OOOChunk {
	return &OOOChunk{samples: make([]sample, 0, capacity)}
}

// Insert adds the sample to the chunk.
func (o *OOOChunk) Insert(t int64, v float64) {

	// find index of sample we should replace
	i := sort.Search(len(o.samples), func(i int) bool { return o.samples[i].t >= t })

	if i >= len(o.samples) {
		// none found. append it at the end
		o.samples = append(o.samples, sample{t, v})
		return
	}

	if o.samples[i].t == t {
		// this sample is an update. drop it.
		// TODO: error reporting? depends on addressing https://github.com/prometheus/prometheus/discussions/10305
		// something something about not being able to detect all updates because they might be in mmapped ooo chunks
		return
	}

	// expand length by 1 to make room. use a zero sample, we will overwrite it anyway
	o.samples = append(o.samples, sample{})
	copy(o.samples[i+1:], o.samples[i:])
	o.samples[i] = sample{t, v}

	return
}

func (o *OOOChunk) NumSamples() int {
	return len(o.samples)
}

func (o *OOOChunk) ToXor() (*XORChunk, error) {
	x := NewXORChunk()
	app, err := x.Appender()
	if err != nil {
		return nil, err
	}
	for _, s := range o.samples {
		app.Append(s.t, s.v)
	}
	return x, nil
}

// TODO: to support querying, implement Iterator

//func (c *OOOChunk) Iterator(it Iterator) Iterator {
//func (c *OOOChunk) NumSamples() int {
//func (c *OOOChunk) Compact() {

//func (it *oooIterator) Seek(t int64) bool {
//func (it *oooIterator) At() (int64, float64) {
//func (it *oooIterator) Err() error {
//func (it *oooIterator) Next() bool {
