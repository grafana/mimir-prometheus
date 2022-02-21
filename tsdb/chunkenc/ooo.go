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

// used in chunk diskmapper.WriteChunk to actually write the data to cut new files based on size limits, crc, etc
//func (c *OOOChunk) Bytes() []byte {
// used in chunk diskmapper.WriteChunk
//func (c *OOOChunk) Encoding() Encoding { -> we can fake 'xor encoding'

//func (c *OOOChunk) Appender() (Appender, error) { // do we need this ? don't think so. we call Insert directly // ganesh agrees
//func (c *OOOChunk) Iterator(it Iterator) Iterator {
//func (c *OOOChunk) NumSamples() int {
//func (c *OOOChunk) Compact() {

// write to disk oooChunk.ToXor()? or implement all methods on OOO?
// when flushing to disk: convert to xorchunk
// for in memory: use oooIterator on ooochunk

//func (it *oooIterator) Seek(t int64) bool {
//func (it *oooIterator) At() (int64, float64) {
//func (it *oooIterator) Err() error {
//func (it *oooIterator) Next() bool {
