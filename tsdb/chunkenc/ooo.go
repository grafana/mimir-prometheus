package chunkenc

import (
	"sort"
)

const maxSize int = 32

type sample struct {
	t int64
	v float64
}

// OOOChunk holds upto maxSize samples and always maintains time-ascending order.
// Inserts for timestamps already seen, are dropped.
type OOOChunk struct {
	samples []sample
}

func NewOOOChunk() *OOOChunk {
	// NOTE: later optimization : start smaller and allocate maxSize later
	return &OOOChunk{samples: make([]sample, 0, maxSize)}
}

// Insert adds the sample to the chunk.
// If the chunk is at capacity, it returns the newest sample that didn't fit (possibly the input)
func (o *OOOChunk) Insert(t int64, v float64) (int64, float64) {

	// find index of sample we should replace
	i := sort.Search(len(o.samples), func(i int) bool { return o.samples[i].t >= t })

	// none found. append it or return it if we're already full
	if i >= len(o.samples) {
		if len(o.samples) == maxSize {
			return t, v
		}
		o.samples = append(o.samples, sample{t, v})
		return 0, 0
	}

	if o.samples[i].t == t {
		// this sample is an update. drop it.
		// TODO: error reporting? depends on addressing https://github.com/prometheus/prometheus/discussions/10305
		// something something about not being able to detect all updates because they might be in mmapped ooo chunks
		return 0, 0
	}

	var retT int64
	var retV float64

	if len(o.samples) == maxSize {
		// this sample will be dropped from the slice, but it needs to be returned
		p := o.samples[maxSize-1]
		retT, retV = p.t, p.v
	} else {
		// expand length by 1 to make room. use a zero sample, we will overwrite it anyway
		o.samples = append(o.samples, sample{})
	}

	copy(o.samples[i+1:], o.samples[i:])
	o.samples[i] = sample{t, v}

	return retT, retV

}

// used in chunk diskmapper.WriteChunk to actually write the data to cut new files based on size limits, crc, etc
//func (c *OOOChunk) Bytes() []byte {
// used in chunk diskmapper.WriteChunk
//func (c *OOOChunk) Encoding() Encoding {
//func (c *OOOChunk) Appender() (Appender, error) { // do we need this ? don't think so. we call Insert directly
//func (c *OOOChunk) Iterator(it Iterator) Iterator {
//func (c *OOOChunk) NumSamples() int {
//func (c *OOOChunk) Compact() {

//func (it *oooIterator) Seek(t int64) bool {
//func (it *oooIterator) At() (int64, float64) {
//func (it *oooIterator) Err() error {
//func (it *oooIterator) Next() bool {
