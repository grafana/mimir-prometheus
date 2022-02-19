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
	return &OOOChunk{samples: make([]sample, 0, maxSize)} // TODO: later optimization : start smaller and allocate maxSize later
}

// Insert adds the point to the chunk.
// If the chunk is at capacity, it returns the newest point that didn't fit (possibly the input)
func (o *OOOChunk) Insert(t int64, v float64) (int64, float64) {

	// find index of point we should replace
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
		// this point is an update. drop it.
		return 0, 0
	}

	var retT int64
	var retV float64

	if len(o.samples) == maxSize {
		// this point will be dropped from the slice, but it needs to be returned
		p := o.samples[maxSize-1]
		retT, retV = p.t, p.v
	} else {
		// expand length by 1 to make room. use a zero point, we will overwrite it anyway
		o.samples = append(o.samples, sample{})
	}

	copy(o.samples[i+1:], o.samples[i:])
	o.samples[i] = sample{t, v}

	return retT, retV

}
