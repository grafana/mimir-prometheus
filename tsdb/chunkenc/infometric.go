// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/prometheus/prometheus/model/histogram"
)

// InfoSampleChunk holds encoded info metric sample data.
type InfoSampleChunk struct {
	b bstream
}

// NewInfoSampleChunk returns a new chunk with info metric encoding of the given
// size.
func NewInfoSampleChunk() *InfoSampleChunk {
	b := make([]byte, 3, 128)
	return &InfoSampleChunk{b: bstream{stream: b, count: 0}}
}

func (c *InfoSampleChunk) Reset(stream []byte) {
	c.b.Reset(stream)
}

// Encoding returns the encoding type.
func (c *InfoSampleChunk) Encoding() Encoding {
	return EncInfoMetric
}

// Bytes returns the underlying byte slice of the chunk.
func (c *InfoSampleChunk) Bytes() []byte {
	return c.b.bytes()
}

// NumSamples returns the number of samples in the chunk.
func (c *InfoSampleChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

// Compact implements the Chunk interface.
func (c *InfoSampleChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

// Appender implements the Chunk interface.
// It is not valid to call Appender() multiple times concurrently or to use multiple
// Appenders on the same chunk.
func (c *InfoSampleChunk) Appender() (Appender, error) {
	it := c.iterator(nil)

	// To get an appender, we must know the state it would have if we had
	// appended all existing data from scratch. We iterate through the end
	// and populate via the iterator's state.
	for it.Next() != ValNone {
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	return &infoSampleAppender{
		b:      &c.b,
		t:      it.t,
		tDelta: it.tDelta,
		lDelta: it.lDelta,
		ils:    it.ils,
	}, nil
}

// Iterator implements the Chunk interface.
func (c *InfoSampleChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

func (c *InfoSampleChunk) iterator(it Iterator) *infoSampleIterator {
	if iter, ok := it.(*infoSampleIterator); ok {
		iter.Reset(c.b.bytes())
		return iter
	}

	iter := &infoSampleIterator{
		// The first 2 bytes contain chunk headers.
		// We skip that for actual samples.
		br:       newBReader(c.b.bytes()[2:]),
		numTotal: binary.BigEndian.Uint16(c.b.bytes()),
		t:        math.MinInt64,
	}
	// The first 3 bytes contain chunk headers.
	// We skip that for actual samples.
	_, _ = iter.br.readBits(24)
	return iter
}

type infoSampleAppender struct {
	b *bstream

	t      int64
	tDelta int64
	lDelta int
	ils    []int
}

// Append implements Appender. This implementation panics because normal float
// samples must never be appended to an info sample chunk.
func (a *infoSampleAppender) Append(int64, float64) {
	panic("appended a float sample to an info sample chunk")
}

// Append implements Appender. This implementation panics because float histogram
// samples must never be appended to an info sample chunk.
func (a *infoSampleAppender) AppendFloatHistogram(*FloatHistogramAppender, int64, *histogram.FloatHistogram, bool) (Chunk, bool, Appender, error) {
	panic("appended a float histogram sample to an info sample chunk")
}

// Append implements Appender. This implementation panics because histogram
// samples must never be appended to an info sample chunk.
func (a *infoSampleAppender) AppendHistogram(prev *HistogramAppender, t int64, h *histogram.Histogram, appendOnly bool) (Chunk, bool, Appender, error) {
	panic("appended a histogram sample to an info sample chunk")
}

// AppendInfoSample implements Appender.
func (a *infoSampleAppender) AppendInfoSample(t int64, identifyingLabels []int) {
	num := binary.BigEndian.Uint16(a.b.bytes())

	if num == 0 {
		putVarbitInt(a.b, t)
		// The labels change
		putVarbitInt(a.b, 1)
		putVarbitInt(a.b, int64(len(identifyingLabels)))
		for _, ix := range identifyingLabels {
			putVarbitInt(a.b, int64(ix))
		}
	} else {
		tDelta := t - a.t
		tDod := tDelta - a.tDelta

		putVarbitInt(a.b, tDod)

		a.tDelta = tDelta

		if slices.Equal(a.ils, identifyingLabels) {
			// The labels do not change
			putVarbitInt(a.b, 0)
		} else {
			// The labels change
			putVarbitInt(a.b, 1)
			lDelta := len(identifyingLabels) - len(a.ils)
			lDod := int64(lDelta - a.lDelta)
			a.lDelta = lDelta

			putVarbitInt(a.b, lDod)
			for _, ix := range identifyingLabels {
				putVarbitInt(a.b, int64(ix))
			}
		}
	}

	a.t = t
	a.ils = identifyingLabels
}

type infoSampleIterator struct {
	br       bstreamReader
	numTotal uint16
	numRead  uint16

	t      int64
	tDelta int64
	l      int
	lDelta int
	ils    []int

	err error
}

func (it *infoSampleIterator) Seek(t int64) ValueType {
	if it.err != nil {
		return ValNone
	}

	for t > it.t || it.numRead == 0 {
		if it.Next() == ValNone {
			return ValNone
		}
	}
	return ValInfoSample
}

func (it *infoSampleIterator) At() (int64, float64) {
	return it.t, 1
}

func (it *infoSampleIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("cannot call infoSampleIterator.AtHistogram")
}

func (it *infoSampleIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("cannot call infoSampleIterator.AtFloatHistogram")
}

func (it *infoSampleIterator) AtInfoSample() (int64, []int) {
	return it.t, it.ils
}

func (it *infoSampleIterator) AtT() int64 {
	return it.t
}

func (it *infoSampleIterator) Err() error {
	return it.err
}

func (it *infoSampleIterator) Reset(b []byte) {
	// The first 2 bytes contain chunk headers.
	// We skip that for actual samples.
	it.br = newBReader(b[2:])
	it.numTotal = binary.BigEndian.Uint16(b)

	it.numRead = 0
	it.t = 0
	it.tDelta = 0
	it.l = 0
	it.lDelta = 0
	it.ils = nil
	it.err = nil
}

func (it *infoSampleIterator) Next() ValueType {
	if it.err != nil || it.numRead == it.numTotal {
		return ValNone
	}

	if it.numRead == 0 {
		t, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		if _, err := readVarbitInt(&it.br); err != nil {
			it.err = err
			return ValNone
		}
		l, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		for i := 0; i < int(l); i++ {
			ix, err := readVarbitInt(&it.br)
			if err != nil {
				it.err = err
				return ValNone
			}
			it.ils = append(it.ils, int(ix))
		}

		it.t = t
		it.l = int(l)

		it.numRead++
		return ValInfoSample
	}

	tDod, err := readVarbitInt(&it.br)
	if err != nil {
		it.err = err
		return ValNone
	}

	it.tDelta += tDod
	it.t += it.tDelta

	changed, err := readVarbitInt(&it.br)
	if err != nil {
		it.err = err
		return ValNone
	}

	if changed == 0 {
		it.numRead++
		return ValInfoSample
	}

	// The identifying labels change
	lDod, err := readVarbitInt(&it.br)
	if err != nil {
		it.err = err
		return ValNone
	}

	it.lDelta += int(lDod)
	it.l += it.lDelta
	it.ils = make([]int, 0, it.l)
	for i := 0; i < it.l; i++ {
		ix, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}

		it.ils = append(it.ils, int(ix))
	}

	it.numRead++
	return ValInfoSample
}
