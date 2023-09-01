// Copyright 2021 The Prometheus Authors
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

package tsdb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

func TestBoundedChunk(t *testing.T) {
	scenarios := map[string]struct {
		inputChunk chunkenc.Chunk
		valueType  chunkenc.ValueType
		appendFunc func(app chunkenc.Appender, ts int64, val float64)
		chunkFunc  func(numSamples int) chunkenc.Chunk
		sampleFunc func(ts int64) sample
	}{
		"float": {
			valueType: chunkenc.ValFloat,
			appendFunc: func(app chunkenc.Appender, ts int64, val float64) {
				app.Append(ts, val)
			},
			chunkFunc: newTestChunk,
			sampleFunc: func(ts int64) sample {
				return sample{t: ts, f: float64(ts)}
			},
		},
		"integer histogram": {
			valueType: chunkenc.ValHistogram,
			appendFunc: func(app chunkenc.Appender, ts int64, val float64) {
				h := tsdbutil.GenerateTestHistogram(int(val))
				prevHApp, _ := app.(*chunkenc.HistogramAppender)
				app.AppendHistogram(prevHApp, ts, h, false)
			},
			chunkFunc: newTestHistogramChunk,
			sampleFunc: func(ts int64) sample {
				return sample{t: ts, h: tsdbutil.GenerateTestHistogram(int(ts))}
			},
		},
		"float histogram": {
			valueType: chunkenc.ValFloatHistogram,
			appendFunc: func(app chunkenc.Appender, ts int64, val float64) {
				fh := tsdbutil.GenerateTestFloatHistogram(int(val))
				prevHApp, _ := app.(*chunkenc.FloatHistogramAppender)
				app.AppendFloatHistogram(prevHApp, ts, fh, false)
			},
			chunkFunc: newTestFloatHistogramChunk,
			sampleFunc: func(ts int64) sample {
				return sample{t: ts, fh: tsdbutil.GenerateTestFloatHistogram(int(ts))}
			},
		},
	}
	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			testBoundedIterable(t, scenario.valueType, scenario.appendFunc, scenario.chunkFunc, scenario.sampleFunc)
		})
	}
}

func testBoundedIterable(t *testing.T,
	valueType chunkenc.ValueType,
	appendFunc func(app chunkenc.Appender, ts int64, val float64),
	chunkFunc func(numSamples int) chunkenc.Chunk,
	sampleFunc func(ts int64) sample,
) {
	tests := []struct {
		name           string
		inputChunk     chunkenc.Chunk
		inputMinT      int64
		inputMaxT      int64
		initialSeek    int64
		seekIsASuccess bool
		expSamples     []sample
	}{
		{
			name:       "if there are no samples it returns nothing",
			inputChunk: chunkFunc(0),
			expSamples: nil,
		},
		{
			name:       "bounds represent a single sample",
			inputChunk: chunkFunc(10),
			expSamples: []sample{sampleFunc(0)},
		},
		{
			name:       "if there are bounds set only samples within them are returned",
			inputChunk: chunkFunc(10),
			inputMinT:  1,
			inputMaxT:  8,
			expSamples: []sample{
				sampleFunc(1),
				sampleFunc(2),
				sampleFunc(3),
				sampleFunc(4),
				sampleFunc(5),
				sampleFunc(6),
				sampleFunc(7),
				sampleFunc(8),
			},
		},
		{
			name:       "if bounds set and only maxt is less than actual maxt",
			inputChunk: chunkFunc(10),
			inputMinT:  0,
			inputMaxT:  5,
			expSamples: []sample{
				sampleFunc(0),
				sampleFunc(1),
				sampleFunc(2),
				sampleFunc(3),
				sampleFunc(4),
				sampleFunc(5),
			},
		},
		{
			name:       "if bounds set and only mint is more than actual mint",
			inputChunk: chunkFunc(10),
			inputMinT:  5,
			inputMaxT:  9,
			expSamples: []sample{
				sampleFunc(5),
				sampleFunc(6),
				sampleFunc(7),
				sampleFunc(8),
				sampleFunc(9),
			},
		},
		{
			name:           "if there are bounds set with seek before mint",
			inputChunk:     chunkFunc(10),
			inputMinT:      3,
			inputMaxT:      7,
			initialSeek:    1,
			seekIsASuccess: true,
			expSamples: []sample{
				sampleFunc(3),
				sampleFunc(4),
				sampleFunc(5),
				sampleFunc(6),
				sampleFunc(7),
			},
		},
		{
			name:           "if there are bounds set with seek between mint and maxt",
			inputChunk:     chunkFunc(10),
			inputMinT:      3,
			inputMaxT:      7,
			initialSeek:    5,
			seekIsASuccess: true,
			expSamples: []sample{
				sampleFunc(5),
				sampleFunc(6),
				sampleFunc(7),
			},
		},
		{
			name:           "if there are bounds set with seek after maxt",
			inputChunk:     chunkFunc(10),
			inputMinT:      3,
			inputMaxT:      7,
			initialSeek:    8,
			seekIsASuccess: false,
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("name=%s", tc.name), func(t *testing.T) {
			iterable := boundedIterable{tc.inputChunk, tc.inputMinT, tc.inputMaxT}

			chunk, err := chunkenc.NewEmptyChunk(tc.inputChunk.Encoding())
			require.NoError(t, err)

			if tc.inputChunk.NumSamples() > 0 {
				app, err := chunk.Appender()
				require.NoError(t, err)
				for ts := tc.inputMinT; ts <= tc.inputMaxT; ts++ {
					appendFunc(app, ts, float64(ts))
				}
			}

			var samples []sample
			it := iterable.Iterator(nil)

			if tc.initialSeek != 0 {
				// Testing Seek()
				val := it.Seek(tc.initialSeek)
				require.Equal(t, tc.seekIsASuccess, val == valueType)
				if val == valueType {
					switch valueType {
					case chunkenc.ValFloat:
						t, v := it.At()
						samples = append(samples, sample{t, v, nil, nil})
					case chunkenc.ValHistogram:
						t, v := it.AtHistogram()
						v.CounterResetHint = histogram.UnknownCounterReset
						samples = append(samples, sample{t, 0, v, nil})
					case chunkenc.ValFloatHistogram:
						t, v := it.AtFloatHistogram()
						v.CounterResetHint = histogram.UnknownCounterReset
						samples = append(samples, sample{t, 0, nil, v})
					}
				}
			}

			// Testing Next()
			for it.Next() == valueType {
				switch valueType {
				case chunkenc.ValFloat:
					t, v := it.At()
					samples = append(samples, sample{t, v, nil, nil})
				case chunkenc.ValHistogram:
					t, v := it.AtHistogram()
					v.CounterResetHint = histogram.UnknownCounterReset
					samples = append(samples, sample{t, 0, v, nil})
				case chunkenc.ValFloatHistogram:
					t, v := it.AtFloatHistogram()
					v.CounterResetHint = histogram.UnknownCounterReset
					samples = append(samples, sample{t, 0, nil, v})
				}
			}

			// it.Next() should keep returning no  value.
			for i := 0; i < 10; i++ {
				require.Equal(t, chunkenc.ValNone, it.Next())
			}

			require.Equal(t, tc.expSamples, samples)
		})
	}
}

func newTestChunk(numSamples int) chunkenc.Chunk {
	xor := chunkenc.NewXORChunk()
	a, _ := xor.Appender()
	for i := 0; i < numSamples; i++ {
		a.Append(int64(i), float64(i))
	}
	return xor
}

func newTestHistogramChunk(numSamples int) chunkenc.Chunk {
	xc := chunkenc.NewHistogramChunk()
	a, _ := xc.Appender()
	for i := 0; i < numSamples; i++ {
		prevHApp, _ := a.(*chunkenc.HistogramAppender)
		h := tsdbutil.GenerateTestHistogram(i)
		a.AppendHistogram(prevHApp, int64(i), h, false)
	}
	return xc
}

func newTestFloatHistogramChunk(numSamples int) chunkenc.Chunk {
	xc := chunkenc.NewFloatHistogramChunk()
	a, _ := xc.Appender()
	for i := 0; i < numSamples; i++ {
		prevHApp, _ := a.(*chunkenc.FloatHistogramAppender)
		fh := tsdbutil.GenerateTestFloatHistogram(i)
		a.AppendFloatHistogram(prevHApp, int64(i), fh, false)
	}
	return xc
}

func TestMergedOOOChunks(t *testing.T) {
	scenarios := map[string]struct {
		inputChunk chunkenc.Chunk
		valueType  chunkenc.ValueType
		appendFunc func(app chunkenc.Appender, ts int64, val float64)
		sampleFunc func(ts int64) sample
	}{
		"float": {
			valueType: chunkenc.ValFloat,
			appendFunc: func(app chunkenc.Appender, ts int64, val float64) {
				app.Append(ts, val)
			},
			sampleFunc: func(ts int64) sample {
				return sample{t: ts, f: float64(ts)}
			},
		},
		"integer histogram": {
			valueType: chunkenc.ValHistogram,
			appendFunc: func(app chunkenc.Appender, ts int64, val float64) {
				h := tsdbutil.GenerateTestHistogram(int(val))
				prevHApp, _ := app.(*chunkenc.HistogramAppender)
				app.AppendHistogram(prevHApp, ts, h, false)
			},
			sampleFunc: func(ts int64) sample {
				return sample{t: ts, h: tsdbutil.GenerateTestHistogram(int(ts))}
			},
		},
		"float histogram": {
			valueType: chunkenc.ValFloatHistogram,
			appendFunc: func(app chunkenc.Appender, ts int64, val float64) {
				fh := tsdbutil.GenerateTestFloatHistogram(int(val))
				prevHApp, _ := app.(*chunkenc.FloatHistogramAppender)
				app.AppendFloatHistogram(prevHApp, ts, fh, false)
			},
			sampleFunc: func(ts int64) sample {
				return sample{t: ts, fh: tsdbutil.GenerateTestFloatHistogram(int(ts))}
			},
		},
	}
	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			testMergedOOOChunks(t, scenario.valueType, scenario.appendFunc, scenario.sampleFunc)
		})
	}
}

func testMergedOOOChunks(t *testing.T,
	valueType chunkenc.ValueType,
	appendFunc func(app chunkenc.Appender, ts int64, val float64),
	sampleFunc func(ts int64) sample,
) {
	var expSamples []sample
	var chks []chunkenc.Chunk

	// Create chunk 1, even numbers from 0 to 10.
	chk, err := valueType.NewChunk()
	require.NoError(t, err)
	app, err := chk.Appender()
	require.NoError(t, err)

	for ts := 0; ts <= 10; ts += 2 {
		appendFunc(app, int64(ts), float64(ts))
		s := sampleFunc(int64(ts))
		expSamples = append(expSamples, s)
	}
	chks = append(chks, chk)

	// Create chunk 2, odd numbers from 1 to 9.
	chk, err = valueType.NewChunk()
	require.NoError(t, err)
	app, err = chk.Appender()
	require.NoError(t, err)

	for ts := 1; ts <= 9; ts += 2 {
		appendFunc(app, int64(ts), float64(ts))
		s := sampleFunc(int64(ts))
		expSamples = append(expSamples, s)
	}
	chks = append(chks, chk)

	// Create chunk 3, 11-15.
	chk, err = valueType.NewChunk()
	require.NoError(t, err)
	app, err = chk.Appender()
	require.NoError(t, err)

	for ts := 11; ts <= 15; ts++ {
		appendFunc(app, int64(ts), float64(ts))
		s := sampleFunc(int64(ts))
		expSamples = append(expSamples, s)
	}
	chks = append(chks, chk)

	slices.SortFunc(expSamples, func(a, b sample) int {
		return int(a.T() - b.T())
	})

	mc := &mergedOOOChunks{}
	for _, c := range chks {
		mc.chunkIterables = append(mc.chunkIterables, c)
	}

	var actSamples []sample
	it := mc.Iterator(nil)
	for it.Next() == valueType {
		switch valueType {
		case chunkenc.ValFloat:
			t, v := it.At()
			actSamples = append(actSamples, sample{t, v, nil, nil})
		case chunkenc.ValHistogram:
			t, v := it.AtHistogram()
			v.CounterResetHint = histogram.UnknownCounterReset
			actSamples = append(actSamples, sample{t, 0, v, nil})
		case chunkenc.ValFloatHistogram:
			t, v := it.AtFloatHistogram()
			v.CounterResetHint = histogram.UnknownCounterReset
			actSamples = append(actSamples, sample{t, 0, nil, v})
		}
	}

	require.Equal(t, expSamples, actSamples)
}

// TestMemSeries_chunk runs a series of tests on memSeries.chunk() calls.
// It will simulate various conditions to ensure all code paths in that function are covered.
func TestMemSeries_chunk(t *testing.T) {
	const chunkRange int64 = 100
	const chunkStep int64 = 5

	appendSamples := func(t *testing.T, s *memSeries, start, end int64, cdm *chunks.ChunkDiskMapper) {
		for i := start; i < end; i += chunkStep {
			ok, _ := s.append(i, float64(i), 0, chunkOpts{
				chunkDiskMapper: cdm,
				chunkRange:      chunkRange,
				samplesPerChunk: DefaultSamplesPerChunk,
			})
			require.True(t, ok, "sample append failed")
		}
	}

	type setupFn func(*testing.T, *memSeries, *chunks.ChunkDiskMapper)

	type callOutput uint8
	const (
		outOpenHeadChunk   callOutput = iota // memSeries.chunk() call returned memSeries.headChunks with headChunk=true & isOpen=true
		outClosedHeadChunk                   // memSeries.chunk() call returned memSeries.headChunks with headChunk=true & isOpen=false
		outMmappedChunk                      // memSeries.chunk() call returned a chunk from memSeries.mmappedChunks with headChunk=false
		outErr                               // memSeries.chunk() call returned an error
	)

	tests := []struct {
		name     string
		setup    setupFn            // optional function called just before the test memSeries.chunk() call
		inputID  chunks.HeadChunkID // requested chunk id for memSeries.chunk() call
		expected callOutput
	}{
		{
			name:     "call ix=0 on empty memSeries",
			inputID:  0,
			expected: outErr,
		},
		{
			name:     "call ix=1 on empty memSeries",
			inputID:  1,
			expected: outErr,
		},
		{
			name: "firstChunkID > ix",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange, cdm)
				require.Empty(t, s.mmappedChunks, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, int64(0), s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, chunkRange-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
				s.firstChunkID = 5
			},
			inputID:  1,
			expected: outErr,
		},
		{
			name: "call ix=0 on memSeries with no mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange, cdm)
				require.Empty(t, s.mmappedChunks, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, int64(0), s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, chunkRange-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  0,
			expected: outOpenHeadChunk,
		},
		{
			name: "call ix=1 on memSeries with no mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange, cdm)
				require.Empty(t, s.mmappedChunks, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, int64(0), s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, chunkRange-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  1,
			expected: outErr,
		},
		{
			name: "call ix=10 on memSeries with no mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange, cdm)
				require.Empty(t, s.mmappedChunks, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, int64(0), s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, chunkRange-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  10,
			expected: outErr,
		},
		{
			name: "call ix=0 on memSeries with 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*4)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  0,
			expected: outMmappedChunk,
		},
		{
			name: "call ix=1 on memSeries with 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*4)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  1,
			expected: outMmappedChunk,
		},
		{
			name: "call ix=3 on memSeries with 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*4)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  3,
			expected: outOpenHeadChunk,
		},
		{
			name: "call ix=0 on memSeries with 3 mmapped chunks and no headChunk",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*4)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
				s.headChunks = nil
			},
			inputID:  0,
			expected: outMmappedChunk,
		},
		{
			name: "call ix=2 on memSeries with 3 mmapped chunks and no headChunk",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*4)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
				s.headChunks = nil
			},
			inputID:  2,
			expected: outMmappedChunk,
		},
		{
			name: "call ix=3 on memSeries with 3 mmapped chunks and no headChunk",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*4)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
				s.headChunks = nil
			},
			inputID:  3,
			expected: outErr,
		},
		{
			name: "call ix=1 on memSeries with 3 mmapped chunks and closed ChunkDiskMapper",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*4)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
				cdm.Close()
			},
			inputID:  1,
			expected: outErr,
		},
		{
			name: "call ix=3 on memSeries with 3 mmapped chunks and closed ChunkDiskMapper",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*4)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
				cdm.Close()
			},
			inputID:  3,
			expected: outOpenHeadChunk,
		},
		{
			name: "call ix=0 on memSeries with 3 head chunks and no mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*3, cdm)
				require.Empty(t, s.mmappedChunks, "wrong number of mmappedChunks")
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, int64(0), s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*3)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  0,
			expected: outClosedHeadChunk,
		},
		{
			name: "call ix=1 on memSeries with 3 head chunks and no mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*3, cdm)
				require.Empty(t, s.mmappedChunks, "wrong number of mmappedChunks")
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, int64(0), s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*3)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  1,
			expected: outClosedHeadChunk,
		},
		{
			name: "call ix=10 on memSeries with 3 head chunks and no mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*3, cdm)
				require.Empty(t, s.mmappedChunks, "wrong number of mmappedChunks")
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Equal(t, int64(0), s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*3)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  10,
			expected: outErr,
		},
		{
			name: "call ix=0 on memSeries with 3 head chunks and 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")

				appendSamples(t, s, chunkRange*4, chunkRange*6, cdm)
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*6)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  0,
			expected: outMmappedChunk,
		},
		{
			name: "call ix=2 on memSeries with 3 head chunks and 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")

				appendSamples(t, s, chunkRange*4, chunkRange*6, cdm)
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*6)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  2,
			expected: outMmappedChunk,
		},
		{
			name: "call ix=3 on memSeries with 3 head chunks and 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")

				appendSamples(t, s, chunkRange*4, chunkRange*6, cdm)
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*6)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  3,
			expected: outClosedHeadChunk,
		},
		{
			name: "call ix=5 on memSeries with 3 head chunks and 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")

				appendSamples(t, s, chunkRange*4, chunkRange*6, cdm)
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*6)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  5,
			expected: outOpenHeadChunk,
		},
		{
			name: "call ix=6 on memSeries with 3 head chunks and 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")

				appendSamples(t, s, chunkRange*4, chunkRange*6, cdm)
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*6)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  6,
			expected: outErr,
		},

		{
			name: "call ix=10 on memSeries with 3 head chunks and 3 mmapped chunks",
			setup: func(t *testing.T, s *memSeries, cdm *chunks.ChunkDiskMapper) {
				appendSamples(t, s, 0, chunkRange*4, cdm)
				s.mmapChunks(cdm)
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, 1, s.headChunks.len(), "wrong number of headChunks")

				appendSamples(t, s, chunkRange*4, chunkRange*6, cdm)
				require.Equal(t, 3, s.headChunks.len(), "wrong number of headChunks")
				require.Len(t, s.mmappedChunks, 3, "wrong number of mmappedChunks")
				require.Equal(t, chunkRange*3, s.headChunks.oldest().minTime, "wrong minTime on last headChunks element")
				require.Equal(t, (chunkRange*6)-chunkStep, s.headChunks.maxTime, "wrong maxTime on first headChunks element")
			},
			inputID:  10,
			expected: outErr,
		},
	}

	memChunkPool := &sync.Pool{
		New: func() interface{} {
			return &memChunk{}
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			chunkDiskMapper, err := chunks.NewChunkDiskMapper(nil, dir, chunkenc.NewPool(), chunks.DefaultWriteBufferSize, chunks.DefaultWriteQueueSize)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, chunkDiskMapper.Close())
			}()

			series := newMemSeries(labels.EmptyLabels(), 1, 0, 0, 0, true)

			if tc.setup != nil {
				tc.setup(t, series, chunkDiskMapper)
			}

			chk, headChunk, isOpen, err := series.chunk(tc.inputID, chunkDiskMapper, memChunkPool)
			switch tc.expected {
			case outOpenHeadChunk:
				require.NoError(t, err, "unexpected error")
				require.True(t, headChunk, "expected a chunk with headChunk=true but got headChunk=%v", headChunk)
				require.True(t, isOpen, "expected a chunk with isOpen=true but got isOpen=%v", isOpen)
			case outClosedHeadChunk:
				require.NoError(t, err, "unexpected error")
				require.True(t, headChunk, "expected a chunk with headChunk=true but got headChunk=%v", headChunk)
				require.False(t, isOpen, "expected a chunk with isOpen=false but got isOpen=%v", isOpen)
			case outMmappedChunk:
				require.NoError(t, err, "unexpected error")
				require.False(t, headChunk, "expected a chunk with headChunk=false but got gc=%v", headChunk)
			case outErr:
				require.Nil(t, chk, "got a non-nil chunk reference returned with an error")
				require.Error(t, err)
			}
		})
	}
}

func TestHeadIndexReader_LabelValuesFor(t *testing.T) {
	getMemPostings := func() *index.MemPostings {
		mp := index.NewMemPostings()
		mp.Add(1, labels.FromStrings("a", "1", "b", "1"))
		mp.Add(2, labels.FromStrings("a", "1", "b", "2"))
		mp.Add(3, labels.FromStrings("a", "1", "b", "3"))
		mp.Add(4, labels.FromStrings("a", "1", "b", "4"))
		mp.Add(5, labels.FromStrings("a", "2", "b", "5"))
		return mp
	}

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		mp := getMemPostings()
		r := headIndexReader{
			head: &Head{
				postings: mp,
			},
		}
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})
		p := mp.Get("a", "1")

		it := r.LabelValuesFor(p, "b")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that b=5 is filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"1", "2", "3", "4"}, vals)
	})

	t.Run("filtering based on empty postings", func(t *testing.T) {
		mp := getMemPostings()
		r := headIndexReader{
			head: &Head{
				postings: mp,
			},
		}
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		it := r.LabelValuesFor(index.EmptyPostings(), "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})

	t.Run("non-existent label name", func(t *testing.T) {
		mp := getMemPostings()
		r := headIndexReader{
			head: &Head{
				postings: mp,
			},
		}
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})
		p := mp.Get("a", "1")

		it := r.LabelValuesFor(p, "c")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}

func TestHeadIndexReader_LabelValuesExcluding(t *testing.T) {
	getMemPostings := func() *index.MemPostings {
		mp := index.NewMemPostings()
		mp.Add(1, labels.FromStrings("a", "1", "b", "1"))
		mp.Add(2, labels.FromStrings("a", "1", "b", "2"))
		mp.Add(3, labels.FromStrings("a", "1", "b", "3"))
		mp.Add(4, labels.FromStrings("a", "1", "b", "4"))
		// Include this to verify that b=5 is still found, even if series with a=1 are excluded
		mp.Add(5, labels.FromStrings("a", "1", "b", "5"))
		// This should be the only value of 5 found, since a!=1
		mp.Add(6, labels.FromStrings("a", "2", "b", "5"))
		return mp
	}

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		mp := getMemPostings()
		r := headIndexReader{
			head: &Head{
				postings: mp,
			},
		}
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		p := mp.Get("a", "1")
		it := r.LabelValuesExcluding(p, "b")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that only b=5 is not filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"5"}, vals)
	})

	t.Run("filtering based on empty postings", func(t *testing.T) {
		mp := getMemPostings()
		r := headIndexReader{
			head: &Head{
				postings: mp,
			},
		}
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		it := r.LabelValuesExcluding(index.EmptyPostings(), "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		require.Equal(t, []string{"1", "2"}, vals)
	})

	t.Run("non-existent label name", func(t *testing.T) {
		mp := getMemPostings()
		r := headIndexReader{
			head: &Head{
				postings: mp,
			},
		}
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		it := r.LabelValuesExcluding(index.EmptyPostings(), "c")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}
