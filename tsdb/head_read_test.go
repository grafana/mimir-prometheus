package tsdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestBoundedChunk_Bytes(t *testing.T) {
	tests := []struct {
		name       string
		inputChunk chunkenc.Chunk
		inputMinT  int64
		inputMaxT  int64
		expBytes   []byte
	}{
		{
			name:       "if there are no samples only expect first two bytes",
			inputChunk: newTestChunk(0),
			expBytes:   []byte{0x0, 0x0},
		},
		{
			name:       "if there are no bounds only first sample is returned",
			inputChunk: newTestChunk(10),
			expBytes:   []byte{0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		},
		{
			name:       "if there are bounds only bytes from samples included are returned",
			inputChunk: newTestChunk(10),
			inputMinT:  1,
			inputMaxT:  8,
			expBytes:   []byte{0x0, 0x8, 0x2, 0x3f, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0xc2, 0x5f, 0xff, 0x6c, 0x6, 0xd6, 0x16, 0xda, 0xd, 0xb0, 0x2d, 0x2d, 0x42, 0x78},
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("name=%s", tc.name), func(t *testing.T) {
			chunk := boundedChunk{tc.inputChunk, tc.inputMinT, tc.inputMaxT}
			require.Equal(t, tc.expBytes, chunk.Bytes())
		})
	}
}

func TestBoundedChunk_Iterator(t *testing.T) {
	tests := []struct {
		name       string
		inputChunk chunkenc.Chunk
		inputMinT  int64
		inputMaxT  int64
		expSamples []sample
	}{
		{
			name:       "if there are no samples it returns nothing",
			inputChunk: newTestChunk(0),
			expSamples: nil,
		},
		{
			name:       "if there are no bounds set only sample at ts 0 is returned",
			inputChunk: newTestChunk(1),
			expSamples: []sample{
				{0, 0},
			},
		},
		{
			name:       "if there are bounds set only samples within them are returned",
			inputChunk: newTestChunk(10),
			inputMinT:  1,
			inputMaxT:  8,
			expSamples: []sample{
				{1, 1},
				{2, 2},
				{3, 3},
				{4, 4},
				{5, 5},
				{6, 6},
				{7, 7},
				{8, 8},
			},
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("name=%s", tc.name), func(t *testing.T) {
			chunk := boundedChunk{tc.inputChunk, tc.inputMinT, tc.inputMaxT}
			it := chunk.Iterator(nil)
			var samples []sample
			for it.Next() {
				t, v := it.At()
				samples = append(samples, sample{t, v})
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
