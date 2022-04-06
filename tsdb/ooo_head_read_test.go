package tsdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type chunkInterval struct {
	// because we permutate the order of chunks, we cannot determine at test declaration time which chunkRefs we expect in the Output.
	// This ID matches expected output chunks against test input chunks, the test runner will assert the chunkRef for the matching chunk
	ID   int
	mint int64
	maxt int64
}

// permutateChunkIntervals returns all possible orders of the given chunkIntervals
func permutateChunkIntervals(in []chunkInterval, out [][]chunkInterval, left, right int) [][]chunkInterval {
	if left == right {
		inCopy := make([]chunkInterval, len(in))
		copy(inCopy, in)
		return append(out, inCopy)
	}
	for i := left; i <= right; i++ {
		in[left], in[i] = in[i], in[left]
		out = permutateChunkIntervals(in, out, left+1, right)
		in[left], in[i] = in[i], in[left]
	}
	return out
}

// TestOOOHeadIndexReader_Series tests that the Series method works as expected.
// However it does so by creating chunks and memory mapping them unlike other
// tests of the head where samples are appended and we let the head memory map.
// We do this because the ingestion path and the appender for out of order
// samples are not ready yet.
func TestOOOHeadIndexReader_Series(t *testing.T) {
	tests := []struct {
		name                string
		queryMinT           int64
		queryMaxT           int64
		inputChunkIntervals []chunkInterval
		expSeriesError      bool
		expChunks           []chunkInterval
	}{
		{
			name:           "Test1: Empty result and no error when head is empty",
			queryMinT:      0,
			queryMaxT:      100,
			expSeriesError: false,
			expChunks:      nil,
		},
		{
			name:      "Test2: If query interval is bigger than the existing chunks nothing is returned",
			queryMinT: 500,
			queryMaxT: 700,
			inputChunkIntervals: []chunkInterval{
				{0, 100, 400},
			},
			expSeriesError: false,
			// ts                    0       100       150       200       250       300       350       400       450       500       550       600       650       700
			// Query Interval                                                                                                  [---------------------------------------]
			// Chunk 0                         [-----------------------------------------------------------]
			// Expected Output  Empty
			expChunks: nil,
		},
		{
			name:      "Test3: If query interval is smaller than the existing chunks nothing is returned",
			queryMinT: 100,
			queryMaxT: 400,
			inputChunkIntervals: []chunkInterval{
				{0, 500, 700},
			},
			expSeriesError: false,
			// ts                    0       100       150       200       250       300       350       400       450       500       550       600       650       700
			// Query Interval                [-----------------------------------------------------------]
			// Chunk 0:                                                                                                        [---------------------------------------]
			// Expected Output  Empty
			expChunks: nil,
		},
		{
			name:      "Test4: Pairwise overlaps should return the references of the first of each pair",
			queryMinT: 0,
			queryMaxT: 700,
			inputChunkIntervals: []chunkInterval{
				{0, 100, 200},
				{1, 500, 600},
				{2, 150, 250},
				{3, 550, 650},
			},
			expSeriesError: false,
			// ts                    0       100       150       200       250       300       350       400       450       500       550       600       650       700
			// Query Interval        [---------------------------------------------------------------------------------------------------------------------------------]
			// Chunk 0:                        [-------------------]
			// Chunk 1:                                                                                                        [-------------------]
			// Chunk 2:                                  [-------------------]
			// Chunk 3:                                                                                                                  [-------------------]
			// Expected Output  [0x1000000, 0x1000001] with OOOLastReferences pointing to 0x1000003
			// Output Graphically              [-----------------------------]                                                 [-----------------------------]
			expChunks: []chunkInterval{
				{0, 100, 250},
				{1, 500, 650},
			},
		},
		{
			name:      "Test5: If all chunks overlap, single big chunk is returned",
			queryMinT: 0,
			queryMaxT: 700,
			inputChunkIntervals: []chunkInterval{
				{0, 100, 200},
				{1, 200, 300},
				{2, 300, 400},
				{3, 400, 500},
			},
			expSeriesError: false,
			// ts                    0       100       150       200       250       300       350       400       450       500       550       600       650       700
			// Query Interval        [---------------------------------------------------------------------------------------------------------------------------------]
			// Chunk 0:                        [-------------------]
			// Chunk 1:                                            [-------------------]
			// Chunk 2:                                                                [-------------------]
			// Chunk 3:                                                                                    [------------------]
			// Expected Output  [0x1000000] with OOOLastReferences pointing to 0x1000003
			// Output Graphically              [------------------------------------------------------------------------------]
			expChunks: []chunkInterval{
				{0, 100, 500},
			},
		},
		{
			name:      "Test6: If no chunks overlap, all chunks are returned",
			queryMinT: 0,
			queryMaxT: 700,
			inputChunkIntervals: []chunkInterval{
				{0, 100, 199},
				{1, 200, 299},
				{2, 300, 399},
				{3, 400, 499},
			},
			expSeriesError: false,
			// ts                    0       100       150       200       250       300       350       400       450       500       550       600       650       700
			// Query Interval        [---------------------------------------------------------------------------------------------------------------------------------]
			// Chunk 0:                        [------------------]
			// Chunk 1:                                            [------------------]
			// Chunk 2:                                                                [------------------]
			// Chunk 3:                                                                                    [------------------]
			// Expected Output  [0x1000000, 0x1000001, 0x1000002, 0x1000003] with OOOLastReferences pointing to 0x1000003
			// Output Graphically              [------------------][------------------][------------------][------------------]
			expChunks: []chunkInterval{
				{0, 100, 199},
				{1, 200, 299},
				{2, 300, 399},
				{3, 400, 499},
			},
		},
		{
			name:      "Test7: a full overlap pair and disjointed triplet",
			queryMinT: 0,
			queryMaxT: 900,
			inputChunkIntervals: []chunkInterval{
				{0, 100, 300},
				{1, 770, 850},
				{2, 150, 250},
				{3, 650, 750},
				{4, 600, 800},
			},
			expSeriesError: false,
			// ts                    0       100       150       200       250       300       350       400       450       500       550       600       650       700       750       800       850
			// Query Interval        [---------------------------------------------------------------------------------------------------------------------------------------------------------------]
			// Chunk 0:                        [---------------------------------------]
			// Chunk 1:                                                                                                                                                               [--------------]
			// Chunk 2:                                  [-------------------]
			// Chunk 3:                                                                                                                                      [-------------------]
			// Chunk 4:                                                                                                                             [---------------------------------------]
			// Expected Output  [0x1000000, 0x1000004] With OOOLastReferences pointing to 0v1000004
			// Output Graphically              [---------------------------------------]                                                            [------------------------------------------------]
			expChunks: []chunkInterval{
				{0, 100, 300},
				{4, 600, 850},
			},
		},
	}

	s1Lset := labels.FromStrings("foo", "bar")
	s1ID := uint64(1)

	for _, tc := range tests {
		var permutations [][]chunkInterval
		if len(tc.inputChunkIntervals) == 0 {
			// handle special case
			permutations = [][]chunkInterval{
				nil,
			}
		} else {
			permutations = permutateChunkIntervals(tc.inputChunkIntervals, nil, 0, len(tc.inputChunkIntervals)-1)
		}
		for perm, intervals := range permutations {
			for _, headChunk := range []bool{false, true} {
				t.Run(fmt.Sprintf("name=%s, permutation=%d, headChunk=%t", tc.name, perm, headChunk), func(t *testing.T) {
					h, _ := newTestHead(t, 1000, false)
					defer func() {
						require.NoError(t, h.Close())
					}()
					require.NoError(t, h.Init(0))

					s1, _, _ := h.getOrCreate(s1ID, s1Lset)

					// define our expected chunks, by looking at the expected ChunkIntervals and setting...
					var expChunks []chunks.Meta
					for _, e := range tc.expChunks {
						meta := chunks.Meta{
							Chunk:   chunkenc.Chunk(nil),
							MinTime: e.mint,
							MaxTime: e.maxt,
							// 1) markers corresponding to the last chunk within this permutation.
							OOOLastMinTime: intervals[len(intervals)-1].mint,
							OOOLastMaxTime: intervals[len(intervals)-1].maxt,
							// 2) LastRef to simply be whatever was the highest ref seen, always
							OOOLastRef: chunks.ChunkRef(chunks.NewHeadChunkRef(1, chunks.HeadChunkID(uint64(len(intervals)-1)))),
						}

						// 3) Ref to whatever Ref the chunk has, that we refer to by ID
						for ref, c := range intervals {
							if c.ID == e.ID {
								meta.Ref = chunks.ChunkRef(chunks.NewHeadChunkRef(1, chunks.HeadChunkID(ref)))
								break
							}
						}
						expChunks = append(expChunks, meta)
					}

					if headChunk && len(intervals) > 0 {
						// Put the last interval in the head chunk
						//t.Logf("headchunk %v", intervals[len(intervals)-1])
						s1.oooHeadChunk = &oooHeadChunk{
							minTime: intervals[len(intervals)-1].mint,
							maxTime: intervals[len(intervals)-1].maxt,
						}
						intervals = intervals[:len(intervals)-1]
					}

					for _, ic := range intervals {
						s1.oooMmappedChunks = append(s1.oooMmappedChunks, &mmappedChunk{
							minTime: ic.mint,
							maxTime: ic.maxt,
						})
						//t.Logf("     chunk %v", ic)
					}

					ir := NewOOOHeadIndexReader(h, tc.queryMinT, tc.queryMaxT)

					var chks []chunks.Meta
					var respLset labels.Labels
					err := ir.Series(storage.SeriesRef(s1ID), &respLset, &chks)
					if tc.expSeriesError {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, s1Lset, respLset)

					require.Equal(t, expChunks, chks)
				})
			}
		}
	}
}
