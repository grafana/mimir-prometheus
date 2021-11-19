package chunks

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var noopChunkWriter = func(_ HeadSeriesRef, _, _ int64, _ chunkenc.Chunk, _ *ChunkDiskMapperRef) error { return nil }

func TestChunkWriteQueueReadingFromQueue(t *testing.T) {
	q := newChunkWriteQueue(1000, noopChunkWriter)

	testChunk := chunkenc.NewXORChunk()
	var ref ChunkDiskMapperRef
	job := chunkWriteJob{
		chk: testChunk,
		ref: &ref,
	}
	q.addJob(job)

	gotChunk := q.get(&ref)

	require.Equal(t, testChunk, gotChunk)
}

func TestChunkWriteQueueWritingThroughQueue(t *testing.T) {
	var gotSeriesRef HeadSeriesRef
	var gotMint, gotMaxt int64
	var gotChunk chunkenc.Chunk
	var gotRef *ChunkDiskMapperRef

	chunkWriter := func(seriesRef HeadSeriesRef, mint, maxt int64, chunk chunkenc.Chunk, ref *ChunkDiskMapperRef) error {
		gotSeriesRef = seriesRef
		gotMint = mint
		gotMaxt = maxt
		gotChunk = chunk
		gotRef = ref
		return nil
	}
	q := newChunkWriteQueue(1000, chunkWriter)

	chunk := chunkenc.NewXORChunk()
	ref := newChunkDiskMapperRef(true, 3, 2)
	var seriesRef HeadSeriesRef = 1
	var mint, maxt int64 = 2, 3

	job := chunkWriteJob{
		seriesRef: seriesRef,
		mint:      mint,
		maxt:      maxt,
		chk:       chunk,
		ref:       ref,
	}
	q.addJob(job)

	// reference should be marked as enqueued
	gotEnqueued := ref.GetEnqueued()
	require.Equal(t, true, gotEnqueued)

	// queue should have one job
	require.Equal(t, q.queueIsEmpty(), false)

	// start processing of queue
	q.start()

	// wait for queue to be consumed
	q.stop()

	// queue should be empty
	require.Equal(t, q.queueIsEmpty(), true)

	// compare whether the write function has received all job attributes correctly
	require.Equal(t, seriesRef, gotSeriesRef)
	require.Equal(t, mint, gotMint)
	require.Equal(t, maxt, gotMaxt)
	require.Equal(t, chunk, gotChunk)
	require.Equal(t, ref.Load(), gotRef.Load())
}
