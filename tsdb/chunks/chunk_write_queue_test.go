package chunks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var noopChunkWriter = func(_ HeadSeriesRef, _, _ int64, _ chunkenc.Chunk, _ *ChunkDiskMapperRef) error { return nil }

func TestChunkWriteQueue_GettingChunkFromQueue(t *testing.T) {
	q := newChunkWriteQueue(1000, noopChunkWriter)
	q.stop()

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

func TestChunkWriteQueue_WritingThroughQueue(t *testing.T) {
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
	q.stop()

	chunk := chunkenc.NewXORChunk()
	ref := &ChunkDiskMapperRef{}
	ref.SetPositionInQueue(2)
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
	ok, pos := ref.GetPositionInQueue()
	require.Equal(t, true, ok)
	require.Equal(t, uint64(0), pos)

	// queue should have one job
	require.Equal(t, q.queueIsEmpty(), false)

	// process the queue
	q.start()
	waitUntilConsumed(t, q)

	// queue should be empty
	require.Equal(t, q.queueIsEmpty(), true)

	// compare whether the write function has received all job attributes correctly
	require.Equal(t, seriesRef, gotSeriesRef)
	require.Equal(t, mint, gotMint)
	require.Equal(t, maxt, gotMaxt)
	require.Equal(t, chunk, gotChunk)
	require.Equal(t, ref.Load(), gotRef.Load())
}

func waitUntilConsumed(t *testing.T, q *chunkWriteQueue) {
	timeout := time.Second
	checkInterval := time.Millisecond * 10
	startTime := time.Now()
	for range time.After(checkInterval) {
		if q.queueIsEmpty() {
			return
		}
		if time.Since(startTime) > timeout {
			t.Fatalf("Timed out waiting for queue to be consumed")
		}
	}
}
