package chunks

/*
import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

var noopChunkWriter = func(_ uint64, _ int64, _ int64, _ chunkenc.Chunk, _ *ChunkDiskMapperRef) error { return nil }

func TestChunkWriteQueueReadingFromQueue(t *testing.T) {
	q := newChunkWriteQueue(1000, noopChunkWriter)

	testChunk := chunkenc.NewXORChunk()
	var ref ChunkDiskMapperRef
	job := chunkWriteJob{
		chk: testChunk,
		ref: &ref,
	}
	q.add(job)

	gotChunk := q.get(&ref)

	require.Equal(t, testChunk, gotChunk)
}

func TestChunkWriteQueueWritingThroughQueue(t *testing.T) {
	var gotSeriesRef uint64
	var gotMint, gotMaxt int64
	var gotChunk chunkenc.Chunk
	testChunkRef := ChunkDiskMapperRef(4)

	chunkWriter := func(seriesRef uint64, mint int64, maxt int64, chunk chunkenc.Chunk) (ChunkDiskMapperRef, error) {
		gotSeriesRef = seriesRef
		gotMint = mint
		gotMaxt = maxt
		gotChunk = chunk
		return testChunkRef, nil
	}
	q := newChunkWriteQueueStarted(1000, chunkWriter)

	testChunk := chunkenc.NewXORChunk()
	var ref ChunkDiskMapperRef
	var seriesRef uint64 = 1
	var mint, maxt int64 = 2, 3

	job := chunkWriteJob{
		seriesRef: seriesRef,
		mint:      mint,
		maxt:      maxt,
		chk:       testChunk,
		ref:       &ref,
	}
	time.Sleep(time.Second)
	q.add(job)

	time.Sleep(time.Second)
	// waits for queue to be consumed
	q.stop()

	time.Sleep(time.Second)
	require.Equal(t, seriesRef, gotSeriesRef)
	require.Equal(t, mint, gotMint)
	require.Equal(t, maxt, gotMaxt)
	require.Equal(t, testChunk, gotChunk)
	require.Equal(t, *job.ref, testChunkRef)
}
*/
