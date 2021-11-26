package chunks

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var noopChunkWriter = func(_ HeadSeriesRef, _, _ int64, _ chunkenc.Chunk, _ ChunkDiskMapperRef, _ bool) error { return nil }

func TestChunkWriteQueue_GettingChunkFromQueue(t *testing.T) {
	q := newChunkWriteQueue(nil, 1000, noopChunkWriter)
	q.stop()

	testChunk := chunkenc.NewXORChunk()
	var ref ChunkDiskMapperRef
	job := chunkWriteJob{
		chk: testChunk,
		ref: ref,
	}
	q.addJob(job)

	gotChunk := q.get(ref)

	require.Equal(t, testChunk, gotChunk)
}

func TestChunkWriteQueue_WritingThroughQueue(t *testing.T) {
	var gotSeriesRef HeadSeriesRef
	var gotMint, gotMaxt int64
	var gotChunk chunkenc.Chunk
	var gotRef ChunkDiskMapperRef
	var gotCutFile bool

	chunkWriter := func(seriesRef HeadSeriesRef, mint, maxt int64, chunk chunkenc.Chunk, ref ChunkDiskMapperRef, cutFile bool) error {
		gotSeriesRef = seriesRef
		gotMint = mint
		gotMaxt = maxt
		gotChunk = chunk
		gotRef = ref
		gotCutFile = cutFile
		return nil
	}

	q := newChunkWriteQueue(nil, 1000, chunkWriter)
	q.stop()

	seriesRef := HeadSeriesRef(1)
	var mint, maxt int64 = 2, 3
	chunk := chunkenc.NewXORChunk()
	ref := newChunkDiskMapperRef(321, 123)
	cutFile := true
	q.addJob(chunkWriteJob{seriesRef: seriesRef, mint: mint, maxt: maxt, chk: chunk, ref: ref, cutFile: cutFile})

	// queue should have one job
	require.Equal(t, q.IsEmpty(), false)

	// process the queue
	q.start()
	defer q.stop()
	waitUntilConsumed(t, q)

	// queue should be empty
	require.Equal(t, q.IsEmpty(), true)

	// compare whether the write function has received all job attributes correctly
	require.Equal(t, seriesRef, gotSeriesRef)
	require.Equal(t, mint, gotMint)
	require.Equal(t, maxt, gotMaxt)
	require.Equal(t, chunk, gotChunk)
	require.Equal(t, ref, gotRef)
	require.Equal(t, cutFile, gotCutFile)
}

func TestChunkWriteQueue_WrappingAroundSizeLimit(t *testing.T) {
	sizeLimit := 100

	q := newChunkWriteQueue(nil, sizeLimit, noopChunkWriter)
	q.stop()

	// Fill the queue to the middle of the size limit.
	for job := 0; job < sizeLimit/2; job++ {
		q.addJob(chunkWriteJob{})
	}

	// Consume all except one job.
	for job := 0; job < sizeLimit/2-1; job++ {
		q.processJob()
	}

	// The queue should not be empty, because there is one job left in it.
	require.Equal(t, q.IsEmpty(), false)

	// Add jobs until the queue is full.
	for job := 0; job < sizeLimit-1; job++ {
		q.addJob(chunkWriteJob{})
	}

	// The queue should not be full.
	require.Equal(t, q.IsFull(), true)

	// Adding another job should block as long as no job from the queue gets consumed.
	addedJob := atomic.NewBool(false)
	go func() {
		q.addJob(chunkWriteJob{})
		addedJob.Store(true)
	}()

	// Wait for 10ms while the adding of a new job is blocked.
	time.Sleep(time.Millisecond * 10)
	require.Equal(t, addedJob.Load(), false)

	// Process one job to unblock the adding of the job.
	q.processJob()

	// Wait for 10ms to allow the adding of the job to finish.
	time.Sleep(time.Millisecond * 10)
	require.Equal(t, addedJob.Load(), true)

	// The queue should be full again.
	require.Equal(t, q.IsFull(), true)

	// Consume <sizeLimit> jobs from the queue.
	for job := 0; job < sizeLimit; job++ {
		require.Equal(t, q.IsEmpty(), false)
		q.processJob()
		require.Equal(t, q.IsFull(), false)
	}

	// Now the queue should be empty.
	require.Equal(t, q.IsEmpty(), true)
}

func TestChunkWriteQueue_HandlerErrorViaCallback(t *testing.T) {
	testError := errors.New("test error")
	chunkWriter := func(_ HeadSeriesRef, _, _ int64, _ chunkenc.Chunk, _ ChunkDiskMapperRef, _ bool) error {
		return testError
	}

	var gotError error
	callback := func(err error) {
		gotError = err
	}

	job := chunkWriteJob{
		callback: callback,
	}

	q := newChunkWriteQueue(nil, 1, chunkWriter)
	defer q.stop()
	q.addJob(job)

	waitUntilConsumed(t, q)

	require.Equal(t, testError, gotError)
}

func waitUntilConsumed(t *testing.T, q *chunkWriteQueue) {
	timeout := time.Second
	checkInterval := time.Millisecond * 10
	startTime := time.Now()

	if q.IsEmpty() {
		return
	}

	for {
		time.Sleep(checkInterval)

		if q.IsEmpty() {
			return
		}

		if time.Since(startTime) > timeout {
			t.Fatalf("Timed out waiting for queue to be consumed")
		}
	}
}
