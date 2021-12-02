package chunks

import (
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestChunkWriteQueue_GettingChunkFromQueue(t *testing.T) {
	var chunkWriterWg sync.WaitGroup
	chunkWriterWg.Add(1)

	// blockingChunkWriter blocks until chunkWriterWg is done.
	blockingChunkWriter := func(_ HeadSeriesRef, _, _ int64, _ chunkenc.Chunk, _ ChunkDiskMapperRef, _ bool) error {
		chunkWriterWg.Wait()
		return nil
	}

	q := newChunkWriteQueue(nil, 1000, blockingChunkWriter)

	defer q.stop()
	defer chunkWriterWg.Done()

	testChunk := chunkenc.NewXORChunk()
	var ref ChunkDiskMapperRef
	job := chunkWriteJob{
		chk: testChunk,
		ref: ref,
	}
	require.NoError(t, q.addJob(job))

	// Retrieve chunk from the queue.
	gotChunk := q.get(ref)
	require.Equal(t, testChunk, gotChunk)
}

func TestChunkWriteQueue_WritingThroughQueue(t *testing.T) {
	var chunkWriterWg sync.WaitGroup
	chunkWriterWg.Add(1)

	var gotSeriesRef HeadSeriesRef
	var gotMint, gotMaxt int64
	var gotChunk chunkenc.Chunk
	var gotRef ChunkDiskMapperRef
	var gotCutFile bool

	// blockingChunkWriter blocks until chunkWriterWg is done.
	blockingChunkWriter := func(seriesRef HeadSeriesRef, mint, maxt int64, chunk chunkenc.Chunk, ref ChunkDiskMapperRef, cutFile bool) error {
		gotSeriesRef = seriesRef
		gotMint = mint
		gotMaxt = maxt
		gotChunk = chunk
		gotRef = ref
		gotCutFile = cutFile
		chunkWriterWg.Wait()
		return nil
	}

	q := newChunkWriteQueue(nil, 1000, blockingChunkWriter)
	defer q.stop()

	seriesRef := HeadSeriesRef(1)
	var mint, maxt int64 = 2, 3
	chunk := chunkenc.NewXORChunk()
	ref := newChunkDiskMapperRef(321, 123)
	cutFile := true
	require.NoError(t, q.addJob(chunkWriteJob{seriesRef: seriesRef, mint: mint, maxt: maxt, chk: chunk, ref: ref, cutFile: cutFile}))

	// queue should have one job because the chunk writer is still blocked.
	require.Equal(t, false, q.IsEmpty())

	// Unblock the chunk writer.
	chunkWriterWg.Done()
	waitUntilConsumed(t, q)

	// queue should be empty
	require.Equal(t, true, q.IsEmpty())

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
	writeChunk := make(chan struct{}, sizeLimit)

	// blockingChunkWriter blocks until the writeChunk channel returns a value.
	blockingChunkWriter := func(seriesRef HeadSeriesRef, mint, maxt int64, chunk chunkenc.Chunk, ref ChunkDiskMapperRef, cutFile bool) error {
		<-writeChunk
		return nil
	}

	q := newChunkWriteQueue(nil, sizeLimit, blockingChunkWriter)
	defer q.stop()

	// Fill the queue to the middle of the size limit.
	for job := 0; job < sizeLimit/2; job++ {
		require.NoError(t, q.addJob(chunkWriteJob{}))
	}

	// Consume all except one job.
	for job := 0; job < sizeLimit/2-1; job++ {
		writeChunk <- struct{}{}
	}

	// The queue should not be empty, because there is one job left in it.
	require.Equal(t, false, q.IsEmpty())

	// Add jobs until the queue is full.
	for job := 0; job < sizeLimit-1; job++ {
		require.NoError(t, q.addJob(chunkWriteJob{}))
	}

	// The queue should not be full.
	require.Equal(t, true, q.IsFull())

	// Adding another job should block as long as no job from the queue gets consumed.
	addedJob := atomic.NewBool(false)
	go func() {
		require.NoError(t, q.addJob(chunkWriteJob{}))
		addedJob.Store(true)
	}()

	// Wait for 10ms while the adding of a new job is blocked.
	time.Sleep(time.Millisecond * 10)
	require.Equal(t, false, addedJob.Load())

	writeChunk <- struct{}{}

	// Wait for 10ms to give the worker time to fully process a job.
	time.Sleep(time.Millisecond * 10)
	require.Equal(t, true, addedJob.Load())

	// The queue should be full again.
	require.Equal(t, true, q.IsFull())

	// Consume <sizeLimit> jobs from the queue.
	for job := 0; job < sizeLimit; job++ {
		require.Equal(t, false, q.IsEmpty())
		writeChunk <- struct{}{}
	}

	waitUntilConsumed(t, q)

	// The queue should not be full anymore.
	require.Equal(t, false, q.IsFull())

	// Now the queue should be empty.
	require.Equal(t, true, q.IsEmpty())
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

	require.NoError(t, q.addJob(job))

	waitUntilConsumed(t, q)

	require.Equal(t, testError, gotError)
}

func waitUntilConsumed(t *testing.T, q *chunkWriteQueue) {
	t.Helper()

	if q == nil {
		return
	}

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
