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

package chunks

import (
	"errors"
	"sync"
	"testing"
	"time"

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
	var (
		gotSeriesRef     HeadSeriesRef
		gotMint, gotMaxt int64
		gotChunk         chunkenc.Chunk
		gotRef           ChunkDiskMapperRef
		gotCutFile       bool
		chunkWriterWg    sync.WaitGroup
	)

	chunkWriterWg.Add(1)

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

	// Unblock the chunk writer.
	chunkWriterWg.Done()

	// Wait until queue is empty.
	require.Eventually(t, func() bool { return queueIsEmpty(q) }, time.Second, time.Millisecond*10)

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
	writeChunkCh := make(chan struct{}, sizeLimit)

	// blockingChunkWriter blocks until the writeChunk channel returns a value.
	blockingChunkWriter := func(seriesRef HeadSeriesRef, mint, maxt int64, chunk chunkenc.Chunk, ref ChunkDiskMapperRef, cutFile bool) error {
		<-writeChunkCh
		return nil
	}

	q := newChunkWriteQueue(nil, sizeLimit, blockingChunkWriter)
	defer q.stop()
	// Unblock writers when shutting down.
	defer close(writeChunkCh)

	var chunkRef ChunkDiskMapperRef
	addChunk := func() {
		require.NoError(t, q.addJob(chunkWriteJob{
			ref: chunkRef,
		}))
		chunkRef++
	}

	writeChunk := func() {
		writeChunkCh <- struct{}{}
	}

	// Fill the queue to the middle of the size limit.
	for job := 0; job < sizeLimit/2; job++ {
		addChunk()
	}

	// Consume the jobs.
	for job := 0; job < sizeLimit/2; job++ {
		writeChunk()
	}

	// Add jobs until the queue is full.
	// Note that one more queue than <sizeLimit> can be added because one will being processed by the worker already,
	// which will then block on the chunk write function.
	for job := 0; job < sizeLimit+1; job++ {
		addChunk()
	}

	// The queue should be full.
	require.True(t, queueIsFull(q))

	// Adding another job should block as long as no job from the queue gets consumed.
	addedJob := atomic.NewBool(false)
	go func() {
		addChunk()
		addedJob.Store(true)
	}()

	// Wait for 10ms while the adding of a new job is blocked.
	time.Sleep(time.Millisecond * 10)
	require.Equal(t, false, addedJob.Load())

	writeChunk()

	// Wait for 10ms to give the worker time to fully process a job.
	time.Sleep(time.Millisecond * 10)
	require.Equal(t, true, addedJob.Load())

	// The queue should be full again.
	require.True(t, queueIsFull(q))

	// Consume <sizeLimit> jobs from the queue.
	for job := 0; job < sizeLimit+1; job++ {
		require.Equal(t, false, queueIsEmpty(q))
		writeChunk()
	}

	require.Eventually(t, func() bool { return queueIsEmpty(q) }, time.Second, time.Millisecond*10)
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

	q := newChunkWriteQueue(nil, 1, chunkWriter)
	defer q.stop()

	job := chunkWriteJob{
		callback: callback,
	}
	require.NoError(t, q.addJob(job))

	require.Eventually(t, func() bool { return queueIsEmpty(q) }, time.Second, time.Millisecond*10)

	require.Equal(t, testError, gotError)
}

func queueIsEmpty(q *chunkWriteQueue) bool {
	return queueSize(q) == 0
}

func queueIsFull(q *chunkWriteQueue) bool {
	// When the queue is full and blocked on the writer the chunkRefMap has one more job than the cap of the jobCh
	// because one job is currently being processed and blocked in the writer.
	return queueSize(q) == cap(q.jobCh)+1
}

func queueSize(q *chunkWriteQueue) int {
	q.chunkRefMapMtx.Lock()
	defer q.chunkRefMapMtx.Unlock()

	// Looking at chunkRefMap instead of jobCh because the job is popped from the chan before it has
	// been fully processed, it remains in the chunkRefMap until the processing is complete.
	return len(q.chunkRefMap)
}
