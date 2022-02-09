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
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	// Minimum recorded peak since since the last freeing
	// of chunkWriteQueue.chunkrefMap to free it again.
	chunkRefMapFreeThreshold = 10

	// Minimum interval between freeing of chunkWriteQueue.chunkRefMap.
	chunkRefMapMinFreeInterval = 10 * time.Minute
)

type chunkWriteJob struct {
	cutFile   bool
	seriesRef HeadSeriesRef
	mint      int64
	maxt      int64
	chk       chunkenc.Chunk
	ref       ChunkDiskMapperRef
	callback  func(error)
}

// chunkWriteQueue is a queue for writing chunks to disk in a non-blocking fashion.
// Chunks that shall be written get added to the queue, which is consumed asynchronously.
// Adding jobs to the queue is non-blocking as long as the queue isn't full.
type chunkWriteQueue struct {
	jobs chan chunkWriteJob

	chunkRefMapMtx      sync.RWMutex
	chunkRefMap         map[ChunkDiskMapperRef]chunkenc.Chunk
	chunkRefMapPeakSize int       // largest size that chunkRefMap has grown to since the last time it got freed.
	chunkRefMapLastFree time.Time // when the chunkRefMap has been freed the last time.

	isRunningMtx sync.Mutex // Protects the isRunning property.
	isRunning    bool       // Used to prevent that new jobs get added to the queue when the chan is already closed.

	workerWg sync.WaitGroup

	writeChunk writeChunkF

	// Keeping three separate counters instead of only a single CounterVec to improve the performance of the critical
	// addJob() method which otherwise would need to perform a WithLabelValues call on the CounterVec.
	adds      prometheus.Counter
	gets      prometheus.Counter
	completed prometheus.Counter
	free      prometheus.Counter
}

// writeChunkF is a function which writes chunks, it is dynamic to allow mocking in tests.
type writeChunkF func(HeadSeriesRef, int64, int64, chunkenc.Chunk, ChunkDiskMapperRef, bool) error

func newChunkWriteQueue(reg prometheus.Registerer, size int, writeChunk writeChunkF) *chunkWriteQueue {
	counters := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_tsdb_chunk_write_queue_operations_total",
			Help: "Number of operations on the chunk_write_queue.",
		},
		[]string{"operation"},
	)

	q := &chunkWriteQueue{
		jobs:                make(chan chunkWriteJob, size),
		chunkRefMap:         make(map[ChunkDiskMapperRef]chunkenc.Chunk),
		chunkRefMapLastFree: time.Now(),
		writeChunk:          writeChunk,

		adds:      counters.WithLabelValues("add"),
		gets:      counters.WithLabelValues("get"),
		completed: counters.WithLabelValues("complete"),
		free:      counters.WithLabelValues("free"),
	}

	if reg != nil {
		reg.MustRegister(counters)
	}

	q.start()
	return q
}

func (c *chunkWriteQueue) start() {
	c.workerWg.Add(1)
	go func() {
		defer c.workerWg.Done()

		for job := range c.jobs {
			c.processJob(job)
		}
	}()

	c.isRunningMtx.Lock()
	c.isRunning = true
	c.isRunningMtx.Unlock()
}

func (c *chunkWriteQueue) processJob(job chunkWriteJob) {
	err := c.writeChunk(job.seriesRef, job.mint, job.maxt, job.chk, job.ref, job.cutFile)
	if job.callback != nil {
		job.callback(err)
	}

	c.chunkRefMapMtx.Lock()
	defer c.chunkRefMapMtx.Unlock()

	delete(c.chunkRefMap, job.ref)

	c.completed.Inc()

	c.freeChunkRefMap()
}

// freeChunkRefMap checks whether the conditions to free the chunkRefMap are met,
// if so it re-initializes it and frees the memory which it currently uses.
// The chunkRefMapMtx must be held when calling this method.
func (c *chunkWriteQueue) freeChunkRefMap() {
	if len(c.chunkRefMap) > 0 {
		// Can't free it while there is data in it.
		return
	}

	if c.chunkRefMapPeakSize < chunkRefMapFreeThreshold {
		// Not freeing it because it has not grown to the minimum threshold yet.
		return
	}

	if time.Since(c.chunkRefMapLastFree) < chunkRefMapMinFreeInterval {
		// Not freeing it because the minimum duration between free-events has not passed yet.
		return
	}

	// Re-initialize the chunk ref map to half of the peak size that it has grown to since the last re-init event.
	// By initializing it to half of the peak size since the last re-init event we try to hit the sweet spot in the
	// trade-off between initializing it to a very small size potentially resulting in many allocations to re-grow it,
	// and initializing it to a large size potentially resulting in unused allocated memory.
	c.chunkRefMap = make(map[ChunkDiskMapperRef]chunkenc.Chunk, c.chunkRefMapPeakSize/2)
	c.chunkRefMapPeakSize = 0
	c.chunkRefMapLastFree = time.Now()
	c.free.Inc()
}

func (c *chunkWriteQueue) addJob(job chunkWriteJob) (err error) {
	defer func() {
		if err == nil {
			c.adds.Inc()
		}
	}()

	// c.isRunningMtx serializes the adding of jobs to the c.chunkRefMap, if c.jobs is full then c.addJob() will block
	// while holding c.isRunningMtx, this guarantees that c.chunkRefMap won't ever grow beyond the queue size + 1.
	c.isRunningMtx.Lock()
	defer c.isRunningMtx.Unlock()

	if !c.isRunning {
		return errors.New("queue is not started")
	}

	c.chunkRefMapMtx.Lock()
	c.chunkRefMap[job.ref] = job.chk

	// Keep track of the peak usage of c.chunkRefMap.
	if len(c.chunkRefMap) > c.chunkRefMapPeakSize {
		c.chunkRefMapPeakSize = len(c.chunkRefMap)
	}
	c.chunkRefMapMtx.Unlock()

	c.jobs <- job

	return nil
}

func (c *chunkWriteQueue) get(ref ChunkDiskMapperRef) chunkenc.Chunk {
	c.chunkRefMapMtx.RLock()
	defer c.chunkRefMapMtx.RUnlock()

	chk, ok := c.chunkRefMap[ref]
	if ok {
		c.gets.Inc()
	}

	return chk
}

func (c *chunkWriteQueue) stop() {
	c.isRunningMtx.Lock()
	defer c.isRunningMtx.Unlock()

	if !c.isRunning {
		return
	}

	c.isRunning = false

	close(c.jobs)

	c.workerWg.Wait()
}

func (c *chunkWriteQueue) queueIsEmpty() bool {
	return c.queueSize() == 0
}

func (c *chunkWriteQueue) queueIsFull() bool {
	// When the queue is full and blocked on the writer the chunkRefMap has one more job than the cap of the jobCh
	// because one job is currently being processed and blocked in the writer.
	return c.queueSize() == cap(c.jobs)+1
}

func (c *chunkWriteQueue) queueSize() int {
	c.chunkRefMapMtx.Lock()
	defer c.chunkRefMapMtx.Unlock()

	// Looking at chunkRefMap instead of jobCh because the job is popped from the chan before it has
	// been fully processed, it remains in the chunkRefMap until the processing is complete.
	return len(c.chunkRefMap)
}
