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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
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
// Adding jobs to the job is non-blocking as long as the queue isn't full.
type chunkWriteQueue struct {
	jobCh chan chunkWriteJob

	chunkRefMapMtx sync.RWMutex
	chunkRefMap    map[ChunkDiskMapperRef]chunkenc.Chunk

	isStartedMtx sync.RWMutex
	isStarted    bool

	workerWg sync.WaitGroup

	writeChunk writeChunkF

	operationsMetric *prometheus.CounterVec
}

// writeChunkF is a function which writes chunks, it is dynamic to allow mocking in tests.
type writeChunkF func(HeadSeriesRef, int64, int64, chunkenc.Chunk, ChunkDiskMapperRef, bool) error

func newChunkWriteQueue(reg prometheus.Registerer, size int, writeChunk writeChunkF) *chunkWriteQueue {
	q := &chunkWriteQueue{
		jobCh:       make(chan chunkWriteJob, size),
		chunkRefMap: make(map[ChunkDiskMapperRef]chunkenc.Chunk, size),
		writeChunk:  writeChunk,

		operationsMetric: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_tsdb_chunk_write_queue_operations_total",
				Help: "Number of operations on the chunk_write_queue.",
			},
			[]string{"operation"},
		),
	}

	if reg != nil {
		reg.MustRegister(q.operationsMetric)
	}

	q.start()
	return q
}

func (c *chunkWriteQueue) start() {
	c.workerWg.Add(1)
	go func() {
		defer c.workerWg.Done()

		for job := range c.jobCh {
			c.processJob(job)
		}
	}()

	c.isStartedMtx.Lock()
	c.isStarted = true
	c.isStartedMtx.Unlock()
}

func (c *chunkWriteQueue) processJob(job chunkWriteJob) {
	err := c.writeChunk(job.seriesRef, job.mint, job.maxt, job.chk, job.ref, job.cutFile)
	if job.callback != nil {
		job.callback(err)
	}

	c.chunkRefMapMtx.Lock()
	defer c.chunkRefMapMtx.Unlock()

	delete(c.chunkRefMap, job.ref)

	c.operationsMetric.WithLabelValues("complete").Inc()
}

func (c *chunkWriteQueue) addJob(job chunkWriteJob) error {
	c.isStartedMtx.RLock()
	defer c.isStartedMtx.RUnlock()

	if !c.isStarted {
		return errors.New("queue is not started")
	}

	c.chunkRefMapMtx.Lock()
	c.chunkRefMap[job.ref] = job.chk
	c.chunkRefMapMtx.Unlock()

	c.jobCh <- job

	c.operationsMetric.WithLabelValues("add").Inc()

	return nil
}

func (c *chunkWriteQueue) get(ref ChunkDiskMapperRef) chunkenc.Chunk {
	c.chunkRefMapMtx.RLock()
	defer c.chunkRefMapMtx.RUnlock()

	chk, ok := c.chunkRefMap[ref]
	if ok {
		c.operationsMetric.WithLabelValues("get").Inc()
	}

	return chk
}

func (c *chunkWriteQueue) stop() {
	c.isStartedMtx.Lock()
	defer c.isStartedMtx.Unlock()

	c.isStarted = false

	close(c.jobCh)

	c.workerWg.Wait()
}
