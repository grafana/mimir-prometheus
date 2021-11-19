package chunks

import (
	"sync"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type chunkWriteJob struct {
	seriesRef HeadSeriesRef
	mint      int64
	maxt      int64
	chk       chunkenc.Chunk
	ref       *ChunkDiskMapperRef
}

type chunkWriteQueue struct {
	jobMtx  sync.RWMutex
	jobs    []chunkWriteJob
	headPos int
	tailPos int

	size      int
	sizeLimit chan struct{}

	workerCtrl chan struct{}
	workerWg   sync.WaitGroup

	writeChunk writeChunkF
}

type writeChunkF func(HeadSeriesRef, int64, int64, chunkenc.Chunk, *ChunkDiskMapperRef) error

func newChunkWriteQueueStarted(size int, writeChunk writeChunkF) *chunkWriteQueue {
	q := newChunkWriteQueue(size, writeChunk)
	q.start()
	return q
}

func newChunkWriteQueue(size int, writeChunk writeChunkF) *chunkWriteQueue {
	return &chunkWriteQueue{
		size:       size,
		jobs:       make([]chunkWriteJob, size),
		headPos:    -1,
		tailPos:    -1,
		sizeLimit:  make(chan struct{}, size),
		workerCtrl: make(chan struct{}, size),
		writeChunk: writeChunk,
	}
}

func (c *chunkWriteQueue) start() {
	c.workerWg.Add(1)

	go func() {
		defer c.workerWg.Done()

		for range c.workerCtrl {
			for !c.queueIsEmpty() {
				c.processJob()
			}
		}
	}()
}

func (c *chunkWriteQueue) queueIsEmpty() bool {
	c.jobMtx.RLock()
	defer c.jobMtx.RUnlock()

	return c._queueIsEmpty()
}

func (c *chunkWriteQueue) _queueIsEmpty() bool {
	return c.headPos < 0 || c.tailPos < 0
}

func (c *chunkWriteQueue) processJob() {
	job, ok := c.getJob()
	if !ok {
		return
	}

	err := c.writeChunk(job.seriesRef, job.mint, job.maxt, job.chk, job.ref)
	if err != nil && err != ErrChunkDiskMapperClosed {
		panic(err)
	}

	c.advanceTail()
}

func (c *chunkWriteQueue) advanceTail() {
	c.jobMtx.Lock()
	defer c.jobMtx.Unlock()

	if c.tailPos == c.headPos {
		// Queue is empty.
		c.tailPos = -1
		c.headPos = -1
	} else {
		c.tailPos = (c.tailPos + 1) % c.size
	}

	<-c.sizeLimit
}

func (c *chunkWriteQueue) getJob() (chunkWriteJob, bool) {
	c.jobMtx.Lock()
	defer c.jobMtx.Unlock()

	if c._queueIsEmpty() {
		return chunkWriteJob{}, false
	}

	return c.jobs[c.tailPos], true
}

func (c *chunkWriteQueue) addJob(job chunkWriteJob) {
	// if queue is full then block here
	c.sizeLimit <- struct{}{}

	c.jobMtx.Lock()
	defer c.jobMtx.Unlock()

	c.headPos = (c.headPos + 1) % c.size
	c.jobs[c.headPos] = job
	if c.tailPos < 0 {
		c.tailPos = c.headPos
	}
	job.ref.SetPositionInQueue(uint64(c.headPos))

	select {
	// non-blocking write to wake up worker because there is at least one job ready to consume
	case c.workerCtrl <- struct{}{}:
	default:
	}
}

func (c *chunkWriteQueue) get(ref *ChunkDiskMapperRef) chunkenc.Chunk {
	c.jobMtx.RLock()
	defer c.jobMtx.RUnlock()

	ok, queuePos := ref.GetPositionInQueue()
	if !ok {
		return nil
	}

	if c.headPos < c.tailPos && uint64(c.headPos) < queuePos && uint64(c.tailPos) > queuePos {
		// positions are wrapped around the size limit
		return nil
	}

	if c.headPos > c.tailPos && (uint64(c.headPos) < queuePos || uint64(c.tailPos) > queuePos) {
		// positions are in increasing order
		return nil
	}

	return c.jobs[queuePos].chk
}

func (c *chunkWriteQueue) stop() {
	close(c.workerCtrl)
	c.workerWg.Wait()
}
