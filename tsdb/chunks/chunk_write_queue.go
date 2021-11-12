package chunks

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type chunkWriteJob struct {
	seriesRef uint64
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

	writeChunk func(uint64, int64, int64, chunkenc.Chunk) (ChunkDiskMapperRef, error)
}

func newChunkWriteQueue(size int, writeChunk func(uint64, int64, int64, chunkenc.Chunk) (ChunkDiskMapperRef, error)) *chunkWriteQueue {
	q := chunkWriteQueue{
		size:       size,
		jobs:       make([]chunkWriteJob, size),
		sizeLimit:  make(chan struct{}, size),
		workerCtrl: make(chan struct{}),
		writeChunk: writeChunk,
	}

	q.worker()

	return &q
}

func (c *chunkWriteQueue) add(job chunkWriteJob) {
	// if queue is full then block here
	c.sizeLimit <- struct{}{}

	c.jobMtx.Lock()
	c.headPos = (c.headPos + 1) % c.size
	c.jobs[c.headPos] = job
	atomic.StoreUint64((*uint64)(job.ref), uint64(ChunkDiskMapperRef(c.headPos).Enqueued()))
	c.jobMtx.Unlock()

	select {
	// non-blocking write to wake up worker because there is a new job
	case c.workerCtrl <- struct{}{}:
	default:
	}
}

func (c *chunkWriteQueue) get(ref ChunkDiskMapperRef) chunkenc.Chunk {
	enqueued, _, _ := ref.Unpack()
	if !enqueued {
		return nil
	}

	queuePos := int(ref)

	c.jobMtx.RLock()
	defer c.jobMtx.RUnlock()

	if c.headPos < c.tailPos && c.headPos < queuePos && c.tailPos > queuePos {
		// positions are wrapped around the size limit
		return nil
	}

	if c.headPos > c.tailPos && (c.headPos > queuePos || c.tailPos < queuePos) {
		// positions are in increasing order
		return nil
	}

	return c.jobs[queuePos].chk
}

func (c *chunkWriteQueue) worker() {
	c.workerWg.Add(1)

	go func() {
		defer c.workerWg.Done()

		for range c.workerCtrl {
			queueIsEmpty := false
			for !queueIsEmpty {
				c.jobMtx.Lock()
				defer c.jobMtx.Unlock()

				queueIsEmpty = c.headPos == c.tailPos
				if !queueIsEmpty {
					job := c.jobs[c.tailPos]
					ref, err := c.writeChunk(job.seriesRef, job.mint, job.maxt, job.chk)
					if err != ErrChunkDiskMapperClosed {
						panic(err)
					}
					atomic.StoreUint64((*uint64)(job.ref), uint64(ref))
					c.tailPos = (c.tailPos + 1) % c.size
					<-c.sizeLimit
				}
			}
		}
	}()
}

func (c *chunkWriteQueue) Close() {
	close(c.workerCtrl)
	c.workerWg.Wait()
}
