package chunks

import "sync"

// writeJobQueue is similar to buffered channel of chunkWriteJob, but manages its own buffers
// to avoid using a lot of memory when it's empty. It does that by storing elements into segments
// of equal size (segmentSize). When segment is not used anymore, reference to it are removed,
// so it can be treated as a garbage.
type writeJobQueue struct {
	maxSize     int
	segmentSize int

	mtx         sync.Mutex            // protects all following variables
	pushed      *sync.Cond            // signalled when something is pushed into the queue
	popped      *sync.Cond            // signalled when element is popped from the queue
	first, last *writeJobQueueSegment // pointer to first and last segment, if any
	size        int                   // total size of the queue
	closed      bool                  // after closing the queue, nothing can be pushed to it
}

type writeJobQueueSegment struct {
	queue []chunkWriteJob
	next  *writeJobQueueSegment
}

func newWriteJobQueue(maxSize, segmentSize int) *writeJobQueue {
	if maxSize <= 0 || segmentSize <= 0 {
		panic("invalid queue")
	}

	q := &writeJobQueue{
		maxSize:     maxSize,
		segmentSize: segmentSize,
	}

	q.pushed = sync.NewCond(&q.mtx)
	q.popped = sync.NewCond(&q.mtx)
	return q
}

func (q *writeJobQueue) close() {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.closed = true

	// unblock all blocked goroutines
	q.pushed.Broadcast()
	q.popped.Broadcast()
}

// push blocks until there is space available in the queue, and then adds job to the queue.
// If queue is closed or gets closed while waiting for space, push returns false.
func (q *writeJobQueue) push(job chunkWriteJob) bool {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	for {
		if q.closed {
			return false
		}

		if q.size >= q.maxSize {
			// wait until queue has more space or is closed
			q.popped.Wait()
			continue
		}

		// cap(q.last.queue)-len(q.last.queue) is free space remaining in the q.last.queue.
		if q.last == nil || cap(q.last.queue)-len(q.last.queue) == 0 {
			prevLast := q.last
			q.last = &writeJobQueueSegment{
				queue: make([]chunkWriteJob, 0, q.segmentSize),
			}

			if prevLast != nil {
				prevLast.next = q.last
			}
			if q.first == nil {
				q.first = q.last
			}
		}

		q.last.queue = append(q.last.queue, job)
		q.size++
		q.pushed.Signal()
		return true
	}
}

// pop returns first job from the queue, and true.
// if queue is empty, pop blocks until there is a job (returns true), or until queue is closed (returns false).
// If queue was already closed, pop first returns all remaining elements from the queue (with true value), and only then returns false.
func (q *writeJobQueue) pop() (chunkWriteJob, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	for {
		if q.size == 0 {
			if q.closed {
				return chunkWriteJob{}, false
			}

			// wait until something is pushed to the queue
			q.pushed.Wait()
			continue
		}

		res := q.first.queue[0]
		q.size--
		q.first.queue = q.first.queue[1:]

		// We don't want to check len(q.first.queue) == 0. It just means that q.first.queue is empty, but maybe
		// there is more free capacity in it and we can still use it (len=0, cap=3 means we can write 3 more elements to it).
		if cap(q.first.queue) == 0 {
			q.first = q.first.next
			if q.first == nil {
				q.last = nil
			}
		}

		q.popped.Signal()
		return res, true
	}
}

func (q *writeJobQueue) length() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	return q.size
}
