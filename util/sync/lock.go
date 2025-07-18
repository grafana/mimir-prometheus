package sync

import (
	"sync"
)

// StripedLock is a read-write lock that minimizes contention by using multiple locks internally.
type StripedLock struct {
	locks []sync.RWMutex
}

// NewStripedLock creates a new striped lock with the number of stripes.
func NewStripedLock(numStripes int) *StripedLock {
	return &StripedLock{
		locks: make([]sync.RWMutex, numStripes),
	}
}

// WithRLock executes a function while holding a read lock for the lock corresponding to the hashKey. The hashKey should
// be consistent when reading or writing items that require the same lock.
func (sl *StripedLock) WithRLock(hashKey int, fn func()) {
	i := hashKey % len(sl.locks)
	sl.locks[i].RLock()
	defer sl.locks[i].RUnlock()
	fn()
}

// WithLock executes a function while holding a write lock for the lock corresponding to the hashKey. The hashKey should
// be consistent when reading or writing items that require the same lock.
func (sl *StripedLock) WithLock(hashKey int, fn func()) {
	i := hashKey % len(sl.locks)
	sl.locks[i].Lock()
	defer sl.locks[i].Unlock()
	fn()
}
