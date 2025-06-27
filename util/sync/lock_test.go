package sync

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestStripedLock_WithRLock(t *testing.T) {
	sl := NewStripedLock(1)
	var wg sync.WaitGroup
	counter := atomic.Int32{}

	// Multiple readers should be able to run concurrently
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sl.WithRLock(0, func() {
				counter.Inc()
				time.Sleep(20 * time.Millisecond)
			})
		}()
	}

	wg.Wait()
	require.Equal(t, int32(3), counter.Load())
}

func TestStripedLock_WithLock(t *testing.T) {
	sl := NewStripedLock(1)
	var wg sync.WaitGroup
	counter := atomic.Int32{}
	maxConcurrent := atomic.Int32{}

	// Writers should be mutually exclusive
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sl.WithLock(0, func() {
				if counter.Inc() > maxConcurrent.Load() {
					maxConcurrent.Store(counter.Load())
				}
				time.Sleep(20 * time.Millisecond)
				counter.Dec()
			})
		}()
	}

	wg.Wait()
	require.Equal(t, int32(1), maxConcurrent.Load())
}
