// Copyright 2025 Grafana Labs
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

package context

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DmitriyVTitov/size"
	"github.com/stretchr/testify/require"
)

func TestContextsTracker(t *testing.T) {
	t.Run("1 tracked context, tracked context doesn't get canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := NewContextsTracker()
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		require.NoError(t, tracker.Add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		tracker.Close()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("1 tracked context, tracked context gets canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := NewContextsTracker()
		t.Cleanup(tracker.Close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx1, cancelCtx1 := context.WithCancel(context.Background())
		require.NoError(t, tracker.Add(ctx1))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx1()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("2 tracked contexts, only 1 tracked context gets canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := NewContextsTracker()
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx1, cancelCtx1 := context.WithCancel(context.Background())
		require.NoError(t, tracker.Add(ctx1))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		require.NoError(t, tracker.Add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx1()
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		tracker.Close()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("2 tracked contexts, both contexts get canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := NewContextsTracker()
		t.Cleanup(tracker.Close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx1, cancelCtx1 := context.WithCancel(context.Background())
		require.NoError(t, tracker.Add(ctx1))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx2, cancelCtx2 := context.WithCancel(context.Background())
		require.NoError(t, tracker.Add(ctx2))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx1()
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx2()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("2 tracked contexts, 1 context gets canceled, the other context has deadline exceeded", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := NewContextsTracker()
		t.Cleanup(tracker.Close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx1, cancelCtx1 := context.WithCancel(context.Background())
		require.NoError(t, tracker.Add(ctx1))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx2, cancelCtx2 := context.WithDeadline(context.Background(), time.Now())
		t.Cleanup(cancelCtx2)
		require.NoError(t, tracker.Add(ctx2))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx1()

		// We expect the 2nd context deadline to expire immediately.
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("add() context to tracker but the tracker has been explicitly closed", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := NewContextsTracker()
		t.Cleanup(tracker.Close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		require.NoError(t, tracker.Add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		tracker.Close()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)

		require.Equal(t, ErrContextsTrackerClosed{}, tracker.Add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("add() context to tracker but the tracker has been implicitly closed because all tracked contexts have been canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := NewContextsTracker()
		t.Cleanup(tracker.Close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx, cancelCtx := context.WithCancel(context.Background())
		require.NoError(t, tracker.Add(ctx))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)

		require.Equal(t, ErrContextsTrackerCanceled{}, tracker.Add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})
}

func TestContextsTracker_Concurrency(t *testing.T) {
	const numContexts = 100

	t.Run("concurrently add and then cancel tracked contexts except 1", func(t *testing.T) {
		tracker, execCtx := NewContextsTracker()
		t.Cleanup(tracker.Close)

		cancels := make([]context.CancelFunc, 0, numContexts)

		// Concurrently add() concurrently.
		addWg := sync.WaitGroup{}
		addWg.Add(numContexts)

		for i := 0; i < numContexts; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)

			go func(ctx context.Context) {
				defer addWg.Done()
				tracker.Add(ctx)
			}(ctx)
		}

		// Wait until all add() functions have done.
		addWg.Wait()

		// Concurrently cancel all contexts minus 1.
		cancelWg := sync.WaitGroup{}
		cancelWg.Add(numContexts - 1)

		for i := 0; i < numContexts-1; i++ {
			go func(i int) {
				defer cancelWg.Done()
				cancels[i]()
			}(i)
		}

		// Wait until contexts have been canceled.
		cancelWg.Wait()

		// Since we canceled all contexts minus 1, we expect the execution context hasn't been canceled yet.
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		// Cancel the last context.
		cancels[numContexts-1]()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("concurrently add and then cancel tracked contexts", func(t *testing.T) {
		tracker, execCtx := NewContextsTracker()
		t.Cleanup(tracker.Close)

		cancels := make([]context.CancelFunc, 0, numContexts)

		// Concurrently add() concurrently.
		addWg := sync.WaitGroup{}
		addWg.Add(numContexts)

		for i := 0; i < numContexts; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)

			go func(ctx context.Context) {
				defer addWg.Done()
				tracker.Add(ctx)
			}(ctx)
		}

		// Wait until all add() functions have done.
		addWg.Wait()

		// Since we haven't canceled any tracked context, we don't expect the execution context has been canceled neither.
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		// Concurrently cancel all contexts.
		cancelWg := sync.WaitGroup{}
		cancelWg.Add(numContexts)

		for i := 0; i < numContexts; i++ {
			go func(i int) {
				defer cancelWg.Done()
				cancels[i]()
			}(i)
		}

		// Wait until contexts have been canceled.
		cancelWg.Wait()

		// Since we canceled all contexts, we expect the execution context has been canceled too.
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("concurrently close tracker", func(t *testing.T) {
		tracker, execCtx := NewContextsTracker()

		for i := 0; i < numContexts; i++ {
			tracker.Add(context.Background())
		}

		// Concurrently close tracker.
		fire := make(chan struct{})

		closeWg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			closeWg.Add(1)

			go func() {
				defer closeWg.Done()

				<-fire
				tracker.Close()
			}()
		}

		requireContextsTrackerExecutionContextDone(t, execCtx, false)
		close(fire)
		requireContextsTrackerExecutionContextDone(t, execCtx, true)

		// Wait until all gouroutines have done.
		closeWg.Wait()
	})
}

func requireContextsTrackerExecutionContextDone(t *testing.T, ctx context.Context, expected bool) {
	t.Helper()

	// The contextsTracker execution context is NOT cancelled synchronously once the tracked
	// contexts have done, so we allow for a short delay.
	select {
	case <-time.After(100 * time.Millisecond):
		if expected {
			t.Fatal("expected contextsTracker execution context to be done")
		}

	case <-ctx.Done():
		if !expected {
			t.Fatal("expected contextsTracker execution context to be not done")
		}
	}
}

func TestErrContextsTrackerClosed(t *testing.T) {
	err := ErrContextsTrackerClosed{}
	require.ErrorIs(t, err, ErrContextsTrackerClosed{})
	require.NotErrorIs(t, context.Canceled, ErrContextsTrackerClosed{})
	require.Equal(t, 0, size.Of(err))
}

func TestErrContextsTrackerCanceled(t *testing.T) {
	err := ErrContextsTrackerCanceled{}
	require.ErrorIs(t, err, ErrContextsTrackerCanceled{})
	require.NotErrorIs(t, context.Canceled, ErrContextsTrackerCanceled{})
	require.Equal(t, 0, size.Of(err))
}
