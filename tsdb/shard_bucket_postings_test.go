// Copyright The Prometheus Authors
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

package tsdb

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

// expandShard drains the bucket lists of one shard into a sorted ref slice.
func expandShard(t *testing.T, s *shardBucketPostings, shardIndex, shardCount uint64) []storage.SeriesRef {
	t.Helper()
	lists, ok := s.postingsFor(shardIndex, shardCount)
	require.True(t, ok)
	refs, err := index.ExpandPostings(index.Merge(t.Context(), lists...))
	require.NoError(t, err)
	return refs
}

func TestShardBucketPostings(t *testing.T) {
	t.Run("membership partitions the refs across shards", func(t *testing.T) {
		t.Parallel()
		s := newShardBucketPostings(8)

		const numRefs = 1000
		byShard := map[uint64][]storage.SeriesRef{}
		for ref := chunks.HeadSeriesRef(1); ref <= numRefs; ref++ {
			hash := uint64(ref) * 0x9e3779b97f4a7c15 // Arbitrary spread.
			s.add(ref, hash)
			byShard[hash%4] = append(byShard[hash%4], storage.SeriesRef(ref))
		}
		require.Equal(t, numRefs, s.numSeries())

		var total int
		for shardIndex := range uint64(4) {
			got := expandShard(t, s, shardIndex, 4)
			require.True(t, slices.IsSorted(got))
			require.Equal(t, byShard[shardIndex], got)
			total += len(got)
		}
		require.Equal(t, numRefs, total)
	})

	t.Run("non-divisible shard counts are not served", func(t *testing.T) {
		t.Parallel()
		s := newShardBucketPostings(64)
		s.add(1, 42)

		for _, shardCount := range []uint64{0, 3, 5, 12, 65, 128} {
			_, ok := s.postingsFor(0, shardCount)
			require.False(t, ok, "shardCount %d", shardCount)
		}
		for _, shardCount := range []uint64{2, 4, 8, 16, 32, 64} {
			_, ok := s.postingsFor(0, shardCount)
			require.True(t, ok, "shardCount %d", shardCount)
		}
	})

	t.Run("nil means disabled", func(t *testing.T) {
		t.Parallel()
		var s *shardBucketPostings

		_, ok := s.postingsFor(0, 4)
		require.False(t, ok)
		require.Zero(t, s.numSeries())
		s.remove(map[storage.SeriesRef]struct{}{1: {}}) // Must not panic.
	})

	t.Run("remove drops deleted refs and keeps reader snapshots intact", func(t *testing.T) {
		t.Parallel()
		s := newShardBucketPostings(2)
		for ref := chunks.HeadSeriesRef(1); ref <= 10; ref++ {
			s.add(ref, uint64(ref))
		}

		// Capture a snapshot before removal.
		before := expandShard(t, s, 0, 2)

		deleted := map[storage.SeriesRef]struct{}{2: {}, 4: {}, 7: {}}
		s.remove(deleted)
		require.Equal(t, 7, s.numSeries())

		after := expandShard(t, s, 0, 2)
		for ref := range deleted {
			require.NotContains(t, after, ref)
		}
		// The pre-removal snapshot still contains the original refs.
		require.Contains(t, before, storage.SeriesRef(2))
	})

	t.Run("out-of-order adds are served sorted", func(t *testing.T) {
		t.Parallel()
		s := newShardBucketPostings(1)
		for _, ref := range []chunks.HeadSeriesRef{5, 3, 9, 1, 7} {
			s.add(ref, 0)
		}

		got := expandShard(t, s, 0, 1)
		require.Equal(t, []storage.SeriesRef{1, 3, 5, 7, 9}, got)
	})

	t.Run("concurrent adds, removes and reads", func(t *testing.T) {
		t.Parallel()
		s := newShardBucketPostings(8)

		const (
			writers       = 4
			refsPerWriter = 10_000
		)

		var wg sync.WaitGroup
		// Writers add interleaved refs (out of order across goroutines) and
		// remove half of their own again, racing bucket re-sorts and prunes.
		for w := range writers {
			wg.Go(func() {
				for i := range refsPerWriter {
					ref := chunks.HeadSeriesRef(i*writers + w + 1)
					s.add(ref, uint64(ref))
					if i%2 == 0 {
						s.remove(map[storage.SeriesRef]struct{}{storage.SeriesRef(ref): {}})
					}
				}
			})
		}
		// Readers verify every captured shard list is sorted.
		for range 2 {
			wg.Go(func() {
				for range 200 {
					for shardIndex := range uint64(4) {
						lists, ok := s.postingsFor(shardIndex, 4)
						if !ok {
							panic("postingsFor unexpectedly not ok")
						}
						refs, err := index.ExpandPostings(index.Merge(t.Context(), lists...))
						if err != nil {
							panic(err)
						}
						if !slices.IsSorted(refs) {
							panic("shard list not sorted")
						}
					}
				}
			})
		}
		wg.Wait()

		// After the dust settles: odd writer-iterations stay, even ones are removed.
		require.Equal(t, writers*refsPerWriter/2, s.numSeries())
		var got []storage.SeriesRef
		for shardIndex := range uint64(4) {
			got = append(got, expandShard(t, s, shardIndex, 4)...)
		}
		require.Len(t, got, writers*refsPerWriter/2)
	})

	t.Run("filter matches intersect output", func(t *testing.T) {
		t.Parallel()
		s := newShardBucketPostings(8)
		rng := rand.New(rand.NewSource(42))
		all := make([]storage.SeriesRef, 0, 5000)
		for ref := chunks.HeadSeriesRef(1); ref <= 5000; ref++ {
			s.add(ref, rng.Uint64())
			all = append(all, storage.SeriesRef(ref))
		}
		// Input: every third ref, split into a merge tree of two
		// interleaved sub-lists.
		var inA, inB []storage.SeriesRef
		for i, ref := range all {
			if i%3 != 0 {
				continue
			}
			if i%2 == 0 {
				inA = append(inA, ref)
			} else {
				inB = append(inB, ref)
			}
		}
		treeInput := func() index.Postings {
			return index.Merge(t.Context(), index.NewListPostings(inA), index.NewListPostings(inB))
		}

		for shardIndex := range uint64(4) {
			lists, ok := s.postingsFor(shardIndex, 4)
			require.True(t, ok)
			got, err := index.ExpandPostings(newShardFilterPostings(treeInput(), index.Merge(t.Context(), lists...)))
			require.NoError(t, err)

			lists, ok = s.postingsFor(shardIndex, 4)
			require.True(t, ok)
			want, err := index.ExpandPostings(index.Intersect(treeInput(), index.Merge(t.Context(), lists...)))
			require.NoError(t, err)
			require.Equal(t, want, got, "shard %d", shardIndex)
		}
	})

	t.Run("filter stops when buckets are exhausted", func(t *testing.T) {
		t.Parallel()
		buckets := index.NewListPostings([]storage.SeriesRef{2, 4})
		input := index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5, 1000})

		f := newShardFilterPostings(input, buckets)
		require.True(t, f.Next())
		require.Equal(t, storage.SeriesRef(2), f.At())
		require.True(t, f.Next())
		require.Equal(t, storage.SeriesRef(4), f.At())
		require.False(t, f.Next())
		require.NoError(t, f.Err())
	})

	t.Run("filter seek is monotone", func(t *testing.T) {
		t.Parallel()
		buckets := index.NewListPostings([]storage.SeriesRef{1, 3, 5, 7, 9})
		input := index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5, 6, 7, 8, 9})

		f := newShardFilterPostings(input, buckets)
		// Seek before any Next must position at the first member, even for
		// v == 0 (no phantom zero ref).
		require.True(t, f.Seek(0))
		require.Equal(t, storage.SeriesRef(1), f.At())
		require.True(t, f.Seek(4))
		require.Equal(t, storage.SeriesRef(5), f.At())
		require.True(t, f.Seek(5)) // Already at or past v: no movement.
		require.Equal(t, storage.SeriesRef(5), f.At())
		require.True(t, f.Seek(8))
		require.Equal(t, storage.SeriesRef(9), f.At())
		require.False(t, f.Seek(10))
		require.NoError(t, f.Err())
	})
}

func BenchmarkShardBucketPostings(b *testing.B) {
	// Precompute random shard hashes once so the hot loop measures the
	// structure (lock + append), not StableHash. Random spread mimics how
	// StableHash scatters series across all buckets.
	const numHashes = 1 << 16
	hashes := make([]uint64, numHashes)
	rng := rand.New(rand.NewSource(1))
	for i := range hashes {
		hashes[i] = rng.Uint64()
	}

	// populate fills a fresh structure with numSeries refs added in increasing
	// ref order, so every bucket ends up sorted (no dirty buckets).
	populate := func(numSeries int) *shardBucketPostings {
		s := newShardBucketPostings(DefaultShardedPostingsBuckets)
		for i := range numSeries {
			s.add(chunks.HeadSeriesRef(i+1), hashes[i%numHashes])
		}
		return s
	}

	// add is the write path: one global-mutex acquire + append per series.
	b.Run("add", func(b *testing.B) {
		b.Run("serial", func(b *testing.B) {
			s := newShardBucketPostings(DefaultShardedPostingsBuckets)
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				s.add(chunks.HeadSeriesRef(i+1), hashes[i%numHashes])
			}
		})

		// parallel exposes contention on the single shardBucketPostings mutex —
		// the per-creation cost finding #1 is about. Run with -cpu 1,4,8,18 to
		// see how add scales with concurrent creators.
		b.Run("parallel", func(b *testing.B) {
			s := newShardBucketPostings(DefaultShardedPostingsBuckets)
			var goroutine atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				// Each goroutine owns a disjoint, monotonic ref range, so only
				// cross-goroutine interleaving (not self) drives bucket churn —
				// matching globally increasing series IDs in production.
				ref := chunks.HeadSeriesRef(goroutine.Add(1) << 40)
				i := 0
				for pb.Next() {
					s.add(ref, hashes[i%numHashes])
					ref++
					i++
				}
			})
		})
	})

	// postingsFor is the read path: read lock, (re)sort dirty buckets once,
	// then snapshot one list per bucket in the shard. populate leaves buckets
	// sorted, so this measures the steady-state read, not the one-off sort.
	b.Run("postingsFor", func(b *testing.B) {
		const numSeries = 1_000_000
		for _, shardCount := range []uint64{16, 64} {
			b.Run(fmt.Sprintf("shardCount=%d", shardCount), func(b *testing.B) {
				s := populate(numSeries)
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					if _, ok := s.postingsFor(uint64(i)%shardCount, shardCount); !ok {
						b.Fatal("postingsFor not ok")
					}
				}
			})
		}
	})

	// remove is the GC path: scan all buckets, rebuild those holding a deleted
	// ref. It mutates, so re-populate per iteration (untimed). remove is fast
	// relative to populate, so run this sub-benchmark with -benchtime=200x.
	b.Run("remove", func(b *testing.B) {
		const numSeries = 100_000
		for b.Loop() {
			b.StopTimer()
			s := populate(numSeries)
			deleted := make(map[storage.SeriesRef]struct{}, numSeries/8)
			for i := 1; i <= numSeries; i += 8 { // ~1/8 churn slice, like a head GC.
				deleted[storage.SeriesRef(i)] = struct{}{}
			}
			b.StartTimer()
			s.remove(deleted)
		}
	})
}
