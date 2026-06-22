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
	lists, _ := s.postingsFor(shardIndex, shardCount)
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

	t.Run("shards partition refs across bucket and shard counts", func(t *testing.T) {
		t.Parallel()
		// Every shard count is served: those that divide the bucket count via
		// whole buckets, the rest by sub-filtering candidate buckets on the
		// shard hash. Both must equal the brute-force shard membership and
		// partition the full ref set exactly.
		const numRefs = 5000
		rng := rand.New(rand.NewSource(7))
		for _, bucketCount := range []int{64, 128} {
			s := newShardBucketPostings(bucketCount)
			refHashes := make(map[storage.SeriesRef]uint64, numRefs)
			for ref := chunks.HeadSeriesRef(1); ref <= numRefs; ref++ {
				h := rng.Uint64()
				s.add(ref, h)
				refHashes[storage.SeriesRef(ref)] = h
			}

			for _, shardCount := range []uint64{2, 6, 64, 96, 128, 192} {
				want := map[uint64][]storage.SeriesRef{}
				for ref := storage.SeriesRef(1); ref <= numRefs; ref++ {
					sh := refHashes[ref] % shardCount
					want[sh] = append(want[sh], ref) // appended in ref order => sorted.
				}

				seen := map[storage.SeriesRef]struct{}{}
				var total int
				for shardIndex := range shardCount {
					got := expandShard(t, s, shardIndex, shardCount)
					require.True(t, slices.IsSorted(got), "bucketCount=%d shardCount=%d shard=%d", bucketCount, shardCount, shardIndex)
					require.Equal(t, want[shardIndex], got, "bucketCount=%d shardCount=%d shard=%d", bucketCount, shardCount, shardIndex)
					for _, ref := range got {
						_, dup := seen[ref]
						require.False(t, dup, "ref %d returned by multiple shards (bucketCount=%d shardCount=%d)", ref, bucketCount, shardCount)
						seen[ref] = struct{}{}
					}
					total += len(got)
				}
				require.Equal(t, numRefs, total, "shards must partition all refs (bucketCount=%d shardCount=%d)", bucketCount, shardCount)
			}
		}
	})

	t.Run("sub-filters when the shard count does not divide the bucket count", func(t *testing.T) {
		t.Parallel()
		s := newShardBucketPostings(64)
		s.add(1, 42)

		// A zero shard count selects nothing.
		lists, subFiltered := s.postingsFor(0, 0)
		require.Nil(t, lists)
		require.False(t, subFiltered)

		// Counts that do not divide 64 are served by sub-filtering.
		for _, shardCount := range []uint64{3, 5, 12, 65, 128} {
			_, subFiltered := s.postingsFor(0, shardCount)
			require.True(t, subFiltered, "shardCount %d", shardCount)
		}
		// Counts that divide 64 use the exact (non-sub-filtered) path.
		for _, shardCount := range []uint64{1, 2, 4, 8, 16, 32, 64} {
			_, subFiltered := s.postingsFor(0, shardCount)
			require.False(t, subFiltered, "shardCount %d", shardCount)
		}
	})

	t.Run("sub-filter stays aligned through resort and remove", func(t *testing.T) {
		t.Parallel()
		// Regression guard: out-of-order adds mark buckets dirty (re-sorted on
		// read) and remove rebuilds buckets; both must keep each ref's hash
		// aligned, or the sub-filter would route refs to the wrong shard.
		s := newShardBucketPostings(64)
		refHashes := map[storage.SeriesRef]uint64{}
		rng := rand.New(rand.NewSource(99))
		for _, ref := range []chunks.HeadSeriesRef{50, 10, 90, 30, 70, 20, 100, 40, 80, 60, 5, 95, 15, 85, 25} {
			h := rng.Uint64()
			s.add(ref, h)
			refHashes[storage.SeriesRef(ref)] = h
		}
		deleted := map[storage.SeriesRef]struct{}{10: {}, 70: {}, 95: {}, 25: {}}
		s.remove(deleted)
		for ref := range deleted {
			delete(refHashes, ref)
		}

		const shardCount = uint64(6) // Does not divide 64 => sub-filter reads hashes.
		seen := map[storage.SeriesRef]struct{}{}
		for shardIndex := range shardCount {
			got := expandShard(t, s, shardIndex, shardCount)
			require.True(t, slices.IsSorted(got))
			for _, ref := range got {
				require.Equal(t, shardIndex, refHashes[ref]%shardCount, "ref %d returned for the wrong shard", ref)
				seen[ref] = struct{}{}
			}
		}
		require.Len(t, seen, len(refHashes))
	})

	t.Run("nil means disabled", func(t *testing.T) {
		t.Parallel()
		var s *shardBucketPostings

		lists, subFiltered := s.postingsFor(0, 4)
		require.Nil(t, lists)
		require.False(t, subFiltered)
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
		// Readers verify every captured shard list is sorted, across an exact
		// (4 divides 8) and a sub-filtered (6 does not) shard count, so the
		// sub-filter's concurrent (ref, hash) header capture is race-checked.
		for range 2 {
			wg.Go(func() {
				for range 200 {
					for _, shardCount := range []uint64{4, 6} {
						for shardIndex := range shardCount {
							lists, _ := s.postingsFor(shardIndex, shardCount)
							refs, err := index.ExpandPostings(index.Merge(t.Context(), lists...))
							if err != nil {
								panic(err)
							}
							if !slices.IsSorted(refs) {
								panic("shard list not sorted")
							}
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
			lists, _ := s.postingsFor(shardIndex, 4)
			got, err := index.ExpandPostings(newShardFilterPostings(treeInput(), index.Merge(t.Context(), lists...)))
			require.NoError(t, err)

			lists, _ = s.postingsFor(shardIndex, 4)
			want, err := index.ExpandPostings(index.Intersect(treeInput(), index.Merge(t.Context(), lists...)))
			require.NoError(t, err)
			require.Equal(t, want, got, "shard %d", shardIndex)
		}
	})

	t.Run("shard hash filter yields and seeks matching refs", func(t *testing.T) {
		t.Parallel()
		// refs sorted, hashes aligned. shard 1 of 3 keeps hash%3 == 1.
		refs := []storage.SeriesRef{2, 4, 6, 8, 10, 12}
		hashes := []uint64{1, 4, 2, 7, 10, 3} // %3: 1,1,2,1,1,0 => keep refs 2,4,8,10.

		got, err := index.ExpandPostings(newShardHashFilterPostings(refs, hashes, 1, 3))
		require.NoError(t, err)
		require.Equal(t, []storage.SeriesRef{2, 4, 8, 10}, got)

		f := newShardHashFilterPostings(refs, hashes, 1, 3)
		require.True(t, f.Seek(0)) // Before any Next: position at first match.
		require.Equal(t, storage.SeriesRef(2), f.At())
		require.True(t, f.Seek(5)) // Skip non-matching 6; first match >= 5 is 8.
		require.Equal(t, storage.SeriesRef(8), f.At())
		require.True(t, f.Seek(8)) // Already at or past v: no movement.
		require.Equal(t, storage.SeriesRef(8), f.At())
		require.False(t, f.Seek(11)) // 12 is not in the shard: exhausted.
		require.NoError(t, f.Err())
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

// BenchmarkShardBucketPostingsFootprint measures the resident size of the index
// relative to the live series it holds, after churn — new series created and an
// equal number removed, as the head turns over between GCs. It reports the slice
// capacity (refs plus the aligned hashes) as bytes per live series and the
// cap/len ratio, so a regression in the index's memory amplification is visible:
// cap/len above ~1 means remove retained the dropped refs' capacity.
func BenchmarkShardBucketPostingsFootprint(b *testing.B) {
	const live = 100_000
	for _, churn := range []int{0, 1, 2} {
		b.Run(fmt.Sprintf("churn=%dx", churn), func(b *testing.B) {
			var capEntries, entries int
			for b.Loop() {
				s := newShardBucketPostings(DefaultShardedPostingsBuckets)
				rng := rand.New(rand.NewSource(1))
				total := (1 + churn) * live
				for ref := 1; ref <= total; ref++ {
					s.add(chunks.HeadSeriesRef(ref), rng.Uint64())
				}
				// Remove churn*live of them, leaving `live` live.
				deleted := make(map[storage.SeriesRef]struct{}, churn*live)
				for ref := 1; ref <= churn*live; ref++ {
					deleted[storage.SeriesRef(ref)] = struct{}{}
				}
				s.remove(deleted)

				capEntries, entries = 0, s.numSeries()
				for _, bk := range s.buckets {
					capEntries += cap(bk)
				}
			}
			// Each entry costs a storage.SeriesRef (8B) plus an aligned shard hash (8B).
			b.ReportMetric(float64(capEntries)*16/float64(live), "capbytes/live")
			b.ReportMetric(float64(capEntries)/float64(entries), "cap/len")
		})
	}
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

	// shardPostings is the read path: gather one list per candidate bucket
	// (whole buckets when the shard count divides the bucket count, else
	// hash-sub-filtered), then drain the shard. populate leaves buckets sorted,
	// so this measures the steady-state read, not the one-off sort. Non-divisor
	// counts (96) exercise the sub-filter; 128 is the exact single-bucket case.
	b.Run("shardPostings", func(b *testing.B) {
		const numSeries = 1_000_000
		for _, shardCount := range []uint64{16, 64, 96, 128} {
			b.Run(fmt.Sprintf("shardCount=%d", shardCount), func(b *testing.B) {
				s := populate(numSeries)
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					lists, _ := s.postingsFor(uint64(i)%shardCount, shardCount)
					if _, err := index.ExpandPostings(index.Merge(b.Context(), lists...)); err != nil {
						b.Fatal(err)
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
