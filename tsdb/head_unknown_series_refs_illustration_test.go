// SPDX-License-Identifier: AGPL-3.0-only

package tsdb

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
)

// TestUnknownSeriesRefs_EarlyCompactionOrphansWALSamples reproduces the production
// "Unknown series references" incident (ingester-zone-a-3), using the real timestamps.
//
// Timeline reproduced (values from the capture):
//
//	Event                         value            ≈ time
//	A (evicted ~05:31) last sample 1786253120000    05:25   -> walExpiry(A)
//	B (evicted ~06:30) last sample 1786256920000    06:28   -> walExpiry(B)
//	C (evicted ~07:00) last sample 1786258500000    06:55   -> walExpiry(C)
//	checkpoint mint                1786255200000    06:00
//
// The bug: the 06:00 checkpoint drops a series' RECORD by *time*
// (walExpiry < mint) but retains the newest ~1/3 of WAL segments by *count*. So batch A —
// whose last sample (05:25) is below the mint (06:00) — has its record dropped, while a copy
// of its sample lingers in a retained segment and orphans on the next replay. Batches B and C
// are spared because their expiries (06:28 / 06:55) are >= the mint, so their records are kept.
//
// It also shows the no-data-loss property: A's data is still queryable from the block that
// CompactSelectedSeries wrote before evicting it.
func TestUnknownSeriesRefs_EarlyCompactionOrphansWALSamples(t *testing.T) {
	const (
		headMin = int64(1786248000000) // ~04:00 — oldest sample in the head
		aLast   = int64(1786253120000) // ~05:25 — batch A last sample (== orphan_max_t)
		bLast   = int64(1786256920000) // ~06:28 — batch B last sample
		cLast   = int64(1786258500000) // ~06:55 — batch C last sample
		cpMint  = int64(1786255200000) // ~06:00 — the checkpoint mint
	)

	dir := t.TempDir()

	opts := DefaultOptions()
	// One large chunk range so all data lands in a single range and CompactSelectedSeries
	// writes exactly one block per evicted batch.
	opts.MinBlockDuration = 24 * 60 * 60 * 1000 // 24h
	opts.MaxBlockDuration = 24 * 60 * 60 * 1000
	opts.WALSegmentSize = 32 * 1024
	opts, rngs, err := validateOpts(opts, nil)
	require.NoError(t, err)

	db, err := open(dir, nil, nil, opts, rngs, nil)
	require.NoError(t, err)
	db.DisableCompactions() // We drive compaction/checkpointing manually.

	aLbls := labels.FromStrings("series", "A_evicted_0531")
	bLbls := labels.FromStrings("series", "B_evicted_0630")
	cLbls := labels.FromStrings("series", "C_evicted_0700")
	kLbls := labels.FromStrings("series", "K_keeper")

	appendSample := func(ref storage.SeriesRef, lbls labels.Labels, ts int64, v float64) storage.SeriesRef {
		app := db.Appender(context.Background())
		r, err := app.Append(ref, lbls, ts, v)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		return r
	}
	nextSegment := func() {
		_, err := db.head.wal.NextSegment()
		require.NoError(t, err)
	}

	// Segment 0: all four series' records + A's early history, all in one segment that the
	// checkpoint will consolidate (so A's record can be dropped, and none of A's history
	// leaks into a retained segment).
	aRef := appendSample(0, aLbls, headMin, 1)
	aRef = appendSample(aRef, aLbls, headMin+1000, 1)
	aRef = appendSample(aRef, aLbls, headMin+2000, 1)
	bRef := appendSample(0, bLbls, headMin, 1)
	cRef := appendSample(0, cLbls, headMin, 1)
	kRef := appendSample(0, kLbls, headMin, 1) // keeper: never evicted, keeps the head non-empty

	// Segments 1..6: keeper filler (one small sample each) to build up the segment count, so
	// that segment 0 lands in the checkpointed lower-2/3 while A's last sample below lands in
	// the retained upper-1/3.
	for i := int64(1); i <= 6; i++ {
		nextSegment()
		kRef = appendSample(kRef, kLbls, headMin+i*1000, float64(i))
	}

	// Segment 7 (RETAINED): A's last sample at 05:25 — the only A sample in a retained segment,
	// and the copy that outlives A's record and orphans.
	nextSegment()
	aRef = appendSample(aRef, aLbls, aLast, 2)
	// Evict A. Head max == aLast (05:25), so walExpiry[A] = 05:25 (below the 06:00 mint).
	require.NoError(t, db.CompactSelectedSeries([]storage.SeriesRef{aRef}))

	// Segment 8 (RETAINED): B's last sample (06:28), then evict B. walExpiry[B] = 06:28 (>= mint).
	nextSegment()
	bRef = appendSample(bRef, bLbls, bLast, 2)
	require.NoError(t, db.CompactSelectedSeries([]storage.SeriesRef{bRef}))

	// Segment 9 (RETAINED): C's last sample (06:55), then evict C. walExpiry[C] = 06:55 (>= mint).
	nextSegment()
	cRef = appendSample(cRef, cLbls, cLast, 2)
	require.NoError(t, db.CompactSelectedSeries([]storage.SeriesRef{cRef}))

	// The armed expiries reproduce the table: only A's is below the checkpoint mint.
	readExpiry := func(ref storage.SeriesRef) (int64, bool) {
		db.head.walExpiriesMtx.Lock()
		defer db.head.walExpiriesMtx.Unlock()
		v, ok := db.head.walExpiries[chunks.HeadSeriesRef(ref)]
		return v, ok
	}
	ae, aok := readExpiry(aRef)
	be, bok := readExpiry(bRef)
	ce, cok := readExpiry(cRef)
	require.True(t, aok)
	require.True(t, bok)
	require.True(t, cok)
	require.Equal(t, aLast, ae, "walExpiry(A) == 05:25")
	require.Equal(t, bLast, be, "walExpiry(B) == 06:28")
	require.Equal(t, cLast, ce, "walExpiry(C) == 06:55")
	require.Less(t, ae, cpMint, "A's expiry is below the checkpoint mint -> record dropped")
	require.GreaterOrEqual(t, be, cpMint, "B's expiry is at/above the checkpoint mint -> record kept")
	require.GreaterOrEqual(t, ce, cpMint, "C's expiry is at/above the checkpoint mint -> record kept")

	// The checkpoint: mint = 06:00. Drops A's record; keeps B and C; retains segments 6..8.
	require.NoError(t, db.head.truncateWAL(cpMint))
	require.NoError(t, db.Close())

	// --- Reopen: real WAL replay ---
	reg := prometheus.NewRegistry()
	db2, err := open(dir, nil, reg, opts, rngs, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()

	// Exactly A's retained sample orphans. B and C do NOT, because their records were kept
	// by their still-valid expiries.
	orphanSamples := prom_testutil.ToFloat64(db2.head.metrics.walReplayUnknownRefsTotal.WithLabelValues("samples"))
	require.Equal(t, float64(1), orphanSamples, "only batch A should orphan; B and C must be spared")

	// No data loss: every evicted series is still fully queryable from its block.
	for _, name := range []string{"A_evicted_0531", "B_evicted_0630", "C_evicted_0700"} {
		q, err := db2.Querier(headMin, cLast+1)
		require.NoError(t, err)
		got := query(t, q, labels.MustNewMatcher(labels.MatchEqual, "series", name))
		require.NotEmpty(t, got, "series %s must remain queryable from its block despite the orphaned WAL sample", name)
	}
}
