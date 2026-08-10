// SPDX-License-Identifier: AGPL-3.0-only

package tsdb

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
)

// TestUnknownSeriesRefs_EarlyCompactionOrphansWALSamples illustrates the bug behind the
// production "Unknown series references" warnings.
//
// The essence: a WAL checkpoint drops a series' *record* while some of that series'
// *sample* records still live in a retained (never-checkpointed) WAL segment. On the next
// replay those samples reference a series that no longer has a record — they are "orphaned"
// and reported as unknown series references.
//
// Real ingredients used (no mocking of the erroneous behaviour):
//   - CompactSelectedSeries: the exact primitive early compaction of non-owned series uses.
//     It persists the selected series to a block, then evicts them from the head and writes
//     a full-delete tombstone. Crucially it does NOT advance HeadMinTime.
//   - head.truncateWAL(mint): the exact primitive DB.Compact uses to checkpoint+truncate the
//     WAL. Its keep() predicate drops the record of an evicted series once mint passes the
//     series' walExpiry, and its `last--` rule always retains the most recent segment(s) raw.
//   - A real close + reopen to force a real WAL replay.
//
// It also demonstrates the no-data-loss claim: the orphaned samples are redundant — the
// series is still fully queryable from the block CompactSelectedSeries wrote before evicting.
func TestUnknownSeriesRefs_EarlyCompactionOrphansWALSamples(t *testing.T) {
	dir := t.TempDir()

	// Block/chunk range large enough that all our samples land in a single head chunk range,
	// so CompactSelectedSeries writes exactly one block for series A.
	opts := DefaultOptions()
	opts.MinBlockDuration = 100000
	opts.MaxBlockDuration = 100000
	// Small WAL segments so our forced rotations create many segments cheaply.
	opts.WALSegmentSize = 32 * 1024

	opts, rngs, err := validateOpts(opts, nil)
	require.NoError(t, err)

	db, err := open(dir, nil, nil, opts, rngs, nil)
	require.NoError(t, err)
	db.DisableCompactions() // We drive compaction/checkpointing manually.

	aLbls := labels.FromStrings("series", "A_non_owned")
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

	// Segment 0: series A's record + its first sample, plus keeper K's record.
	aRef := appendSample(0, aLbls, 10, 1.0)
	kRef := appendSample(0, kLbls, 1, 1.0)

	// Segments 1..7: keeper filler, one sample per segment, so the eventual checkpoint has
	// enough segments to actually run and so segment 0 (holding A's record) is checkpointed.
	for i := int64(2); i <= 8; i++ {
		nextSegment()
		kRef = appendSample(kRef, kLbls, i, float64(i))
	}

	// Segment 8 (will be RETAINED by the checkpoint's `last--` rule): A's second sample.
	// In-order for A (10 -> 20) and written late, so it sits in a recent, retained segment
	// even though its timestamp is old — this is what makes it outlive A's record.
	nextSegment()
	aRef = appendSample(aRef, aLbls, 20, 2.0)

	require.Equal(t, uint64(2), db.head.NumSeries(), "precondition: A and K in head")
	require.Equal(t, int64(20), db.head.MaxTime())

	// --- Early compaction of the non-owned series A ---
	// Persists A to a block (durably!) then evicts it and writes a full-delete tombstone.
	require.NoError(t, db.CompactSelectedSeries([]storage.SeriesRef{aRef}))
	require.Equal(t, uint64(1), db.head.NumSeries(), "A evicted, K remains")
	require.Len(t, db.Blocks(), 1, "A persisted to exactly one block before eviction")

	// Prove the safeguard IS armed immediately after eviction (before the checkpoint below
	// deletes it as expired).
	db.head.walExpiriesMtx.Lock()
	a, armedBefore := db.head.walExpiries[chunks.HeadSeriesRef(aRef)]
	db.head.walExpiriesMtx.Unlock()
	require.True(t, armedBefore, "walExpiries must be armed for A immediately after eviction")
	fmt.Printf("db.head.walExpiries[chunks.HeadSeriesRef(%d)]=%d\n", aRef, a)

	// --- A checkpoint whose mint is past A's data ---
	// This is what DB.Compact does. mint=1000 > A's walExpiry (=A.maxt=20), so A's record is
	// dropped from the checkpoint; A's second sample survives in the retained last segment.
	require.NoError(t, db.head.truncateWAL(1000))

	require.NoError(t, db.Close())

	// --- Reopen: real WAL replay, capturing DIAGNOSTIC logs and the unknown-refs metric ---
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	reg := prometheus.NewRegistry()

	db2, err := open(dir, logger, reg, opts, rngs, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()

	orphanSamples := prom_testutil.ToFloat64(db2.head.metrics.walReplayUnknownRefsTotal.WithLabelValues("samples"))
	neverSeen := prom_testutil.ToFloat64(db2.head.metrics.walReplayUnknownRefsTotal.WithLabelValues("orphan_never_seen_series_record"))
	byTombstone := prom_testutil.ToFloat64(db2.head.metrics.walReplayUnknownRefsTotal.WithLabelValues("orphan_deleted_by_tombstone"))

	t.Logf("orphaned samples=%v  never_seen_series_record=%v  deleted_by_tombstone=%v", orphanSamples, neverSeen, byTombstone)
	t.Logf("DIAGNOSTIC logs during replay:\n%s", logBuf.String())

	// The bug: A's retained WAL sample is orphaned because A's record was dropped.
	require.Positive(t, orphanSamples, "expected orphaned samples (unknown series references) on replay")
	require.Positive(t, neverSeen, "orphans should be attributed to a genuinely-absent series record (mechanism A)")

	// No data loss: A is still fully queryable from the block written before eviction.
	q, err := db2.Querier(0, 100000)
	require.NoError(t, err)
	got := query(t, q, labels.MustNewMatcher(labels.MatchEqual, "series", "A_non_owned"))
	require.Equal(t, map[string][]chunks.Sample{
		`{series="A_non_owned"}`: {sample{t: 10, f: 1.0}, sample{t: 20, f: 2.0}},
	}, got, "series A must remain fully queryable from the block despite the orphaned WAL samples")
}

// TestUnknownSeriesRefs_RestartLosesWALExpiriesNoExpiryDrop illustrates the "dropped_no_expiry"
// path observed in production, which is reached specifically because of a restart.
//
// walExpiries — the safeguard that keeps an evicted series' record in checkpoints while its
// samples still live in the WAL — is an in-memory map. It does not survive a restart, and it
// is only re-armed during replay by deleteSeriesByID, which runs when a full-delete tombstone
// deletes a series that is still present. Re-arming uses the series' latest sample time, so it
// is SKIPPED when the series has no samples to apply during replay (maxTime == MinInt64).
//
// This test manufactures exactly that "no samples to apply" situation the realistic way:
// before the restart, a checkpoint runs whose mint is above A's samples but below A's (still
// armed) walExpiry. That checkpoint correctly KEEPS A's record but DROPS A's sample records.
// So after the restart the on-disk WAL/checkpoint holds A's record but none of A's samples:
// replay recreates A empty, the tombstone deletes it, no expiry is re-armed, and the next
// checkpoint drops A's record as dropped_no_expiry — purely as a consequence of the restart.
//
// Real ingredients: CompactSelectedSeries (early-compaction primitive, writes the block), a
// real close+reopen (loses walExpiries), and head.truncateWAL (the checkpoint primitive).
func TestUnknownSeriesRefs_RestartLosesWALExpiriesNoExpiryDrop(t *testing.T) {
	dir := t.TempDir()

	// chunkRange 100 so A's samples [10,20] form a single block [0,100).
	opts := DefaultOptions()
	opts.MinBlockDuration = 100
	opts.MaxBlockDuration = 100
	opts.WALSegmentSize = 32 * 1024

	opts, rngs, err := validateOpts(opts, nil)
	require.NoError(t, err)

	db, err := open(dir, nil, nil, opts, rngs, nil)
	require.NoError(t, err)
	db.DisableCompactions() // We drive compaction/checkpointing manually.

	aLbls := labels.FromStrings("series", "A_non_owned")
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

	// Segment 0: A's record + both its samples (low timestamps 10, 20).
	aRef := appendSample(0, aLbls, 10, 1.0)
	aRef = appendSample(aRef, aLbls, 20, 2.0)
	// Keeper K, at rising high timestamps across several segments. K keeps the head alive and,
	// crucially, pushes head.MaxTime up to 900 so A's armed walExpiry (= head.MaxTime) sits far
	// above A's own samples. Enough segments so the checkpoint below actually runs.
	kRef := appendSample(0, kLbls, 100, 1.0)
	for i := int64(2); i <= 9; i++ {
		nextSegment()
		kRef = appendSample(kRef, kLbls, i*100, float64(i))
	}

	// Early compaction: persists A to a block [0,100) (durably), evicts it, arms
	// walExpiries[A] = head.MaxTime() = 900 in memory, and writes a full-delete tombstone.
	require.NoError(t, db.CompactSelectedSeries([]storage.SeriesRef{aRef}))
	require.Equal(t, uint64(1), db.head.NumSeries(), "A evicted, K remains")
	require.Len(t, db.Blocks(), 1)

	// Prove the safeguard IS armed before the restart.
	db.head.walExpiriesMtx.Lock()
	_, armedBefore := db.head.walExpiries[chunks.HeadSeriesRef(aRef)]
	db.head.walExpiriesMtx.Unlock()
	require.True(t, armedBefore, "walExpiries must be armed for A immediately after eviction")

	// A checkpoint with mint between A's samples (<=20) and A's walExpiry (900): it KEEPS A's
	// record (walExpiry 900 >= 50) but DROPS A's sample records (10, 20 < 50). This is the
	// normal, correct behaviour — while the in-memory walExpiry is alive, the record is
	// preserved. The result on disk: a checkpoint holding A's record but none of A's samples.
	require.NoError(t, db.head.truncateWAL(50))

	require.NoError(t, db.Close())

	// --- Restart: walExpiries is in-memory, so it is now empty ---
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	reg := prometheus.NewRegistry()

	db2, err := open(dir, logger, reg, opts, rngs, nil)
	require.NoError(t, err)
	db2.DisableCompactions() // We drive the post-restart checkpoint manually.
	defer func() { require.NoError(t, db2.Close()) }()

	// After replay: walExpiries started empty (in-memory, lost on restart). Replay recreated A
	// from its record in the checkpoint, but the checkpoint held NONE of A's samples, so A had
	// maxTime == MinInt64 when the full-delete tombstone deleted it — deleteSeriesByID therefore
	// armed NO expiry. The safeguard is gone, and replay could not rebuild it.
	db2.head.walExpiriesMtx.Lock()
	_, armedAfter := db2.head.walExpiries[chunks.HeadSeriesRef(aRef)]
	db2.head.walExpiriesMtx.Unlock()
	require.False(t, armedAfter, "restart must have lost A's walExpiry (and replay must not re-arm it)")

	// Weakness #1 is harmless w.r.t. orphans: A came back EMPTY (no samples), so there is
	// nothing to orphan. dropped_no_expiry never produces an "unknown series reference".
	require.Zero(t, prom_testutil.ToFloat64(db2.head.metrics.walReplayUnknownRefsTotal.WithLabelValues("samples")),
		"a dropped_no_expiry series has no lingering samples, so it must not orphan")

	// Now the checkpoint that a startup Compact runs sees A's record (still carried in the prior
	// checkpoint) but A is neither in the head nor expiry-protected, so it drops the record with
	// dropped_no_expiry — exactly the production path.
	require.NoError(t, db2.head.truncateWAL(2000))

	logs := logBuf.String()
	t.Logf("DIAGNOSTIC logs after restart:\n%s", logs)
	require.Contains(t, logs, "DIAGNOSTIC WAL checkpoint keep decisions",
		"expected a checkpoint keep-decisions log")
	require.Contains(t, logs, "dropped_no_expiry=1",
		"expected A's record to be dropped as dropped_no_expiry after the restart lost its walExpiry")
	require.Contains(t, logs, "dropped_expired_expiry=0",
		"the drop must be due to a missing expiry, not an expired one")

	// No data loss: A remains fully queryable from the block written before eviction.
	q, err := db2.Querier(0, 100000)
	require.NoError(t, err)
	got := query(t, q, labels.MustNewMatcher(labels.MatchEqual, "series", "A_non_owned"))
	require.Equal(t, map[string][]chunks.Sample{
		`{series="A_non_owned"}`: {sample{t: 10, f: 1.0}, sample{t: 20, f: 2.0}},
	}, got, "series A must remain fully queryable from the block")

	require.NotEmpty(t, strings.TrimSpace(logs))
}

// TestUnknownSeriesRefs_RestartReArmsExpiry_OrphanIsExpiredNotNoExpiry proves that a restart
// (which wipes the in-memory walExpiries) does NOT turn an orphan into a dropped_no_expiry
// (weakness #1) case: replay RE-ARMS the expiry from the series' still-present sample, so the
// record drop is dropped_expired_expiry (weakness #2) and the orphan surfaces via #2.
//
// The mechanism is the re-arm. A record is dropped as dropped_no_expiry only when the series
// replays "empty": deleteSeriesByID skips re-arming the expiry when maxTime == MinInt64 (no
// samples applied). But any sample that could later orphan is, by definition, still present and
// replayable — so on replay it IS applied, which re-arms the expiry from that sample, forcing
// the expired path. In short: an orphan-able sample always forces dropped_expired_expiry;
// dropped_no_expiry only happens when there is nothing left to orphan (see
// TestUnknownSeriesRefs_RestartLosesWALExpiriesNoExpiryDrop, which asserts zero orphans). So the
// only orphan-producing weakness is #2.
//
// Scope: this concerns orphans caused by a checkpoint dropping the series record (the
// never_seen_series_record kind). A full-delete tombstone that is still present at replay and
// deletes the series mid-replay is a separate path (deleted_by_tombstone_in_replay), not
// covered here.
//
// End to end: evict A mid-life (tombstone + armed expiry), restart (wipes walExpiries), replay
// re-arms A's expiry from its lingering retained sample, a checkpoint drops A's record via
// dropped_expired_expiry, and A's retained sample orphans on the next replay — weakness #2.
func TestUnknownSeriesRefs_RestartReArmsExpiry_OrphanIsExpiredNotNoExpiry(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.MinBlockDuration = 100000
	opts.MaxBlockDuration = 100000
	opts.WALSegmentSize = 32 * 1024
	opts, rngs, err := validateOpts(opts, nil)
	require.NoError(t, err)

	db, err := open(dir, nil, nil, opts, rngs, nil)
	require.NoError(t, err)
	db.DisableCompactions()

	aLbls := labels.FromStrings("series", "A_non_owned")
	kLbls := labels.FromStrings("series", "K_keeper")
	appendSample := func(ref storage.SeriesRef, lbls labels.Labels, ts int64, v float64) storage.SeriesRef {
		app := db.Appender(context.Background())
		r, err := app.Append(ref, lbls, ts, v)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		return r
	}
	nextSegment := func() { _, err := db.head.wal.NextSegment(); require.NoError(t, err) }

	aRef := appendSample(0, aLbls, 10, 1.0)
	kRef := appendSample(0, kLbls, 1, 1.0)
	for i := int64(2); i <= 8; i++ {
		nextSegment()
		kRef = appendSample(kRef, kLbls, i, float64(i))
	}
	nextSegment()
	aRef = appendSample(aRef, aLbls, 20, 2.0) // A's second sample in a retained segment.

	require.NoError(t, db.CompactSelectedSeries([]storage.SeriesRef{aRef}))
	require.NoError(t, db.Close()) // restart -> walExpiries wiped

	// Reopen #1: replay. Does A come back empty (=> no re-arm) or do its samples get applied (=> re-arm)?
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	reg := prometheus.NewRegistry()
	db2, err := open(dir, logger, reg, opts, rngs, nil)
	require.NoError(t, err)
	db2.DisableCompactions()

	// After the restart, walExpiries started empty — but replay APPLIED A's still-present
	// sample and the tombstone then re-armed the expiry from it. So the safeguard is back,
	// and there is no orphan yet (A's record is present, its sample binds to it).
	db2.head.walExpiriesMtx.Lock()
	keepUntil, armedAfter := db2.head.walExpiries[chunks.HeadSeriesRef(aRef)]
	db2.head.walExpiriesMtx.Unlock()
	orphan2 := prom_testutil.ToFloat64(db2.head.metrics.walReplayUnknownRefsTotal.WithLabelValues("samples"))
	require.True(t, armedAfter, "replay must re-arm A's walExpiry from its still-present sample, despite the restart wiping the in-memory map")
	require.Equal(t, int64(20), keepUntil, "re-armed expiry must equal A's max applied sample time")
	require.Zero(t, orphan2, "no orphan while A's record is still present")

	// Drop A's record. Because the expiry was re-armed (=20) and mint=1000 > 20, this is the
	// EXPIRED path (weakness #2), NOT no_expiry (weakness #1).
	require.NoError(t, db2.head.truncateWAL(1000))
	logs := logBuf.String()
	t.Logf("checkpoint keep-decision log:\n%s", logs)
	require.Contains(t, logs, "dropped_expired_expiry=1", "the drop must be via an expired (present) expiry")
	require.Contains(t, logs, "dropped_no_expiry=0", "the drop must NOT be via a missing expiry")
	require.NoError(t, db2.Close())

	// Reopen #2: now A's record is gone; A's retained sample orphans — via the #2 (expired) path.
	reg3 := prometheus.NewRegistry()
	db3, err := open(dir, nil, reg3, opts, rngs, nil)
	require.NoError(t, err)
	db3.DisableCompactions()
	defer func() { require.NoError(t, db3.Close()) }()
	orphan3 := prom_testutil.ToFloat64(db3.head.metrics.walReplayUnknownRefsTotal.WithLabelValues("samples"))
	require.Positive(t, orphan3, "A's retained sample orphans after its record is dropped (via the expired path)")
}
