package tsdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
)

// TestOOOMmapRefRegressionAfterRestart reproduces a Head GC bug.
//
// ChunkDiskMapper.Truncate resets the head chunk file sequence when it deletes
// every chunk file. Head.minOOOMmapRef is in memory and is not reset with it.
// After a restart, already-compacted OOO chunks are reloaded from the old chunk
// files and the next OOO compaction stores one of those refs as minOOOMmapRef.
// If that compaction then deletes every chunk file, later OOO chunks are written
// into a new 000001 and compare less than or equal to minOOOMmapRef, so
// truncateChunksBefore drops them with no log or metric.
//
// The post-restart compaction sequence (forced CompactHead over whatever is left
// in the head, then an unconditional CompactOOOHead) mirrors Mimir's
// userTSDB.compactHead, which runs on both the idle and forced compaction paths
// and is the only caller that reaches CompactOOOHead regardless of whether the
// in-order compaction did anything. Prometheus's own db.Compact only reaches
// CompactOOOHead if that same call also compacted an in-order range.
//
// restart=false keeps the original file sequence and returns all 33 samples.
// restart=true returns 0. The final assertion is the bug, not the expected fix.
func TestOOOMmapRefRegressionAfterRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			runOOOMmapRefRegression(t, restart)
		})
	}
}

func runOOOMmapRefRegression(t *testing.T, restart bool) {
	ctx := context.Background()
	dir := t.TempDir()
	minute := time.Minute.Milliseconds()

	opts := DefaultOptions()
	opts.OutOfOrderCapMax = 32
	opts.OutOfOrderTimeWindow = 600 * minute

	appendRange := func(db *DB, lbls labels.Labels, fromMin, toMin int64) {
		app := db.Appender(ctx)
		for m := fromMin; m <= toMin; m++ {
			_, err := app.Append(0, lbls, m*minute, float64(m))
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}

	target := labels.FromStrings("foo", "target")
	fillers := make([]labels.Labels, 200)
	for i := range fillers {
		fillers[i] = labels.FromStrings("foo", fmt.Sprintf("filler-%d", i))
	}

	// First process: in-order data, OOO data for many series, then a compaction
	// that m-maps the OOO head chunks into a head chunk file and GCs them.
	db := newTestDB(t, withDir(dir), withOpts(opts))
	db.DisableCompactions()
	for _, l := range append(fillers, target) {
		appendRange(db, l, 250, 350)
	}
	for _, l := range fillers {
		appendRange(db, l, 90, 110)
	}
	for _, l := range append(fillers, target) {
		appendRange(db, l, 400, 450)
	}
	require.NoError(t, db.Compact(ctx))
	t.Logf("first process minOOOMmapRef=%v", chunks.ChunkDiskMapperRef(db.head.minOOOMmapRef.Load()))
	if restart {
		require.NoError(t, db.Close())
		// Second process: the already-compacted OOO chunks are reloaded from the
		// head chunk file, and the WBL is empty.
		db = newTestDB(t, withDir(dir), withOpts(opts))
		db.DisableCompactions()

		// Mimir's forced/idle compaction path (userTSDB.compactHead) always runs an
		// in-order range compaction over whatever is left in the head, then
		// unconditionally runs CompactOOOHead, regardless of whether that in-order
		// compaction did anything. Reproduce that exact sequence instead of calling
		// CompactOOOHead on its own, which Mimir never does.
		h := db.Head()
		if h.NumSeries() > 0 {
			require.NoError(t, db.CompactHead(NewRangeHead(h, h.MinTime(), h.MaxTime())))
		}
	}
	require.NoError(t, db.CompactOOOHead(ctx))
	minRef := chunks.ChunkDiskMapperRef(db.head.minOOOMmapRef.Load())
	t.Logf("after reload + OOO compaction minOOOMmapRef=%v", minRef)

	// 33 OOO samples: the 33rd cuts and m-maps a chunk holding the first 32.
	appendRange(db, target, 150, 182)
	ms := db.head.series.getByHash(target.Hash(), target)
	require.NotNil(t, ms)
	require.Len(t, ms.ooo.oooMmappedChunks, 1)
	newRef := ms.ooo.oooMmappedChunks[0].ref
	t.Logf("new OOO chunk ref=%v (greater than minOOOMmapRef: %v)", newRef, newRef.GreaterThan(minRef))

	// In-order compaction runs Head GC (truncateMemory) before the OOO compaction.
	before := prom_testutil.ToFloat64(db.head.metrics.outOfOrderSamplesAppended.WithLabelValues(sampleMetricTypeFloat))
	t.Logf("before 600..700: head maxt=%d minValidTime=%d", db.head.MaxTime()/minute, db.head.minValidTime.Load()/minute)
	for _, l := range append(fillers, target) {
		appendRange(db, l, 600, 700)
	}
	after := prom_testutil.ToFloat64(db.head.metrics.outOfOrderSamplesAppended.WithLabelValues(sampleMetricTypeFloat))
	t.Logf("OOO samples appended during 600..700: %v", after-before)
	require.NoError(t, db.Compact(ctx))

	q, err := db.Querier(150*minute, 182*minute)
	require.NoError(t, err)
	res := query(t, q, labels.MustNewMatcher(labels.MatchEqual, "foo", "target"))
	got := len(res[target.String()])
	for _, b := range db.Blocks() {
		t.Logf("block %d-%d min samples=%d", b.MinTime()/minute, b.MaxTime()/minute, b.Meta().Stats.NumSamples)
	}
	if s := res[target.String()]; len(s) > 0 {
		t.Logf("first=%d last=%d", s[0].T()/minute, s[len(s)-1].T()/minute)
	}
	q2, err := db.Querier(600*minute, 700*minute)
	require.NoError(t, err)
	total := 0
	shapes := map[string]int{}
	for _, s := range query(t, q2, labels.MustNewMatcher(labels.MatchRegexp, "foo", ".+")) {
		total += len(s)
		if len(s) > 0 {
			shapes[fmt.Sprintf("n=%d first=%d last=%d", len(s), s[0].T()/minute, s[len(s)-1].T()/minute)]++
		}
	}
	t.Logf("series shapes 600..700: %v", shapes)
	t.Logf("in-order samples 600..700 across all series: %d (want %d), head mint=%d", total, 101*201, db.head.MinTime()/minute)
	t.Logf("samples returned for 150..182: %d (want 33)", got)
	require.Equal(t, 33, got)
}
