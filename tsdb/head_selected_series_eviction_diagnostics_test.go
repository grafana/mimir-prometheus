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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

const selectedSeriesEvictionBreakdownMsg = "selected series eviction: requested vs tombstoned breakdown"

// TestHead_TruncateSeries_LogsRequestedVsTombstonedBreakdown demonstrates the new diagnostic
// added to truncateSeries/gcSeries: when a CompactSelectedSeries-style eviction call doesn't turn
// every requested ref into a written tombstone, the gap is now attributed to a specific cause
// (declined by the caller's shouldEvict predicate, or retained chunk-less because of a recent
// exemplar) instead of only being visible as a silent discrepancy between the ingester's own
// before/after series counts. The log must stay silent when the whole batch is fully evicted.
func TestHead_TruncateSeries_LogsRequestedVsTombstonedBreakdown(t *testing.T) {
	newHead := func(t *testing.T, logs *bytes.Buffer) *Head {
		t.Helper()
		dir := t.TempDir()
		wal, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32*1024, compression.None)
		require.NoError(t, err)
		opts := newTestHeadDefaultOptions(1000, false)
		opts.ChunkDirRoot = dir
		h, err := NewHead(nil, slog.New(slog.NewTextHandler(logs, nil)), wal, nil, opts, nil)
		require.NoError(t, err)
		h.initTime(0)
		return h
	}

	appendSample := func(t *testing.T, h *Head, name string, ts int64, v float64) storage.SeriesRef {
		t.Helper()
		app := h.Appender(context.Background())
		ref, err := app.Append(0, labels.FromStrings("__name__", name), ts, v)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		return ref
	}

	t.Run("mixed batch: logs the breakdown with each ref's reason", func(t *testing.T) {
		var logs bytes.Buffer
		h := newHead(t, &logs)
		defer func() { _ = h.Close() }()

		evictable := appendSample(t, h, "evictable", 100, 1)
		declined := appendSample(t, h, "declined_by_caller", 100, 2)
		withExemplar := appendSample(t, h, "has_recent_exemplar", 100, 3)

		app := h.Appender(context.Background())
		_, err := app.AppendExemplar(withExemplar, labels.FromStrings("__name__", "has_recent_exemplar"),
			exemplar.Exemplar{Ts: 100, Value: 3, Labels: labels.FromStrings("trace_id", "abc")})
		require.NoError(t, err)
		require.NoError(t, app.Commit())

		refs := []storage.SeriesRef{evictable, declined, withExemplar}
		n, err := h.truncateSeries(refs, 100, func(s *memSeries) bool {
			return s.ref != chunks.HeadSeriesRef(declined)
		})
		require.NoError(t, err)
		require.Equal(t, 1, n, "only the plain evictable series should actually be deleted")

		require.Nil(t, h.series.getByID(chunks.HeadSeriesRef(evictable)), "evictable series must be gone")
		require.NotNil(t, h.series.getByID(chunks.HeadSeriesRef(declined)), "declined series must still be live")
		require.NotNil(t, h.series.getByID(chunks.HeadSeriesRef(withExemplar)), "exemplar-retained series must still be live")

		logStr := logs.String()
		require.Contains(t, logStr, selectedSeriesEvictionBreakdownMsg)
		require.Contains(t, logStr, "requested=3")
		require.Contains(t, logStr, "tombstoned=1")
		require.Contains(t, logStr, "should_evict_declined=1")
		require.Contains(t, logStr, "exemplar_retained=1")
		// The sample lists should name the actual ref, not just a count.
		require.Contains(t, logStr, fmt.Sprintf("%d", declined), "log should include the declined ref number: %s", logStr)
	})

	t.Run("fully evicted batch: stays silent", func(t *testing.T) {
		var logs bytes.Buffer
		h := newHead(t, &logs)
		defer func() { _ = h.Close() }()

		ref := appendSample(t, h, "evictable", 100, 1)

		n, err := h.truncateSeries([]storage.SeriesRef{ref}, 100, func(*memSeries) bool { return true })
		require.NoError(t, err)
		require.Equal(t, 1, n)

		require.NotContains(t, logs.String(), selectedSeriesEvictionBreakdownMsg,
			"must not log anything extra when the whole requested batch was successfully tombstoned")
	})
}
