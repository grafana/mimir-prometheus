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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

const newSeriesWALLogFailureWarnMsg = "failed to persist newly created series' declaration to the WAL, on both the original commit and the rollback retry"

// TestHeadAppender_Commit_LogsWhenNewSeriesDeclarationFailsOnBothCommitAndRollback demonstrates the
// new diagnostic added to headAppenderBase.Commit(): when a.log() fails for a commit that created
// at least one brand-new series, and Rollback()'s own retry of a.log() also fails, the series now
// lives permanently in head memory (getOrCreate already inserted it before any WAL write was
// attempted) with no declaration ever durably logged -- a possible source of "Unknown series
// references" on a future replay, and of outright data loss for any of its samples that never make
// it into a durable block either. This must be logged with the affected ref(s) and both errors; it
// must NOT be logged for an ordinary commit failure that doesn't involve a brand-new series.
func TestHeadAppender_Commit_LogsWhenNewSeriesDeclarationFailsOnBothCommitAndRollback(t *testing.T) {
	t.Run("logs: commit creates a new series and the WAL is already closed (both attempts fail)", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32*1024, compression.None)
		require.NoError(t, err)
		require.NoError(t, wal.Close()) // every subsequent wal.Log() call will fail identically.

		var logs bytes.Buffer
		opts := newTestHeadDefaultOptions(1000, false)
		opts.ChunkDirRoot = dir
		h, err := NewHead(nil, slog.New(slog.NewTextHandler(&logs, nil)), wal, nil, opts, nil)
		require.NoError(t, err)
		defer func() { _ = h.Close() }()
		h.initTime(0)

		app := h.Appender(context.Background())
		ref, err := app.Append(0, labels.FromStrings("name", "brand_new_series"), 100, 1.0)
		require.NoError(t, err)

		err = app.Commit()
		require.Error(t, err, "commit must fail: the WAL is closed")

		require.Contains(t, logs.String(), newSeriesWALLogFailureWarnMsg)
		require.Contains(t, logs.String(), "commit_err=")
		require.Contains(t, logs.String(), "rollback_err=")
		require.Contains(t, logs.String(), "file already closed", "both the commit_err and rollback_err fields should surface the real WAL error")
		require.NotNil(t, h.series.getByID(chunks.HeadSeriesRef(ref)), "the series must still be live in memory despite the WAL write failing -- that's exactly the hazard being logged")
	})

	t.Run("does not log: commit fails for an already-existing series (no new declaration at stake)", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32*1024, compression.None)
		require.NoError(t, err)

		var logs bytes.Buffer
		opts := newTestHeadDefaultOptions(1000, false)
		opts.ChunkDirRoot = dir
		h, err := NewHead(nil, slog.New(slog.NewTextHandler(&logs, nil)), wal, nil, opts, nil)
		require.NoError(t, err)
		defer func() { _ = h.Close() }()
		h.initTime(0)

		// First commit succeeds normally: the series gets a durable declaration, same as any
		// ordinary series.
		app := h.Appender(context.Background())
		lbls := labels.FromStrings("name", "already_existing_series")
		_, err = app.Append(0, lbls, 100, 1.0)
		require.NoError(t, err)
		require.NoError(t, app.Commit())

		// Now close the WAL and append a second sample for the SAME series: getOrCreate finds it
		// already live (created=false), so no declaration is queued at all -- this commit's
		// failure has nothing to do with the hazard being logged.
		require.NoError(t, wal.Close())
		app = h.Appender(context.Background())
		_, err = app.Append(0, lbls, 200, 2.0)
		require.NoError(t, err)
		err = app.Commit()
		require.Error(t, err, "commit must still fail: the WAL is closed")

		require.NotContains(t, logs.String(), newSeriesWALLogFailureWarnMsg)
	})

	t.Run("does not log: the series record was written and only a later record failed", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		// A 2-page segment: the series record (about 40KB) fits once but not twice, and the
		// samples record doesn't fit in what's left.
		wal, err := wlog.NewSize(nil, nil, walDir, 2*32*1024, compression.None)
		require.NoError(t, err)

		var logs bytes.Buffer
		opts := newTestHeadDefaultOptions(1000, false)
		opts.ChunkDirRoot = dir
		h, err := NewHead(nil, slog.New(slog.NewTextHandler(&logs, nil)), wal, nil, opts, nil)
		require.NoError(t, err)
		defer func() { _ = h.Close() }()
		h.initTime(0)

		app := h.Appender(context.Background())
		for i := range 1500 {
			lbls := labels.FromStrings("name", fmt.Sprintf("series_%d", i))
			for ts := range int64(5) {
				_, err = app.Append(0, lbls, ts, float64(ts))
				require.NoError(t, err)
			}
		}
		// Creating a new segment now fails: the samples record fails on the commit, and the
		// series record fails on the rollback retry.
		require.NoError(t, os.RemoveAll(walDir))

		err = app.Commit()
		require.Error(t, err)
		require.ErrorContains(t, err, "log samples")
		require.NotContains(t, logs.String(), newSeriesWALLogFailureWarnMsg)
	})
}
