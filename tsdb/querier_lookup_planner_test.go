// Copyright 2025 The Prometheus Authors
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
	"context"
	"errors"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/annotations"
)

// mockLookupPlanner is a controllable index lookup planner for testing.
type mockLookupPlanner struct {
	returnError   bool
	indexMatchers []*labels.Matcher
	scanMatchers  []*labels.Matcher
	errorToReturn error
}

func (m *mockLookupPlanner) PlanIndexLookup(_ context.Context, _ index.LookupPlan, _ *storage.SelectHints) (index.LookupPlan, error) {
	if m.returnError {
		if m.errorToReturn != nil {
			return nil, m.errorToReturn
		}
		return nil, errors.New("mock planner error")
	}

	return &ConcreteLookupPlan{
		indexMatchers: m.indexMatchers,
		scanMatchers:  m.scanMatchers,
	}, nil
}

// mockIndexWithPlanner wraps mockIndex and injects a controllable planner.
type mockIndexWithPlanner struct {
	mockIndex
	planner index.LookupPlanner
}

func (m *mockIndexWithPlanner) IndexLookupPlanner() index.LookupPlanner {
	return m.planner
}

// ConcreteLookupPlan is a simple implementation of LookupPlan for testing.
// Note: We can't use the unexported concreteLookupPlan from index package,
// so we need our own implementation.
type ConcreteLookupPlan struct {
	indexMatchers []*labels.Matcher
	scanMatchers  []*labels.Matcher
}

func (p *ConcreteLookupPlan) IndexMatchers() []*labels.Matcher {
	return p.indexMatchers
}

func (p *ConcreteLookupPlan) ScanMatchers() []*labels.Matcher {
	return p.scanMatchers
}

// mockBlockReader implements BlockReader for testing with controllable components.
type mockBlockReader struct {
	index      IndexReader
	chunks     ChunkReader
	tombstones tombstones.Reader
	meta       BlockMeta
}

func (m *mockBlockReader) Index() (IndexReader, error) {
	return m.index, nil
}

func (m *mockBlockReader) Chunks() (ChunkReader, error) {
	return m.chunks, nil
}

func (m *mockBlockReader) Tombstones() (tombstones.Reader, error) {
	return m.tombstones, nil
}

func (m *mockBlockReader) Meta() BlockMeta {
	return m.meta
}

func (*mockBlockReader) Size() int64 {
	return 1024 // dummy size
}

// seriesSetWithLabels is an interface that both storage.SeriesSet and storage.ChunkSeriesSet satisfy
// for accessing series labels. This allows us to write generic test functions.
type seriesSetWithLabels[T storage.Labels] interface {
	Next() bool
	Err() error
	Warnings() annotations.Annotations
	At() T
}

// createTestIndex creates a properly initialized mockIndex for testing planner behavior.
func createTestIndex() mockIndex {
	idx := newMockIndex()

	// Create dummy chunk metadata
	dummyChunk := chunks.Meta{MinTime: 0, MaxTime: 100, Ref: 1}

	// Add test series with different label combinations and dummy chunks
	idx.AddSeries(1, labels.FromStrings("__name__", "metric1", "job", "test", "instance", "a"), dummyChunk)
	idx.AddSeries(2, labels.FromStrings("__name__", "metric1", "job", "test", "instance", "b"), dummyChunk)
	idx.AddSeries(3, labels.FromStrings("__name__", "metric2", "job", "prod", "instance", "a"), dummyChunk)
	idx.AddSeries(4, labels.FromStrings("__name__", "metric2", "job", "prod", "instance", "b"), dummyChunk)
	idx.AddSeries(5, labels.FromStrings("__name__", "metric3", "job", "", "instance", "a"), dummyChunk) // Empty job value for scan matcher testing

	return idx
}

// createMockBlockReader creates a mock BlockReader with the given planner.
func createMockBlockReader(planner index.LookupPlanner) *mockBlockReader {
	baseIndex := createTestIndex()
	indexWithPlanner := &mockIndexWithPlanner{
		mockIndex: baseIndex,
		planner:   planner,
	}

	chunks := mockChunkReader(make(map[chunks.ChunkRef]chunkenc.Chunk))
	tombstones := tombstones.NewMemTombstones()

	return &mockBlockReader{
		index:      indexWithPlanner,
		chunks:     chunks,
		tombstones: tombstones,
		meta: BlockMeta{
			ULID:    ulid.MustNew(ulid.Now(), nil),
			MinTime: 0,
			MaxTime: 100,
		},
	}
}

type anyQuerier[L storage.Labels, S seriesSetWithLabels[L]] interface {
	Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) S
}

// testQuerierWithIndexLookupPlanner tests chunk querier with index lookup planner.
func testQuerierWithIndexLookupPlanner[L storage.Labels, S seriesSetWithLabels[L], Q anyQuerier[L, S]](t *testing.T, createQuerier func(blockReader *mockBlockReader) Q) {
	t.Run("planner returns error", func(t *testing.T) {
		planner := &mockLookupPlanner{
			returnError:   true,
			errorToReturn: errors.New("test planner error"),
		}
		blockReader := createMockBlockReader(planner)

		querier := createQuerier(blockReader)

		ms := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric1"),
		}

		seriesSet := querier.Select(context.Background(), false, nil, ms...)

		// Regular querier should ignore planner errors and still succeed
		count := 0
		for seriesSet.Next() {
			count++
		}
		require.NoError(t, seriesSet.Err())
		require.Equal(t, 2, count, "should return metric1 series despite planner error")
	})

	t.Run("planner splits matchers - scan matchers filter series", func(t *testing.T) {
		// Index matchers: job="test" (should match series 1,2)
		// Scan matchers: instance="a" (should filter to only series 1)
		planner := &mockLookupPlanner{
			returnError: false,
			indexMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
			},
			scanMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "instance", "a"),
			},
		}
		blockReader := createMockBlockReader(planner)

		querier := createQuerier(blockReader)

		ms := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
			labels.MustNewMatcher(labels.MatchEqual, "instance", "a"),
		}

		seriesSet := querier.Select(context.Background(), false, nil, ms...)

		var actualSeries []labels.Labels
		for seriesSet.Next() {
			actualSeries = append(actualSeries, seriesSet.At().Labels())
		}
		require.NoError(t, seriesSet.Err())

		// Should only return series 1 (metric1, job=test, instance=a)
		require.Len(t, actualSeries, 1)
		require.Equal(t, "metric1", actualSeries[0].Get("__name__"))
		require.Equal(t, "test", actualSeries[0].Get("job"))
		require.Equal(t, "a", actualSeries[0].Get("instance"))
	})

	t.Run("planner returns only index matchers", func(t *testing.T) {
		planner := &mockLookupPlanner{
			returnError: false,
			indexMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric2"),
				labels.MustNewMatcher(labels.MatchEqual, "job", "prod"),
			},
			scanMatchers: nil, // No scan matchers
		}
		blockReader := createMockBlockReader(planner)

		querier := createQuerier(blockReader)

		ms := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric2"),
			labels.MustNewMatcher(labels.MatchEqual, "job", "prod"),
		}

		seriesSet := querier.Select(context.Background(), false, nil, ms...)

		var actualSeries []labels.Labels
		for seriesSet.Next() {
			actualSeries = append(actualSeries, seriesSet.At().Labels())
		}
		require.NoError(t, seriesSet.Err())

		// Should return both metric2 series (3,4)
		require.Len(t, actualSeries, 2)
		for _, s := range actualSeries {
			require.Equal(t, "metric2", s.Get("__name__"))
			require.Equal(t, "prod", s.Get("job"))
		}
	})

	t.Run("empty string scan matcher filters correctly", func(t *testing.T) {
		// Test case where scan matcher filters on empty string values
		planner := &mockLookupPlanner{
			returnError: false,
			indexMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric3"),
			},
			scanMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", ""), // Empty job value
			},
		}
		blockReader := createMockBlockReader(planner)

		querier := createQuerier(blockReader)

		ms := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric3"),
			labels.MustNewMatcher(labels.MatchEqual, "job", ""),
		}

		seriesSet := querier.Select(context.Background(), false, nil, ms...)

		var actualSeries []labels.Labels
		for seriesSet.Next() {
			actualSeries = append(actualSeries, seriesSet.At().Labels())
		}
		require.NoError(t, seriesSet.Err())

		// Should return series 5 (metric3 with empty job)
		require.Len(t, actualSeries, 1)
		require.Equal(t, "metric3", actualSeries[0].Get("__name__"))
		require.Empty(t, actualSeries[0].Get("job"))
		require.Equal(t, "a", actualSeries[0].Get("instance"))
	})
}

// TestSelectSeriesSetWithIndexLookupPlanner tests that the regular querier respects the index lookup planner.
func TestSelectSeriesSetWithIndexLookupPlanner(t *testing.T) {
	createQuerier := func(blockReader *mockBlockReader) storage.Querier {
		querier, err := NewBlockQuerier(blockReader, 0, 100)
		require.NoError(t, err)
		return querier
	}
	testQuerierWithIndexLookupPlanner[storage.Series](t, createQuerier)
}

// TestSelectChunkSeriesSetWithIndexLookupPlanner tests that the chunk querier respects the index lookup planner.
func TestSelectChunkSeriesSetWithIndexLookupPlanner(t *testing.T) {
	createQuerier := func(blockReader *mockBlockReader) storage.ChunkQuerier {
		querier, err := NewBlockChunkQuerier(blockReader, 0, 100)
		require.NoError(t, err)
		return querier
	}
	testQuerierWithIndexLookupPlanner[storage.ChunkSeries](t, createQuerier)
}
