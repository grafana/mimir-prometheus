package tsdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func TestDBAppenderSeriesWithMetadata(t *testing.T) {
	db := openTestDB(t, nil, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// TODO(jesusvazquez) Add a test to make sure metadata changes between timestamps.
	ctx := context.Background()
	testCases := []struct {
		name        string
		lbsStrings  [][]string
		lbsMatchers []*labels.Matcher
		result      map[string][]chunks.Sample
		activate    bool
	}{
		{
			name: "query series written with metadata, should only return series labels",
			lbsStrings: [][]string{
				{"__name__", "http_requests_total", "job", "foo", "__metadata__foo__service", "foo", "__metadata__node__ip", "192.168.1.1"},
				{"__name__", "http_requests_total", "job", "foo", "__metadata__foo__service", "foo", "__metadata__node__ip", "192.168.1.2"},
			},
			lbsMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total")},
			// TODO(jesusvazquez) There should be a single series http_requests_total{job="ingester"} with two samples since the metadata does not form part of the series.
			result: map[string][]chunks.Sample{
				labels.FromStrings(
					"__name__", "http_requests_total",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
					sample{t: 1, f: 1},
				},
			},
			activate: false,
		},
		{
			// TODO(jesusvazquez) Add a test to verify that the metadata is stored in the metadata store.
			name: "query metadata labels, should return all series matching the metadata",
			lbsStrings: [][]string{
				{"__name__", "http_requests_total", "job", "foo", "__metadata__foo__service", "foo", "__metadata__node__ip", "192.168.1.1"},
				{"__name__", "http_requests_dropped_total", "job", "foo", "__metadata__foo__service", "foo", "__metadata__node__ip", "192.168.1.1"},
				{"__name__", "http_requests_total", "job", "foo", "__metadata__foo__service", "foo", "__metadata__node__ip", "192.168.1.2"},
				{"__name__", "http_requests_total", "job", "bar", "__metadata__foo__service", "foo", "__metadata__node__ip", "192.168.1.1"},
			},
			lbsMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__metadata__node__ip", "192.168.1.1"),
				labels.MustNewMatcher(labels.MatchEqual, "job", "foo"),
			},
			result: map[string][]chunks.Sample{
				labels.FromStrings(
					"__name__", "http_requests_total",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
				},
				labels.FromStrings(
					"__name__", "http_requests_dropped_total",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
				},
			},
			activate: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			app1 := db.Appender(ctx)

			// Add a sample with metadata.
			for _, lbs := range tc.lbsStrings {
				_, err := app1.Append(0, labels.FromStrings(lbs...), 0, 0)
				require.NoError(t, err)
			}

			err := app1.Commit()
			require.NoError(t, err)

			q, err := db.Querier(0, 200)
			require.NoError(t, err)

			res := query(t, q, tc.lbsMatchers...)
			if tc.activate {
				require.Equal(t, tc.result, res)
			}
		})
	}
}
