package tsdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func TestDBAppenderSeriesWithMetadata(t *testing.T) {
	// TODO(jesusvazquez) Add a test to make sure metadata changes between timestamps.
	ctx := context.Background()
	testCases := []struct {
		name        string
		lbsStrings  [][]string
		lbsMatchers []*labels.Matcher
		result      map[string][]chunks.Sample
	}{
		{
			name: "query series written with metadata, should only return series labels",
			lbsStrings: [][]string{
				{"__name__", "http_requests_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1"},
				{"__name__", "http_requests_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.2"},
			},
			lbsMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total")},
			result: map[string][]chunks.Sample{
				labels.FromStrings(
					"__name__", "http_requests_total",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
					sample{t: 1, f: 1},
				},
			},
		},
		{
			name: "query metadata labels, should return all series matching the metadata and the queried metalabels",
			lbsStrings: [][]string{
				{"__name__", "http_requests_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1"},
				{"__name__", "http_requests_dropped_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1"},
				{"__name__", "http_requests_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.2"},
				{"__name__", "http_requests_total", "job", "bar", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1"},
			},
			lbsMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__metalabel__node__ip", "192.168.1.1"),
				labels.MustNewMatcher(labels.MatchEqual, "job", "foo"),
			},
			result: map[string][]chunks.Sample{
				labels.FromStrings(
					"__name__", "http_requests_total",
					"__metalabel__node__ip", "192.168.1.1",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
					sample{t: 2, f: 2},
				},
				labels.FromStrings(
					"__name__", "http_requests_dropped_total",
					"__metalabel__node__ip", "192.168.1.1",
					"job", "foo",
				).String(): {
					sample{t: 1, f: 1},
				},
			},
		},
		{
			name: "query metadata labels with regex",
			lbsStrings: [][]string{
				{"__name__", "http_requests_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1"},
				{"__name__", "http_requests_dropped_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1"},
				{"__name__", "http_requests_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.2"},
				{"__name__", "http_requests_total", "job", "bar", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1"},
			},
			lbsMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__metalabel__node__ip", ".*"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
			},
			result: map[string][]chunks.Sample{
				labels.FromStrings(
					"__name__", "http_requests_total",
					"__metalabel__node__ip", "192.168.1.1",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
					sample{t: 2, f: 2},
				},
				labels.FromStrings(
					"__name__", "http_requests_total",
					"__metalabel__node__ip", "192.168.1.2",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
					sample{t: 2, f: 2},
				},
				labels.FromStrings(
					"__name__", "http_requests_total",
					"__metalabel__node__ip", "192.168.1.1",
					"job", "bar",
				).String(): {
					sample{t: 3, f: 3},
				},
			},
		},
		{
			name: "query metadata labels with multiple metadata matchers",
			lbsStrings: [][]string{
				{"__name__", "http_requests_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1", "__metalabel__foo__type", "type1"},
				{"__name__", "http_requests_dropped_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1", "__metalabel__foo__type", "type2"},
				{"__name__", "http_requests_total", "job", "foo", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.2", "__metalabel__foo__type", "type1"},
				{"__name__", "http_requests_total", "job", "bar", "__metalabel__foo__service", "foo", "__metalabel__node__ip", "192.168.1.1", "__metalabel__foo__type", "type2"},
			},
			lbsMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
				labels.MustNewMatcher(labels.MatchRegexp, "__metalabel__node__ip", ".*"),
				labels.MustNewMatcher(labels.MatchEqual, "__metalabel__foo__type", "type1"),
			},
			result: map[string][]chunks.Sample{
				labels.FromStrings(
					"__name__", "http_requests_total",
					"__metalabel__node__ip", "192.168.1.1",
					"__metalabel__foo__type", "type1",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
					sample{t: 2, f: 2},
				},
				labels.FromStrings(
					"__name__", "http_requests_total",
					"__metalabel__node__ip", "192.168.1.2",
					"__metalabel__foo__type", "type1",
					"job", "foo",
				).String(): {
					sample{t: 0, f: 0},
					sample{t: 2, f: 2},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t, nil, nil)
			t.Cleanup(func() {
				require.NoError(t, db.Close())
			})
			app1 := db.Appender(ctx)

			// Add a sample with metadata.
			for i, lbs := range tc.lbsStrings {
				_, err := app1.Append(0, labels.FromStrings(lbs...), int64(i), float64(i))
				require.NoError(t, err)
			}

			err := app1.Commit()
			require.NoError(t, err)

			q, err := db.Querier(0, 200)
			require.NoError(t, err)

			res := query(t, q, tc.lbsMatchers...)
			require.Equal(t, tc.result, res)
		})
	}
}
