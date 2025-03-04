package tsdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

type labelStats struct {
	totalValues int
	totalSeries int
}

func TestPlanIndexLookup(t *testing.T) {

	// Define test series with their labels
	stats := struct {
		totalSeries    int
		labelNameStats map[string]labelStats
	}{
		totalSeries: 1000,
		labelNameStats: map[string]labelStats{
			"__name__": {totalValues: 10, totalSeries: 1000},
			"method":   {totalValues: 4, totalSeries: 100},
			"status":   {totalValues: 5, totalSeries: 100},
			"pod":      {totalValues: 1000, totalSeries: 1000},
			"instance": {totalValues: 3, totalSeries: 1000},
			"job":      {totalValues: 1, totalSeries: 1000},
		},
	}

	ix := &mockIndexPostingsReader{
		stats: stats,
	}

	cases := []struct {
		name            string
		matchers        []*labels.Matcher
		expectedIndex   []*labels.Matcher // matchers we expect to be used for index lookup
		expectedPending []*labels.Matcher // matchers we expect to be evaluated later
	}{
		{
			name: "one specific matcher is enough",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "method", "GET"),
				labels.MustNewMatcher(labels.MatchEqual, "pod", "pod1"),
			},
			expectedIndex: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "pod", "pod1"),
			},
			expectedPending: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "method", "GET"),
			},
		},
		{
			name: "a specific regex pod matcher should be used",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod1|pod2"),
			},
			expectedIndex: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod1|pod2"),
			},
			expectedPending: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Call planIndexLookup
			p, err := planIndexLookup(context.Background(), tc.matchers, ix, nil)
			require.NoError(t, err)

			// Assert on the plan's matchers
			matchersEqual(t, p.indexMatchers(), tc.expectedIndex, "index matchers")
			matchersEqual(t, p.pendingMatchers(), tc.expectedPending, "pending matchers")
		})
	}
}

type mockIndexPostingsReader struct {
	stats struct {
		totalSeries    int
		labelNameStats map[string]labelStats
	}
}

func (m *mockIndexPostingsReader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	panic("implement me")
}

func (m *mockIndexPostingsReader) PostingsForLabelMatching(ctx context.Context, name string, match func(value string) bool) index.Postings {
	panic("implement me")
}

func (m *mockIndexPostingsReader) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	panic("implement me")
}

func (m *mockIndexPostingsReader) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, error) {
	panic("implement me")
}

func (m *mockIndexPostingsReader) LabelValuesCount(ctx context.Context, name string) (int64, error) {
	return int64(m.stats.labelNameStats[name].totalValues), nil
}

func (m *mockIndexPostingsReader) TotalSeriesWithLabel(ctx context.Context, name string) (int64, error) {
	return int64(m.stats.labelNameStats[name].totalSeries), nil
}

func (m *mockIndexPostingsReader) TotalSeries() int64 {
	return int64(m.stats.totalSeries)
}

// Helper function to compare slices of matchers
func matchersEqual(t *testing.T, a, b []*labels.Matcher, msg string) {
	expectedMatchers := []string{}
	for _, m := range b {
		expectedMatchers = append(expectedMatchers, m.String())
	}

	gotMatchers := []string{}
	for _, m := range a {
		gotMatchers = append(gotMatchers, m.String())
	}
	require.ElementsMatch(t, expectedMatchers, gotMatchers, msg)
}
