// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promql_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/regexp"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
)

func TestIncludeInfoMetricDataLabels(t *testing.T) {
	const (
		load = `
load 10s
  metric{instance="a", job="1", label="value"} 0 1 2
  target_info{instance="a", job="1", data="info", another_data="another info"} 1 1 1
`
	)

	lb := labels.NewScratchBuilder(0)
	lb.Add(labels.MetricName, "metric")
	lb.Add("instance", "a")
	lb.Add("job", "1")
	lb.Add("label", "value")
	labelsWithoutInfoDataLabels := lb.Labels()
	lb.Add("data", "info")
	labelsWithInfoDataLabels := lb.Labels()
	targetInfoLabels := labels.FromStrings(
		labels.MetricName, "target_info", "instance", "a", "job", "1", "another_data", "another info", "data", "info",
	)

	testCases := []struct {
		name                     string
		query                    string
		enableAutomaticInclusion bool
		infoMetrics              map[string][]string
		dataLabelMatchers        map[string][]*labels.Matcher
		result                   promql.Matrix
	}{
		{
			name:                     "include info metric data label",
			query:                    "metric",
			enableAutomaticInclusion: true,
			infoMetrics:              map[string][]string{"target_info": {"instance", "job"}},
			dataLabelMatchers: map[string][]*labels.Matcher{
				"data": {labels.MustNewMatcher(labels.MatchRegexp, "data", ".+")},
			},
			result: promql.Matrix{
				promql.Series{
					Metric: labelsWithInfoDataLabels,
					Floats: []promql.FPoint{
						{
							T: 0,
							F: 0,
						},
						{
							T: 10000,
							F: 1,
						},
						{
							T: 20000,
							F: 2,
						},
					},
				},
			},
		},
		{
			name:                     "non-existent info metric",
			query:                    "metric",
			enableAutomaticInclusion: true,
			infoMetrics:              map[string][]string{"other_info": {"instance", "job"}},
			dataLabelMatchers: map[string][]*labels.Matcher{
				"data": {labels.MustNewMatcher(labels.MatchRegexp, "data", ".+")},
			},
			result: promql.Matrix{
				promql.Series{
					Metric: labelsWithoutInfoDataLabels,
					Floats: []promql.FPoint{
						{
							T: 0,
							F: 0,
						},
						{
							T: 10000,
							F: 1,
						},
						{
							T: 20000,
							F: 2,
						},
					},
				},
			},
		},
		{
			name:                     "don't include info metric data labels",
			query:                    "metric",
			enableAutomaticInclusion: false,
			infoMetrics:              map[string][]string{"target_info": {"instance", "job"}},
			dataLabelMatchers: map[string][]*labels.Matcher{
				"data": {labels.MustNewMatcher(labels.MatchRegexp, "data", ".+")},
			},
			result: promql.Matrix{
				promql.Series{
					Metric: labelsWithoutInfoDataLabels,
					Floats: []promql.FPoint{
						{
							T: 0,
							F: 0,
						},
						{
							T: 10000,
							F: 1,
						},
						{
							T: 20000,
							F: 2,
						},
					},
				},
			},
		},
		{
			name:                     "try including info metric data labels, but non-matching identifying labels",
			query:                    "metric",
			enableAutomaticInclusion: true,
			infoMetrics:              map[string][]string{"target_info": {"id"}},
			dataLabelMatchers: map[string][]*labels.Matcher{
				"data": {labels.MustNewMatcher(labels.MatchRegexp, "data", ".+")},
			},
			result: promql.Matrix{
				promql.Series{
					Metric: labelsWithoutInfoDataLabels,
					Floats: []promql.FPoint{
						{
							T: 0,
							F: 0,
						},
						{
							T: 10000,
							F: 1,
						},
						{
							T: 20000,
							F: 2,
						},
					},
				},
			},
		},
		{
			name:                     "info metrics themselves are ignored by default when it comes to enriching with info metric data labels",
			query:                    "target_info",
			enableAutomaticInclusion: true,
			infoMetrics:              map[string][]string{"target_info": {"instance", "job"}},
			dataLabelMatchers: map[string][]*labels.Matcher{
				"data": {labels.MustNewMatcher(labels.MatchRegexp, "data", ".+")},
			},
			result: promql.Matrix{
				promql.Series{
					Metric: targetInfoLabels,
					Floats: []promql.FPoint{
						{
							T: 0,
							F: 1,
						},
						{
							T: 10000,
							F: 1,
						},
						{
							T: 20000,
							F: 1,
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			engine := promqltest.NewTestEngine(false, 0, promqltest.DefaultMaxSamplesPerQuery, func(o *promql.EngineOpts) {
				o.IncludeInfoMetricLabels = promql.IncludeInfoMetricLabelsOpts{
					AutomaticInclusionEnabled: tc.enableAutomaticInclusion,
					InfoMetrics:               tc.infoMetrics,
					DataLabelMatchers:         tc.dataLabelMatchers,
					IgnoreMetrics:             []*regexp.Regexp{regexp.MustCompile("^.+_info$")},
				}
			})
			ctx := context.Background()
			storage := promqltest.LoadedStorage(t, load)
			t.Cleanup(func() { _ = storage.Close() })

			start := time.Unix(0, 0)
			end := time.Unix(20, 0)
			qry, err := engine.NewRangeQuery(ctx, storage, nil, tc.query, start, end, 10*time.Second)
			require.NoError(t, err)

			res := qry.Exec(ctx)
			require.NoError(t, res.Err)
			mat, ok := res.Value.(promql.Matrix)
			require.True(t, ok)

			t.Log("Expected labels", tc.result[0].Metric)
			t.Log("Labels", mat[0].Metric)
			require.Equal(t, tc.result, mat)
		})
	}
}
