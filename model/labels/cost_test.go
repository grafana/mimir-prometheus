// Copyright 2025 Grafana Labs
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

package labels

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var matcherTestCases = []struct {
	cost float64
	l    string
	t    MatchType
	v    string
}{
	// = matchers
	{1, "__name__", MatchEqual, "mimir_target_series_per_ingester"},
	{1, "__name__", MatchEqual, "cortex_partition_ring_partitions"},
	{1, "container", MatchEqual, "distributor"},
	{1, "state", MatchEqual, "Active"},
	{1, "name", MatchEqual, "ingester-partitions"},
	{1, "__name__", MatchEqual, "cortex_distributor_samples_in_total"},
	{1, "__name__", MatchEqual, "namespace_user:cortex_ingester_owned_series:sum_filtered_max_over_time_1d"},
	{1, "__name__", MatchEqual, "kube_statefulset_replicas"},
	{1, "__name__", MatchEqual, "adaptive_metrics_canary_agg"},
	{1, "__name__", MatchEqual, "loki_distributor_bytes_received_total"},
	{1, "__name__", MatchEqual, "cortex_lifecycler_read_only"},
	{1, "cluster", MatchEqual, "ops-region-0"},
	{1, "job", MatchEqual, "integrations/db-o11y"},
	{1, "__name__", MatchEqual, "up"},
	{1, "namespace", MatchEqual, "prod-service-01"},
	{1, "namespace", MatchEqual, "prod-service-02"},

	// =~ matchers
	{30, "statefulset", MatchRegexp, "(ingester|mimir-write).*"},
	{1, "cluster", MatchRegexp, ".+"},
	{1.1, "cluster", MatchRegexp, "prod-region-1"},
	{1.1, "tenant", MatchRegexp, "(29)"},
	{4, "partition", MatchRegexp, longRegex1},
	{1.1, "cluster", MatchRegexp, "prod-region-2"},
	{1, "__name__", MatchRegexp, "aws_.+_info"},
	{1.1, "cluster", MatchRegexp, "prod-region-3"},
	{1.1, "cluster", MatchRegexp, "ops-region-0"},
	{1.1, "namespace", MatchRegexp, "prod-service-03"},
	{4.4, "reason", MatchRegexp, "(rate_limited|per_stream_rate_limit|blocked_ingestion|missing_enforced_labels)"},
	{1.1, "cluster", MatchRegexp, "prod-region-4"},
	{28, "route", MatchRegexp, "(prometheus|api_prom)_api_v1_.+"},
	{1.1, "namespace", MatchRegexp, "prod-service-04"},
	{1.1, "namespace", MatchRegexp, "ops-service-01"},
	{4.4, "route", MatchRegexp, "api_(v1|prom)_push|otlp_v1_metrics|api_v1_push_influx_write"},
	{47, "route", MatchRegexp, ".*v1.*|.*prom.*"},
	{1, "job", MatchRegexp, "(prod-service-05)/((gateway|cortex-gw.*))"},
	{1, "job", MatchRegexp, "(ops-service-01)/((compactor.*|cortex|mimir))"},
	{1.1, "namespace", MatchRegexp, "prod-service-06"},
	{280, "route", MatchRegexp, "(/base.Ruler/Rules|/indexgatewaypb.IndexGateway/GetChunkRef|/indexgatewaypb.IndexGateway/GetSeries|/indexgatewaypb.IndexGateway/GetShards|/indexgatewaypb.IndexGateway/GetStats|/indexgatewaypb.IndexGateway/GetVolume|/indexgatewaypb.IndexGateway/LabelNamesForMetricName|/indexgatewaypb.IndexGateway/LabelValuesForMetricName|/indexgatewaypb.IndexGateway/QueryIndex|/logproto.BloomGateway/FilterChunkRefs|/logproto.Pattern/Query|/logproto.Querier/GetChunkIDs|/logproto.Querier/GetDetectedLabels|/logproto.Querier/GetStats|/logproto.Querier/GetVolume|/logproto.Querier/Label|/logproto.Querier/Query|/logproto.Querier/QuerySample|/logproto.Querier/Series|/logproto.StreamData/GetStreamRates)"},

	// != matchers
	{1, "job", MatchNotEqual, "integrations/db-o11y"},
	{1, "version", MatchNotEqual, "12.1.0-91295"},
	{1, "namespace", MatchNotEqual, "AWS/ECS"},
	{1, "container", MatchNotEqual, "istio-proxy"},
	{1, "owner_kind", MatchNotEqual, "ReplicaSet"},
	{1, "target", MatchNotEqual, "remote"},
	{1, "status", MatchNotEqual, "ok"},
	{1, "topic", MatchNotEqual, ""},

	// !~ matchers
	{1, "statefulset", MatchNotRegexp, "ingester-zone-.-partition"},
	{33, "topic", MatchNotRegexp, "(.+)-KSTREAM-AGGREGATE-STATE-STORE-(.+)"},
	{2.2, "workload_type", MatchNotRegexp, "job|cronjob"},
	{2.2, "created_by_kind", MatchNotRegexp, "Job|TaskRun"},
	{1, "namespace", MatchNotRegexp, "kube-.*"},
	{2.2, "job", MatchNotRegexp, "(ecs-dockerstats-exporter)|(vmagent)"},
	{31, "job", MatchNotRegexp, ".*envoy-stats.*"},
	{21, "db_name", MatchNotRegexp, "template.*|^$"},
	{31, "exported_job", MatchNotRegexp, ".*envoy-stats.*"},
	{5.5, "job", MatchNotRegexp, "integrations/(windows|node_exporter|unix|docker|db-o11y)"},
	{4.4, "job", MatchNotRegexp, "integrations/(windows|node_exporter|unix|docker)"},
	{2.2, "k8s_src_owner_type", MatchNotRegexp, "Pod|Node"},
	{2.2, "k8s_dst_owner_type", MatchNotRegexp, "Pod|Node"},
	{18, "image_spec", MatchNotRegexp, "(.*):1364de3"},
	{18, "image_spec", MatchNotRegexp, "(.*):cfc5ca8"},
	{1.1, "namespace", MatchNotRegexp, "(ops-service-02)"},
	{1, "slug", MatchNotRegexp, "ephemeral.*"},
	{18, "image_spec", MatchNotRegexp, "(.*):d849bcd"},
	{18, "image_spec", MatchNotRegexp, "(.*):cb8eaaa"},

	// long matchers
	{4, "pod", MatchRegexp, longRegex2},
	{27569, "pod", MatchRegexp, longRegex3},
	{9065, "pod", MatchRegexp, longRegex4},
}

func TestSingleMatchCost(t *testing.T) {
	for _, tt := range matcherTestCases {
		matcher, err := NewMatcher(tt.t, tt.l, tt.v)
		require.NoError(t, err)
		matcherStr := matcher.String()
		if truncatelen := 50; len(matcherStr) > truncatelen {
			matcherStr = matcherStr[:truncatelen]
		}
		t.Run(matcherStr, func(t *testing.T) {
			require.Equal(t, tt.cost, matcher.SingleMatchCost())
		})
	}
}

func TestSelectivity(t *testing.T) {
	tests := []struct {
		numSeries   uint64
		selectivity float64
		l           string
		t           MatchType
		v           string
	}{
		{0, 1.0, "name", MatchEqual, "up"},
		{0, 1.0, "name", MatchEqual, ""},
		{0, 1.0, "name", MatchNotEqual, "up"},
		{0, 1.0, "name", MatchNotEqual, ""},
		{0, 1.0, "name", MatchRegexp, "up|kube_pod_info"},
		{0, 1.0, "name", MatchRegexp, ".+"},
		{0, 1.0, "name", MatchNotRegexp, "up|kube_pod_info"},
		{0, 1.0, "name", MatchNotRegexp, ".+"},

		{1, 1.0, "name", MatchRegexp, "up|kube_pod_info"},
		{1, 0.0, "name", MatchNotRegexp, "up|kube_pod_info"},
		{2, 1.0, "name", MatchRegexp, "up|kube_pod_info"},
		{8, 0.25, "name", MatchRegexp, "up|kube_pod_info"},
		{8, 0.75, "name", MatchNotRegexp, "up|kube_pod_info"},
		{8, 0.375, "name", MatchRegexp, "up|kube_pod_info|down"},

		{8, 0.125, "name", MatchEqual, "up"},
		{8, 0.875, "name", MatchNotEqual, "up"},
		{8, 0.125, "name", MatchRegexp, "up"},
		{8, 0.875, "name", MatchNotRegexp, "up"},

		{8, 0.0, "name", MatchRegexp, ""},
		{8, 1.0, "name", MatchNotRegexp, ""},
		{8, 0.0, "name", MatchEqual, ""},
		{8, 1.0, "name", MatchNotEqual, ""},

		{8, 1.0, "name", MatchRegexp, ".+"},
		{8, 0.0, "name", MatchNotRegexp, ".+"},
		{8, 0.1, "name", MatchRegexp, ".*(foo|bar).*"},
		{8, 0.9, "name", MatchNotRegexp, ".*(foo|bar).*"},
		{8, 0.1, "name", MatchRegexp, "(foo|bar).*"},
		{8, 0.1, "name", MatchRegexp, ".*foo.*|.*bar.*"},

		{math.MaxUint64, 0, "name", MatchEqual, "up"},
		{math.MaxUint64, 0, "name", MatchRegexp, "up|kube_pod_info"},
	}

	for _, tt := range tests {
		matcher, err := NewMatcher(tt.t, tt.l, tt.v)
		require.NoError(t, err)
		t.Run(fmt.Sprintf("%d series {%s}", tt.numSeries, matcher), func(t *testing.T) {
			// Tolerate a single value error in 10M values
			const tolerance = 1e-7
			actualSelectivity := matcher.EstimateSelectivity(tt.numSeries)
			require.InDelta(t, tt.selectivity, actualSelectivity, tolerance)
		})
	}
}

func BenchmarkStringEquality(b *testing.B) {
	benchmarks := []struct {
		name     string
		str1     string
		str2     string
		expected bool
	}{
		{"flavour=Equal_8chars", "abc12345", "abc12345", true},
		{"flavour=Equal_32chars", strings.Repeat("abcd", 8), strings.Repeat("abcd", 8), true},
		{"flavour=Equal_64chars", strings.Repeat("abcd", 16), strings.Repeat("abcd", 16), true},
		{"flavour=NotEqual_8chars", "abc12345", "abc12346", false},
		{"flavour=NotEqual_32chars", strings.Repeat("abcd", 8), strings.Repeat("abce", 8), false},
		{"flavour=NotEqual_64chars", strings.Repeat("abcd", 16), strings.Repeat("abce", 16), false},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			var result bool
			for i := 0; i < b.N; i++ {
				result = bm.str1 == bm.str2
			}
			if result != bm.expected {
				b.Fatalf("unexpected result: got %v, want %v", result, bm.expected)
			}
		})
	}
}

func BenchmarkStringHasPrefix(b *testing.B) {
	shortPrefix := "abcd1234"
	shortTarget := shortPrefix + strings.Repeat("xyz", 10)

	longPrefix := strings.Repeat("abcd", 8)
	longTarget := longPrefix + strings.Repeat("xyz", 10)

	nearMissPrefix := strings.Repeat("a", 31) + "b"
	nearMissTarget := strings.Repeat("a", 32)

	benchmarks := []struct {
		name     string
		target   string
		prefix   string
		expected bool
	}{
		{"flavour=ShortPrefix_8chars_Match", shortTarget, shortPrefix, true},
		{"flavour=LongPrefix_32chars_Match", longTarget, longPrefix, true},
		{"flavour=NearMiss_LastChar_32times", nearMissTarget, nearMissPrefix, false},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			var result bool
			for i := 0; i < b.N; i++ {
				result = strings.HasPrefix(bm.target, bm.prefix)
			}
			if result != bm.expected {
				b.Fatalf("unexpected result: got %v, want %v", result, bm.expected)
			}
		})
	}
}

func BenchmarkSliceContains(b *testing.B) {
	createSlice := func(size int) []string {
		slice := make([]string, size)
		for i := 0; i < size; i++ {
			slice[i] = fmt.Sprintf("item_%d", i)
		}
		return slice
	}

	searchTarget := "target_item"

	benchmarks := []struct {
		name string
		size int
	}{
		{"size=1", 1},
		{"size=2", 2},
		{"size=8", 8},
		{"size=16", 16},
	}

	for _, bm := range benchmarks {
		slice := createSlice(bm.size)
		slice = append(slice, searchTarget)

		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			var found bool
			for i := 0; i < b.N; i++ {
				found = false
				for _, item := range slice {
					if item == searchTarget {
						found = true
						break
					}
				}
			}
			if !found {
				b.Fatal("target not found in slice")
			}
		})
	}
}

func BenchmarkMapContains(b *testing.B) {
	createMap := func(size int) map[string]bool {
		m := make(map[string]bool, size)
		for i := 0; i < size; i++ {
			m[fmt.Sprintf("key_%d", i)] = true
		}
		return m
	}

	searchTarget := "target_key"

	benchmarks := []struct {
		name string
		size int
	}{
		{"size=2", 2},
		{"size=16", 16},
		{"size=32", 32},
		{"size=128", 128},
		{"size=256", 256},
	}

	for _, bm := range benchmarks {
		m := createMap(bm.size)
		m[searchTarget] = true

		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			var found bool
			for i := 0; i < b.N; i++ {
				_, found = m[searchTarget]
			}
			if !found {
				b.Fatal("target not found in map")
			}
		})
	}
}

func BenchmarkCostEstimation(b *testing.B) {
	b.Run("op=StringEquality", func(b *testing.B) {
		BenchmarkStringEquality(b)
	})
	b.Run("op=StringHasPrefix", func(b *testing.B) {
		BenchmarkStringHasPrefix(b)
	})
	b.Run("op=SliceContains", func(b *testing.B) {
		BenchmarkSliceContains(b)
	})
	b.Run("op=MapContains", func(b *testing.B) {
		BenchmarkMapContains(b)
	})
}

type rankedMatcher struct {
	cost        float64
	timePerOp   time.Duration
	matcher     *Matcher
	costRank    int
	runtimeRank int
}

func BenchmarkMatcherCostVsRuntime(b *testing.B) {
	var matchers []rankedMatcher

	for _, tc := range matcherTestCases {
		matcher, err := NewMatcher(tc.t, tc.l, tc.v)
		if err != nil {
			b.Fatalf("Failed to create matcher: %v", err)
		}

		matchers = append(matchers, rankedMatcher{
			cost:    tc.cost,
			matcher: matcher,
		})

		matcherStr := matcher.String()
		if len(matcherStr) > 50 {
			matcherStr = matcherStr[:50]
		}

		matcherWithRuntime := &(matchers[len(matchers)-1])
		b.Run(matcherStr, func(b *testing.B) {
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				for _, value := range values {
					_ = matcher.Matches(value)
				}
			}
			elapsed := time.Since(start)
			timePerOp := elapsed / time.Duration(b.N)

			// Round to nearest 10ns for more fair ranking.
			const roundToXNanosec = 10
			roundedTime := ((timePerOp + roundToXNanosec - 1) / roundToXNanosec) * roundToXNanosec
			matcherWithRuntime.timePerOp = roundedTime
		})
	}

	rankByRuntime(matchers)
	rankByCost(matchers)

	// Sort by how closely cost ranking matches runtime ranking.
	sort.Slice(matchers, func(i, j int) bool {
		return math.Abs(float64(matchers[i].costRank-matchers[i].runtimeRank)) <
			math.Abs(float64(matchers[j].costRank-matchers[j].runtimeRank))
	})

	avgRankDiff := 0
	for _, cm := range matchers {
		matcherStr := cm.matcher.String()
		if len(matcherStr) > 80 {
			matcherStr = matcherStr[:80] + "..."
		}
		avgRankDiff += int(math.Abs(float64(cm.costRank - cm.runtimeRank)))
		b.Logf("rankDiff=%2d\tcostRank=%2d\truntimeRank=%2d\tcost=%6.1f\ttimePerOp=%10s\t: %s", cm.costRank-cm.runtimeRank, cm.costRank, cm.runtimeRank, cm.cost, cm.timePerOp, matcherStr)
	}
	b.Logf("Average rank difference: %.2f", float64(avgRankDiff)/float64(len(matchers)))
	b.Logf("Kendall's Tau: %.4f (1.0 = perfect positive correlation, -1.0 = perfect negative correlation)", kendallsTau(matchers))
}

func rankByRuntime(matchers []rankedMatcher) {
	sort.Slice(matchers, func(i, j int) bool {
		return matchers[i].timePerOp < matchers[j].timePerOp
	})

	currentRank := 1
	for i := range matchers {
		if i > 0 && matchers[i].timePerOp != matchers[i-1].timePerOp {
			currentRank++
		}
		matchers[i].runtimeRank = currentRank
	}
}

func rankByCost(matchers []rankedMatcher) {
	sort.Slice(matchers, func(i, j int) bool {
		return matchers[i].cost < matchers[j].cost
	})

	currentRank := 1
	for i := range matchers {
		if i > 0 && matchers[i].cost != matchers[i-1].cost {
			currentRank++
		}
		matchers[i].costRank = currentRank
	}
}

// kendallsTau calculates Kendall's Tau correlation coefficient between two rankings.
// It measures the ordinal association between the two rankings.
// Returns a value between -1 and 1, where 1 indicates perfect positive correlation,
// -1 indicates perfect negative correlation, and 0 indicates no correlation.
// https://en.wikipedia.org/wiki/Kendall_rank_correlation_coefficient
func kendallsTau(matchers []rankedMatcher) float64 {
	n := len(matchers)
	if n < 2 {
		return 0.0
	}

	concordant := 0
	discordant := 0
	tiesX := 0
	tiesY := 0

	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			xi, xj := matchers[i].costRank, matchers[j].costRank
			yi, yj := matchers[i].runtimeRank, matchers[j].runtimeRank

			xDiff := xi - xj
			yDiff := yi - yj

			switch {
			case xDiff == 0 && yDiff == 0:
				continue
			case xDiff == 0:
				tiesX++
			case yDiff == 0:
				tiesY++
			case (xDiff > 0 && yDiff > 0) || (xDiff < 0 && yDiff < 0):
				concordant++
			default:
				discordant++
			}
		}
	}

	// Calculate Kendall's Tau using the general formula that handles ties
	numerator := float64(concordant - discordant)
	denominator := math.Sqrt(float64(concordant+discordant+tiesX) * float64(concordant+discordant+tiesY))

	if denominator == 0 {
		return 0.0
	}

	return numerator / denominator
}
