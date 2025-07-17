package labels

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleMatchCost(t *testing.T) {
	tests := []struct {
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
		{1, "cluster", MatchEqual, "ops-eu-south-0"},
		{1, "job", MatchEqual, "integrations/db-o11y"},
		{1, "__name__", MatchEqual, "up"},
		{1, "namespace", MatchEqual, "hosted-grafana"},
		{1, "namespace", MatchEqual, "grafana-com"},

		// =~ matchers
		{30, "statefulset", MatchRegexp, "(ingester|mimir-write).*"},
		{1, "cluster", MatchRegexp, ".+"},
		{1, "cluster", MatchRegexp, "prod-gb-south-1"},
		{1, "tenant", MatchRegexp, "(29)"},
		{5.2, "partition", MatchRegexp, longRegex1},
		{1, "cluster", MatchRegexp, "prod-us-east-0"},
		{0.5, "__name__", MatchRegexp, "aws_.+_info"},
		{1, "cluster", MatchRegexp, "prod-eu-west-2"},
		{1, "cluster", MatchRegexp, "ops-eu-south-0"},
		{1, "namespace", MatchRegexp, "loki-prod-035"},
		{4, "reason", MatchRegexp, "(rate_limited|per_stream_rate_limit|blocked_ingestion|missing_enforced_labels)"},
		{1, "cluster", MatchRegexp, "prod-us-central-0"},
		{28, "route", MatchRegexp, "(prometheus|api_prom)_api_v1_.+"},
		{1, "namespace", MatchRegexp, "asserts"},
		{1, "namespace", MatchRegexp, "mimir-ops-03"},
		{4, "route", MatchRegexp, "api_(v1|prom)_push|otlp_v1_metrics|api_v1_push_influx_write"},
		{0.5, "job", MatchRegexp, "(cortex-prod-13)/((gateway|cortex-gw.*))"},
		{0.5, "job", MatchRegexp, "(mimir-ops-03)/((compactor.*|cortex|mimir))"},
		{1, "namespace", MatchRegexp, "loki-prod-031"},
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
		{0.5, "statefulset", MatchNotRegexp, "ingester-zone-.-partition"},
		{33, "topic", MatchNotRegexp, "(.+)-KSTREAM-AGGREGATE-STATE-STORE-(.+)"},
		{2, "workload_type", MatchNotRegexp, "job|cronjob"},
		{2, "created_by_kind", MatchNotRegexp, "Job|TaskRun"},
		{0.5, "namespace", MatchNotRegexp, "kube-.*"},
		{2, "job", MatchNotRegexp, "(ecs-dockerstats-exporter)|(vmagent)"},
		{31, "job", MatchNotRegexp, ".*envoy-stats.*"},
		{21, "db_name", MatchNotRegexp, "template.*|^$"},
		{31, "exported_job", MatchNotRegexp, ".*envoy-stats.*"},
		{5, "job", MatchNotRegexp, "integrations/(windows|node_exporter|unix|docker|db-o11y)"},
		{4, "job", MatchNotRegexp, "integrations/(windows|node_exporter|unix|docker)"},
		{2, "k8s_src_owner_type", MatchNotRegexp, "Pod|Node"},
		{2, "k8s_dst_owner_type", MatchNotRegexp, "Pod|Node"},
		{18, "image_spec", MatchNotRegexp, "(.*):1364de3"},
		{18, "image_spec", MatchNotRegexp, "(.*):cfc5ca8"},
		{1, "namespace", MatchNotRegexp, "(cortex-ops-01)"},
		{0.5, "slug", MatchNotRegexp, "ephemeral.*"},
		{18, "image_spec", MatchNotRegexp, "(.*):d849bcd"},
		{18, "image_spec", MatchNotRegexp, "(.*):cb8eaaa"},

		// long matchers
		{71.49, "pod", MatchRegexp, longRegex2},
		{27569, "pod", MatchRegexp, longRegex3},
		{9065, "pod", MatchRegexp, longRegex4},
	}

	for _, tt := range tests {
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

		{8, 0.1, "name", MatchRegexp, ".*(foo|bar).*"},
		{8, 0.9, "name", MatchNotRegexp, ".*(foo|bar).*"},
		{8, 0.1, "name", MatchRegexp, "(foo|bar).*"},

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
