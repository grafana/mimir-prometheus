package labels

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		{5.2, "partition", MatchRegexp, "0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56|57|58|59|60|61|62|63|64|65|66|67|68|69|70|71|72|73|74|75|76|77|78|79|80|81|82|83|84|85|86|87|88|89|90|91|92|93|94|95|96|97|98|99|100|101|102|103|104|105|106|107|108|109|110|111|112|113|114|115|116|117|118|119|120|121|122|123|124|125|126|127|128|129|130|131|132|133|134|135|136|137|138|139|140|141|142|143|144|145|146|147|148|149|150|151|152|153|154|155|156|157|158|159|160|161|162|163|164|165|166|167|168|169|170|171|172|173|174|175|176|177|178|179|180|181|182|183|184|185|186|187|188|189|190|191|192|193|194|195|196|197|198|199|200|201|202|203|204|205|206|207|208|209|210|211|212|213|214|215|216|217|218|219|220|221|222|223|224|225|226|227|228|229|230|231|232|233|234|235|236|237|238|239|240|241|242|243|244|245|246|247|248|249|250|251|252|253|254|255|256|257|258|259|260|261|262|263|264|265|266|267|268|269|270|271|272|273|274|275|276|277|278|279|280|281|282|283|284|285|286|287|288|289|290|291|292|293|294|295|296|297|298|299|300|301|302|303|304|305|306|307|308|309|310|311|312|313|314|315|316|317|318|319|320|321|322|323|324|325|326|327|328|329|330|331|332|333|334|335|336|337|338|339|340|341|342|343|344|345|346|347|348|349|350|351|352|353|354|355|356|357|358|359|360|361|362|363|364|365|366|367|368|369|370|371|372|373|374|375|376|377|378|379|380|381|382|383|384|385|386|387|388|389|390|391|392|393|394|395|396|397|398|399|400|401|402|403|404|405|406|407|408|409|410|411|412|413|414|415|416|417|418|419"},
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
		{71.49, "pod", MatchRegexp, longRegex1},
		{27569, "pod", MatchRegexp, longRegex2},
		{9065, "pod", MatchRegexp, longRegex3},
	}

	for _, tt := range tests {
		matcher, err := NewMatcher(tt.t, tt.l, tt.v)
		assert.NoError(t, err)
		matcherStr := matcher.String()
		if truncatelen := 50; len(matcherStr) > truncatelen {
			matcherStr = matcherStr[:truncatelen]
		}
		t.Run(matcherStr, func(t *testing.T) {
			assert.Equal(t, tt.cost, matcher.SingleMatchCost())
		})
	}
}
