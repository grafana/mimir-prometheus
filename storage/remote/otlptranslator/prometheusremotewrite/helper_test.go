// Copyright 2024 The Prometheus Authors
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
// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/debbf30360b8d3a0ded8db09c4419d2a9c99b94a/pkg/translator/prometheusremotewrite/helper_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package prometheusremotewrite

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestCreateAttributes(t *testing.T) {
	resourceAttrs := map[string]string{
		"service.name":        "service name",
		"service.instance.id": "service ID",
		"existent-attr":       "resource value",
		// This one is for testing conflict with metric attribute.
		"metric-attr": "resource value",
		// This one is for testing conflict with auto-generated job attribute.
		"job": "resource value",
		// This one is for testing conflict with auto-generated instance attribute.
		"instance": "resource value",
	}
	scopeAttrs := pcommon.NewMap()
	scopeAttrs.FromRaw(map[string]any{
		"attr1": "value1",
		"attr2": "value2",
	})
	defaultScope := scope{
		name:       "test-scope",
		version:    "1.0.0",
		schemaURL:  "https://schema.com",
		attributes: scopeAttrs,
	}

	resource := pcommon.NewResource()
	for k, v := range resourceAttrs {
		resource.Attributes().PutStr(k, v)
	}
	attrs := pcommon.NewMap()
	attrs.PutStr("metric-attr", "metric value")
	attrs.PutStr("metric-attr-other", "metric value other")

	testCases := []struct {
		name                         string
		scope                        scope
		promoteAllResourceAttributes bool
		promoteResourceAttributes    []string
		promoteScope                 bool
		ignoreResourceAttributes     []string
		ignoreAttrs                  []string
		expectedLabels               []prompb.Label
	}{
		{
			name:                      "Successful conversion without resource attribute promotion and without scope promotion",
			scope:                     defaultScope,
			promoteScope:              false,
			promoteResourceAttributes: nil,
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "metric_attr_other",
					Value: "metric value other",
				},
			},
		},
		{
			name:                      "Successful conversion without resource attribute promotion and with scope promotion",
			scope:                     defaultScope,
			promoteScope:              true,
			promoteResourceAttributes: nil,
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "metric_attr_other",
					Value: "metric value other",
				},
				{
					Name:  "otel_scope_name",
					Value: defaultScope.name,
				},
				{
					Name:  "otel_scope_schema_url",
					Value: defaultScope.schemaURL,
				},
				{
					Name:  "otel_scope_version",
					Value: defaultScope.version,
				},
				{
					Name:  "otel_scope_attr1",
					Value: "value1",
				},
				{
					Name:  "otel_scope_attr2",
					Value: "value2",
				},
			},
		},
		{
			name:                      "Successful conversion without resource attribute promotion and with scope promotion, but without scope",
			scope:                     scope{},
			promoteResourceAttributes: nil,
			promoteScope:              true,
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "metric_attr_other",
					Value: "metric value other",
				},
			},
		},
		{
			name:                      "Successful conversion with some attributes ignored",
			promoteResourceAttributes: nil,
			ignoreAttrs:               []string{"metric-attr-other"},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
		},
		{
			name:                      "Successful conversion with some attributes ignored and with scope promotion",
			scope:                     defaultScope,
			promoteScope:              true,
			promoteResourceAttributes: nil,
			ignoreAttrs:               []string{"metric-attr-other"},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "otel_scope_name",
					Value: defaultScope.name,
				},
				{
					Name:  "otel_scope_version",
					Value: defaultScope.version,
				},
				{
					Name:  "otel_scope_schema_url",
					Value: defaultScope.schemaURL,
				},
				{
					Name:  "otel_scope_attr1",
					Value: "value1",
				},
				{
					Name:  "otel_scope_attr2",
					Value: "value2",
				},
			},
		},
		{
			name:                      "Successful conversion with resource attribute promotion and with scope promotion",
			scope:                     defaultScope,
			promoteResourceAttributes: []string{"non-existent-attr", "existent-attr"},
			promoteScope:              true,
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "metric_attr_other",
					Value: "metric value other",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "otel_scope_name",
					Value: defaultScope.name,
				},
				{
					Name:  "otel_scope_version",
					Value: defaultScope.version,
				},
				{
					Name:  "otel_scope_schema_url",
					Value: defaultScope.schemaURL,
				},
				{
					Name:  "otel_scope_attr1",
					Value: "value1",
				},
				{
					Name:  "otel_scope_attr2",
					Value: "value2",
				},
			},
		},
		{
			name:                      "Successful conversion with resource attribute promotion and with scope promotion, conflicting resource attributes are ignored",
			scope:                     defaultScope,
			promoteScope:              true,
			promoteResourceAttributes: []string{"non-existent-attr", "existent-attr", "metric-attr", "job", "instance"},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "metric_attr_other",
					Value: "metric value other",
				},
				{
					Name:  "otel_scope_name",
					Value: defaultScope.name,
				},
				{
					Name:  "otel_scope_version",
					Value: defaultScope.version,
				},
				{
					Name:  "otel_scope_schema_url",
					Value: defaultScope.schemaURL,
				},
				{
					Name:  "otel_scope_attr1",
					Value: "value1",
				},
				{
					Name:  "otel_scope_attr2",
					Value: "value2",
				},
			},
		},
		{
			name:                      "Successful conversion with resource attribute promotion and with scope promotion, attributes are only promoted once",
			scope:                     defaultScope,
			promoteScope:              true,
			promoteResourceAttributes: []string{"existent-attr", "existent-attr"},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "metric_attr_other",
					Value: "metric value other",
				},
				{
					Name:  "otel_scope_name",
					Value: defaultScope.name,
				},
				{
					Name:  "otel_scope_version",
					Value: defaultScope.version,
				},
				{
					Name:  "otel_scope_schema_url",
					Value: defaultScope.schemaURL,
				},
				{
					Name:  "otel_scope_attr1",
					Value: "value1",
				},
				{
					Name:  "otel_scope_attr2",
					Value: "value2",
				},
			},
		},
		{
			name:                         "Successful conversion promoting all resource attributes and with scope promotion",
			scope:                        defaultScope,
			promoteAllResourceAttributes: true,
			promoteScope:                 true,
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "metric_attr_other",
					Value: "metric value other",
				},
				{
					Name:  "service_name",
					Value: "service name",
				},
				{
					Name:  "service_instance_id",
					Value: "service ID",
				},
				{
					Name:  "otel_scope_name",
					Value: defaultScope.name,
				},
				{
					Name:  "otel_scope_version",
					Value: defaultScope.version,
				},
				{
					Name:  "otel_scope_schema_url",
					Value: defaultScope.schemaURL,
				},
				{
					Name:  "otel_scope_attr1",
					Value: "value1",
				},
				{
					Name:  "otel_scope_attr2",
					Value: "value2",
				},
			},
		},
		{
			name:                         "Successful conversion promoting all resource attributes and with scope promotion, ignoring 'service.instance.id'",
			scope:                        defaultScope,
			promoteScope:                 true,
			promoteAllResourceAttributes: true,
			ignoreResourceAttributes: []string{
				"service.instance.id",
			},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "metric_attr_other",
					Value: "metric value other",
				},
				{
					Name:  "service_name",
					Value: "service name",
				},
				{
					Name:  "otel_scope_name",
					Value: defaultScope.name,
				},
				{
					Name:  "otel_scope_version",
					Value: defaultScope.version,
				},
				{
					Name:  "otel_scope_schema_url",
					Value: defaultScope.schemaURL,
				},
				{
					Name:  "otel_scope_attr1",
					Value: "value1",
				},
				{
					Name:  "otel_scope_attr2",
					Value: "value2",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			settings := Settings{
				PromoteResourceAttributes: NewPromoteResourceAttributes(config.OTLPConfig{
					PromoteAllResourceAttributes: tc.promoteAllResourceAttributes,
					PromoteResourceAttributes:    tc.promoteResourceAttributes,
					IgnoreResourceAttributes:     tc.ignoreResourceAttributes,
				}),
				PromoteScopeMetadata: tc.promoteScope,
			}
			lbls := createAttributes(resource, attrs, tc.scope, settings, tc.ignoreAttrs, false, prompb.MetricMetadata{}, model.MetricNameLabel, "test_metric")

			require.ElementsMatch(t, lbls, tc.expectedLabels)
		})
	}
}

func Test_convertTimeStamp(t *testing.T) {
	tests := []struct {
		name string
		arg  pcommon.Timestamp
		want int64
	}{
		{"zero", 0, 0},
		{"1ms", 1_000_000, 1},
		{"1s", pcommon.Timestamp(time.Unix(1, 0).UnixNano()), 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertTimeStamp(tt.arg)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestPrometheusConverter_AddSummaryDataPoints(t *testing.T) {
	scopeAttrs := pcommon.NewMap()
	scopeAttrs.FromRaw(map[string]any{
		"attr1": "value1",
		"attr2": "value2",
	})
	defaultScope := scope{
		name:       "test-scope",
		version:    "1.0.0",
		schemaURL:  "https://schema.com",
		attributes: scopeAttrs,
	}

	now := time.Now()
	nowUnixNano := pcommon.Timestamp(now.UnixNano())
	nowMinus2m30s := pcommon.Timestamp(now.Add(-2 * time.Minute).Add(-30 * time.Second).UnixNano())
	nowMinus6m := pcommon.Timestamp(now.Add(-20 * time.Second).UnixNano())
	nowMinus1h := pcommon.Timestamp(now.Add(-1 * time.Hour).UnixNano())
	tests := []struct {
		name                  string
		metric                func() pmetric.Metric
		scope                 scope
		promoteScope          bool
		overrideValidInterval time.Duration
		want                  func() map[uint64]*prompb.TimeSeries
	}{
		{
			name: "summary with start time and without scope promotion",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowUnixNano)

				return metric
			},
			scope:        defaultScope,
			promoteScope: false,
			want: func() map[uint64]*prompb.TimeSeries {
				countLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + createdSuffix},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(countLabels): {
						Labels: countLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowUnixNano)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
		{
			name: "summary with start time equal to sample timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowUnixNano)

				return metric
			},
			scope:        defaultScope,
			promoteScope: false,
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + createdSuffix},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowUnixNano)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
		{
			name: "summary with start time within default valid interval to sample timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowMinus2m30s)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + createdSuffix},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: math.Float64frombits(value.QuietZeroNaN), Timestamp: convertTimeStamp(nowMinus2m30s)},
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: math.Float64frombits(value.QuietZeroNaN), Timestamp: convertTimeStamp(nowMinus2m30s)},
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowMinus2m30s)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
			overrideValidInterval: 10 * time.Minute,
		},
		{
			name: "summary with start time within overiden valid interval to sample timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowMinus6m)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + createdSuffix},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: math.Float64frombits(value.QuietZeroNaN), Timestamp: convertTimeStamp(nowMinus6m)},
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: math.Float64frombits(value.QuietZeroNaN), Timestamp: convertTimeStamp(nowMinus6m)},
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowMinus6m)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
		{
			name: "summary with start time and with scope conversion",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowMinus1h)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + createdSuffix},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowMinus1h)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
		{
			name: "summary with start time and with scope promotion",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowUnixNano)

				return metric
			},
			scope:        defaultScope,
			promoteScope: true,
			want: func() map[uint64]*prompb.TimeSeries {
				scopeLabels := []prompb.Label{
					{
						Name:  "otel_scope_attr1",
						Value: "value1",
					},
					{
						Name:  "otel_scope_attr2",
						Value: "value2",
					},
					{
						Name:  "otel_scope_name",
						Value: defaultScope.name,
					},
					{
						Name:  "otel_scope_schema_url",
						Value: defaultScope.schemaURL,
					},
					{
						Name:  "otel_scope_version",
						Value: defaultScope.version,
					},
				}
				countLabels := append([]prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}, scopeLabels...)
				sumLabels := append([]prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}, scopeLabels...)
				createdLabels := append([]prompb.Label{
					{
						Name:  model.MetricNameLabel,
						Value: "test_summary" + createdSuffix,
					},
				}, scopeLabels...)
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(countLabels): {
						Labels: countLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowUnixNano)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
		{
			name: "summary without start time and without scope promotion",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)

				return metric
			},
			scope:        defaultScope,
			promoteScope: false,
			want: func() map[uint64]*prompb.TimeSeries {
				countLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(countLabels): {
						Labels: countLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := tt.metric()
			converter := NewPrometheusConverter()

			err := converter.addSummaryDataPoints(
				context.Background(),
				metric.Summary().DataPoints(),
				pcommon.NewResource(),
				Settings{
					ExportCreatedMetric:                        true,
					PromoteScopeMetadata:                       tt.promoteScope,
					EnableCreatedTimestampZeroIngestion:        true,
					EnableStartTimeQuietZero:                   true,
					ValidIntervalCreatedTimestampZeroIngestion: tt.overrideValidInterval,
				},
				prompb.MetricMetadata{MetricFamilyName: metric.Name()},
				tt.scope,
				promslog.NewNopLogger(),
			)
			require.NoError(t, err)

			testutil.RequireEqualWithOptions(t, tt.want(), converter.unique, []cmp.Option{cmp.Comparer(equalSamples)})
			require.Empty(t, converter.conflicts)
		})
	}
}

func equalSamples(a, b prompb.Sample) bool {
	// Compare Float64bits so NaN values which are exactly the same will compare equal.
	return a.Timestamp == b.Timestamp && math.Float64bits(a.Value) == math.Float64bits(b.Value)
}

func TestPrometheusConverter_AddHistogramDataPoints(t *testing.T) {
	scopeAttrs := pcommon.NewMap()
	scopeAttrs.FromRaw(map[string]any{
		"attr1": "value1",
		"attr2": "value2",
	})
	defaultScope := scope{
		name:       "test-scope",
		version:    "1.0.0",
		schemaURL:  "https://schema.com",
		attributes: scopeAttrs,
	}
	ts := pcommon.Timestamp(time.Now().UnixNano())
	tests := []struct {
		name         string
		metric       func() pmetric.Metric
		scope        scope
		promoteScope bool
		want         func() map[uint64]*prompb.TimeSeries
	}{
		{
			name: "histogram with start time and without scope promotion",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_hist")
				metric.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.Histogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(ts)
				pt.SetStartTimestamp(ts)

				return metric
			},
			scope:        defaultScope,
			promoteScope: false,
			want: func() map[uint64]*prompb.TimeSeries {
				countLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist" + createdSuffix},
				}
				infLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist_bucket"},
					{Name: model.BucketLabel, Value: "+Inf"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(countLabels): {
						Labels: countLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(infLabels): {
						Labels: infLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(ts)), Timestamp: convertTimeStamp(ts)},
						},
					},
				}
			},
		},
		{
			name: "histogram with start time and with scope promotion",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_hist")
				metric.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.Histogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(ts)
				pt.SetStartTimestamp(ts)

				return metric
			},
			scope:        defaultScope,
			promoteScope: true,
			want: func() map[uint64]*prompb.TimeSeries {
				scopeLabels := []prompb.Label{
					{
						Name:  "otel_scope_attr1",
						Value: "value1",
					},
					{
						Name:  "otel_scope_attr2",
						Value: "value2",
					},
					{
						Name:  "otel_scope_name",
						Value: defaultScope.name,
					},
					{
						Name:  "otel_scope_schema_url",
						Value: defaultScope.schemaURL,
					},
					{
						Name:  "otel_scope_version",
						Value: defaultScope.version,
					},
				}
				countLabels := append([]prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist" + countStr},
				}, scopeLabels...)
				infLabels := append([]prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist_bucket"},
					{Name: model.BucketLabel, Value: "+Inf"},
				}, scopeLabels...)
				createdLabels := append([]prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist" + createdSuffix},
				}, scopeLabels...)
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(countLabels): {
						Labels: countLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(infLabels): {
						Labels: infLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(ts)), Timestamp: convertTimeStamp(ts)},
						},
					},
				}
			},
		},
		{
			name: "histogram without start time",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_hist")
				metric.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.Histogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(ts)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist" + countStr},
				}
				infLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist_bucket"},
					{Name: model.BucketLabel, Value: "+Inf"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(infLabels): {
						Labels: infLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := tt.metric()
			converter := NewPrometheusConverter()

			err := converter.addHistogramDataPoints(
				context.Background(),
				metric.Histogram().DataPoints(),
				pcommon.NewResource(),
				Settings{
					ExportCreatedMetric:                 true,
					PromoteScopeMetadata:                tt.promoteScope,
					EnableCreatedTimestampZeroIngestion: true,
				},
				prompb.MetricMetadata{MetricFamilyName: metric.Name()},
				tt.scope,
				promslog.NewNopLogger(),
			)
			require.NoError(t, err)

			require.Equal(t, tt.want(), converter.unique)
			require.Empty(t, converter.conflicts)
		})
	}
}

func TestGetPromExemplars(t *testing.T) {
	ctx := context.Background()
	everyN := &everyNTimes{n: 1}

	t.Run("Exemplars with int value", func(t *testing.T) {
		pt := pmetric.NewNumberDataPoint()
		exemplar := pt.Exemplars().AppendEmpty()
		exemplar.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
		exemplar.SetIntValue(42)
		exemplars, err := getPromExemplars(ctx, everyN, pt)
		require.NoError(t, err)
		require.Len(t, exemplars, 1)
		require.Equal(t, float64(42), exemplars[0].Value)
	})

	t.Run("Exemplars with double value", func(t *testing.T) {
		pt := pmetric.NewNumberDataPoint()
		exemplar := pt.Exemplars().AppendEmpty()
		exemplar.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
		exemplar.SetDoubleValue(69.420)
		exemplars, err := getPromExemplars(ctx, everyN, pt)
		require.NoError(t, err)
		require.Len(t, exemplars, 1)
		require.Equal(t, 69.420, exemplars[0].Value)
	})

	t.Run("Exemplars with unsupported value type", func(t *testing.T) {
		pt := pmetric.NewNumberDataPoint()
		exemplar := pt.Exemplars().AppendEmpty()
		exemplar.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
		_, err := getPromExemplars(ctx, everyN, pt)
		require.Error(t, err)
	})
}

func TestAddTypeAndUnitLabels(t *testing.T) {
	testCases := []struct {
		name           string
		inputLabels    []prompb.Label
		metadata       prompb.MetricMetadata
		expectedLabels []prompb.Label
	}{
		{
			name: "overwrites existing type and unit labels and preserves other labels",
			inputLabels: []prompb.Label{
				{Name: "job", Value: "test-job"},
				{Name: "__type__", Value: "old_type"},
				{Name: "instance", Value: "test-instance"},
				{Name: "__unit__", Value: "old_unit"},
				{Name: "custom_label", Value: "custom_value"},
			},
			metadata: prompb.MetricMetadata{
				Type: prompb.MetricMetadata_COUNTER,
				Unit: "seconds",
			},
			expectedLabels: []prompb.Label{
				{Name: "job", Value: "test-job"},
				{Name: "instance", Value: "test-instance"},
				{Name: "custom_label", Value: "custom_value"},
				{Name: "__type__", Value: "counter"},
				{Name: "__unit__", Value: "seconds"},
			},
		},
		{
			name: "adds type and unit labels when missing",
			inputLabels: []prompb.Label{
				{Name: "job", Value: "test-job"},
				{Name: "instance", Value: "test-instance"},
			},
			metadata: prompb.MetricMetadata{
				Type: prompb.MetricMetadata_GAUGE,
				Unit: "bytes",
			},
			expectedLabels: []prompb.Label{
				{Name: "job", Value: "test-job"},
				{Name: "instance", Value: "test-instance"},
				{Name: "__type__", Value: "gauge"},
				{Name: "__unit__", Value: "bytes"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := addTypeAndUnitLabels(tc.inputLabels, tc.metadata, Settings{AllowUTF8: false})
			require.ElementsMatch(t, tc.expectedLabels, result)
		})
	}
}
