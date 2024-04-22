// DO NOT EDIT. COPIED AS-IS. SEE ../README.md

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite // import "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"

import (
	"math"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/value"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func addGaugeNumberDataPoints[TS any](converter MetricsConverter[TS], dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, settings Settings, name string) {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		labels := createLabels(
			converter,
			resource,
			pt.Attributes(),
			settings.ExternalLabels,
			nil,
			true,
			model.MetricNameLabel,
			name,
		)
		ts := ConvertTimestamp(pt.Timestamp())
		var val float64
		switch pt.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			val = float64(pt.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			val = pt.DoubleValue()
		}
		if pt.Flags().NoRecordedValue() {
			val = math.Float64frombits(value.StaleNaN)
		}
		converter.AddSample(ts, val, labels)
	}
}

func addSumNumberDataPoints[TS any](converter MetricsConverter[TS], dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, metric pmetric.Metric, settings Settings, name string) error {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		lbls := createLabels(
			converter,
			resource,
			pt.Attributes(),
			settings.ExternalLabels,
			nil,
			true,
			model.MetricNameLabel,
			name,
		)
		// convert ns to ms
		timestamp := ConvertTimestamp(pt.Timestamp())
		var val float64
		switch pt.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			val = float64(pt.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			val = pt.DoubleValue()
		}
		if pt.Flags().NoRecordedValue() {
			val = math.Float64frombits(value.StaleNaN)
		}
		ts := converter.AddSample(timestamp, val, lbls)
		converter.AddExemplars(ConvertExemplars(pt, converter), ts)

		// add created time series if needed
		if settings.ExportCreatedMetric && metric.Sum().IsMonotonic() {
			startTimestamp := pt.StartTimestamp()
			if startTimestamp == 0 {
				return nil
			}

			converter.AddTimeSeriesIfNeeded(lbls.CloneWithName(name+createdSuffix), startTimestamp, pt.Timestamp())
		}
	}
	return nil
}
