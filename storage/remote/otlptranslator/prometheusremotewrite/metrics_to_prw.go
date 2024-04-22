// DO NOT EDIT. COPIED AS-IS. SEE ../README.md

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite // import "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"

import (
	"cmp"
	"slices"
	"strconv"

	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Settings struct {
	Namespace           string
	ExternalLabels      map[string]string
	DisableTargetInfo   bool
	ExportCreatedMetric bool
	AddMetricSuffixes   bool
	SendMetadata        bool
}

// FromMetrics converts pmetric.Metrics to Prometheus remote write format.
//
// Deprecated: FromMetrics exists for historical compatibility and should not be used.
// Use instead PrometheusConverter.FromMetrics() and then PrometheusConverter.TimeSeries()
// to obtain the Prometheus remote write format metrics.
func FromMetrics(md pmetric.Metrics, settings Settings) (map[string]*prompb.TimeSeries, error) {
	c := NewPrometheusConverter()
	errs := c.FromMetrics(md, settings)
	tss := c.TimeSeries()
	out := make(map[string]*prompb.TimeSeries, len(tss))
	for i := range tss {
		out[strconv.Itoa(i)] = &tss[i]
	}

	return out, errs
}

// PrometheusConverter converts from OTel write format to Prometheus write format.
type PrometheusConverter struct {
	unique    map[uint64]*prompb.TimeSeries
	conflicts map[uint64][]*prompb.TimeSeries
}

func NewPrometheusConverter() *PrometheusConverter {
	return &PrometheusConverter{
		unique:    map[uint64]*prompb.TimeSeries{},
		conflicts: map[uint64][]*prompb.TimeSeries{},
	}
}

// FromMetrics converts pmetric.Metrics to Prometheus remote write format.
func (c *PrometheusConverter) FromMetrics(md pmetric.Metrics, settings Settings) error {
	return ConvertMetrics[*prompb.TimeSeries](md, settings, c)
}

// TimeSeries returns a slice of the prompb.TimeSeries that were converted from OTel format.
func (c *PrometheusConverter) TimeSeries() []prompb.TimeSeries {
	conflicts := 0
	for _, ts := range c.conflicts {
		conflicts += len(ts)
	}
	allTS := make([]prompb.TimeSeries, 0, len(c.unique)+conflicts)
	for _, ts := range c.unique {
		allTS = append(allTS, *ts)
	}
	for _, cTS := range c.conflicts {
		for _, ts := range cTS {
			allTS = append(allTS, *ts)
		}
	}

	return allTS
}

func isSameMetric(ts *prompb.TimeSeries, lbls labelsAdapter) bool {
	if len(ts.Labels) != len(lbls) {
		return false
	}
	for i, l := range ts.Labels {
		if l.Name != ts.Labels[i].Name || l.Value != ts.Labels[i].Value {
			return false
		}
	}
	return true
}

// AddHistogramExemplars adds exemplars for the dataPoint. For each exemplar, if it can find a bucket bound corresponding to its value,
// the exemplar is added to the bucket bound's time series, provided that the time series' has samples.
func (c *PrometheusConverter) AddHistogramExemplars(exemplars ExemplarSet, bucketBounds []BucketBoundsData[*prompb.TimeSeries]) {
	if len(bucketBounds) == 0 {
		return
	}

	if len(exemplars.(promExemplars)) == 0 {
		return
	}

	slices.SortFunc(bucketBounds, func(a, b BucketBoundsData[*prompb.TimeSeries]) int {
		return cmp.Compare(a.Bound, b.Bound)
	})
	for _, exemplar := range exemplars.(promExemplars) {
		for _, bound := range bucketBounds {
			if len(bound.TS.Samples) > 0 && exemplar.Value <= bound.Bound {
				bound.TS.Exemplars = append(bound.TS.Exemplars, exemplar)
				break
			}
		}
	}
}

func (c *PrometheusConverter) AddExemplars(exemplars ExemplarSet, ts *prompb.TimeSeries) {
	ts.Exemplars = append(ts.Exemplars, exemplars.(promExemplars)...)
}

// AddSample finds a TimeSeries that corresponds to lbls, and adds a sample with timestamp and value to it.
// If there is no corresponding TimeSeries already, it's created.
// The corresponding TimeSeries is returned.
// If either lbls is nil/empty or sample is nil, nothing is done.
func (c *PrometheusConverter) AddSample(timestamp int64, value float64, lbls LabelSet) *prompb.TimeSeries {
	if len(lbls.(labelsAdapter)) == 0 {
		// This shouldn't happen
		return nil
	}

	ts, _ := c.GetOrCreateTimeSeries(lbls)
	ts.Samples = append(ts.Samples, prompb.Sample{
		Timestamp: timestamp,
		Value:     value,
	})
	return ts
}
