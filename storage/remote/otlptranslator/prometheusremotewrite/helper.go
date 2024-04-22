// DO NOT EDIT. COPIED AS-IS. SEE ../README.md

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite // import "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"

import (
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"

	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
)

const (
	sumStr        = "_sum"
	countStr      = "_count"
	bucketStr     = "_bucket"
	leStr         = "le"
	quantileStr   = "quantile"
	pInfStr       = "+Inf"
	createdSuffix = "_created"
	// maxExemplarRunes is the maximum number of UTF-8 exemplar characters
	// according to the prometheus specification
	// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars
	maxExemplarRunes = 128
	// Trace and Span id keys are defined as part of the spec:
	// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification%2Fmetrics%2Fdatamodel.md#exemplars-2
	traceIDKey       = "trace_id"
	spanIDKey        = "span_id"
	infoType         = "info"
	targetMetricName = "target_info"
)

type BucketBoundsData[TS any] struct {
	TS    TS
	Bound float64
}

// TimeSeriesSignature returns a hashed label set signature.
// The label slice should not contain duplicate label names; this method sorts the slice by label name before creating
// the signature.
// The algorithm is the same as in Prometheus' labels.StableHash function.
func TimeSeriesSignature(labels LabelSet) uint64 {
	labels.Sort()

	// Use xxhash.Sum64(b) for fast path as it's faster.
	b := make([]byte, 0, 1024)
	var h *xxhash.Digest
	labels.Range(0, func(i int, name, value string) bool {
		if len(b)+len(name)+len(value)+2 >= cap(b) {
			// If labels entry is 1KB+ do not allocate whole entry.
			h = xxhash.New()
			_, _ = h.Write(b)
			labels.Range(i, func(_ int, name, value string) bool {
				_, _ = h.WriteString(name)
				_, _ = h.Write(seps)
				_, _ = h.WriteString(value)
				_, _ = h.Write(seps)
				return true
			})
			return false
		}

		b = append(b, name...)
		b = append(b, seps[0])
		b = append(b, value...)
		b = append(b, seps[0])
		return true
	})
	if h != nil {
		return h.Sum64()
	}
	return xxhash.Sum64(b)
}

var seps = []byte{'\xff'}

// createLabels creates a slice of Prometheus Labels with OTLP attributes and pairs of string values.
// Unpaired string values are ignored. String pairs overwrite OTLP labels if collisions happen and
// if logOnOverwrite is true, the overwrite is logged. Resulting label names are sanitized.
func createLabels[TS any](converter MetricsConverter[TS], resource pcommon.Resource, attributes pcommon.Map, externalLabels map[string]string,
	ignoreAttrs []string, logOnOverwrite bool, extras ...string) LabelSet {
	resourceAttrs := resource.Attributes()
	serviceName, haveServiceName := resourceAttrs.Get(conventions.AttributeServiceName)
	instance, haveInstanceID := resourceAttrs.Get(conventions.AttributeServiceInstanceID)

	// Calculate the maximum possible number of labels we could return so we can preallocate l
	maxLabelCount := attributes.Len() + len(externalLabels) + len(extras)/2
	if haveServiceName {
		maxLabelCount++
	}
	if haveInstanceID {
		maxLabelCount++
	}

	// map ensures no duplicate label name
	l := make(map[string]string, maxLabelCount)

	labels := converter.AllocateLabels(maxLabelCount)
	// XXX: Should we always drop service namespace/service name/service instance ID from the labels
	// (as they get mapped to other Prometheus labels)?
	attributes.Range(func(key string, value pcommon.Value) bool {
		if !slices.Contains(ignoreAttrs, key) {
			labels = labels.Append(key, value.AsString())
		}
		return true
	})
	// Ensure attributes are sorted by key for consistent merging of keys which
	// collide when sanitized.
	labels.Sort()

	labels.Range(0, func(_ int, name, value string) bool {
		finalKey := prometheustranslator.NormalizeLabel(name)
		if existingValue, alreadyExists := l[finalKey]; alreadyExists {
			l[finalKey] = existingValue + ";" + value
		} else {
			l[finalKey] = value
		}

		return true
	})

	// Map service.name + service.namespace to job
	if haveServiceName {
		val := serviceName.AsString()
		if serviceNamespace, ok := resourceAttrs.Get(conventions.AttributeServiceNamespace); ok {
			val = fmt.Sprintf("%s/%s", serviceNamespace.AsString(), val)
		}
		l[model.JobLabel] = val
	}
	// Map service.instance.id to instance
	if haveInstanceID {
		l[model.InstanceLabel] = instance.AsString()
	}
	for key, value := range externalLabels {
		// External labels have already been sanitized
		if _, alreadyExists := l[key]; alreadyExists {
			// Skip external labels if they are overridden by metric attributes
			continue
		}
		l[key] = value
	}

	for i := 0; i < len(extras); i += 2 {
		if i+1 >= len(extras) {
			break
		}
		_, found := l[extras[i]]
		if found && logOnOverwrite {
			log.Println("label " + extras[i] + " is overwritten. Check if Prometheus reserved labels are used.")
		}
		// internal labels should be maintained
		name := extras[i]
		if !(len(name) > 4 && name[:2] == "__" && name[len(name)-2:] == "__") {
			name = prometheustranslator.NormalizeLabel(name)
		}
		l[name] = extras[i+1]
	}

	labels = labels.Reset()
	for k, v := range l {
		labels = labels.Append(k, v)
	}
	return labels
}

// isValidAggregationTemporality checks whether an OTel metric has a valid
// aggregation temporality for conversion to a Prometheus metric.
func isValidAggregationTemporality(metric pmetric.Metric) bool {
	//exhaustive:enforce
	switch metric.Type() {
	case pmetric.MetricTypeGauge, pmetric.MetricTypeSummary:
		return true
	case pmetric.MetricTypeSum:
		return metric.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeHistogram:
		return metric.Histogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeExponentialHistogram:
		return metric.ExponentialHistogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	}
	return false
}

func addHistogramDataPoints[TS any](converter MetricsConverter[TS], dataPoints pmetric.HistogramDataPointSlice,
	resource pcommon.Resource, settings Settings, baseName string) {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		timestamp := ConvertTimestamp(pt.Timestamp())
		baseLabels := createLabels(converter, resource, pt.Attributes(), settings.ExternalLabels, nil, false)

		// If the sum is unset, it indicates the _sum metric point should be
		// omitted
		if pt.HasSum() {
			// treat sum as a sample in an individual TimeSeries
			val := pt.Sum()
			if pt.Flags().NoRecordedValue() {
				val = math.Float64frombits(value.StaleNaN)
			}

			sumlabels := baseLabels.CloneWithName(baseName + sumStr)
			converter.AddSample(timestamp, val, sumlabels)

		}

		// treat count as a sample in an individual TimeSeries
		val := float64(pt.Count())
		if pt.Flags().NoRecordedValue() {
			val = math.Float64frombits(value.StaleNaN)
		}

		countlabels := baseLabels.CloneWithName(baseName + countStr)
		converter.AddSample(timestamp, val, countlabels)

		// cumulative count for conversion to cumulative histogram
		var cumulativeCount uint64

		var bucketBounds []BucketBoundsData[TS]

		// process each bound, based on histograms proto definition, # of buckets = # of explicit bounds + 1
		for i := 0; i < pt.ExplicitBounds().Len() && i < pt.BucketCounts().Len(); i++ {
			bound := pt.ExplicitBounds().At(i)
			cumulativeCount += pt.BucketCounts().At(i)
			val := float64(cumulativeCount)
			if pt.Flags().NoRecordedValue() {
				val = math.Float64frombits(value.StaleNaN)
			}
			boundStr := strconv.FormatFloat(bound, 'f', -1, 64)
			labels := baseLabels.CloneWithName(baseName+bucketStr, leStr, boundStr)
			ts := converter.AddSample(timestamp, val, labels)

			bucketBounds = append(bucketBounds, BucketBoundsData[TS]{TS: ts, Bound: bound})
		}
		// add le=+Inf bucket
		if pt.Flags().NoRecordedValue() {
			val = math.Float64frombits(value.StaleNaN)
		} else {
			val = float64(pt.Count())
		}
		infLabels := baseLabels.CloneWithName(baseName+bucketStr, leStr, pInfStr)
		ts := converter.AddSample(timestamp, val, infLabels)

		bucketBounds = append(bucketBounds, BucketBoundsData[TS]{TS: ts, Bound: math.Inf(1)})
		converter.AddHistogramExemplars(ConvertExemplars(pt, converter), bucketBounds)

		startTimestamp := pt.StartTimestamp()
		if settings.ExportCreatedMetric && startTimestamp != 0 {
			labels := baseLabels.CloneWithName(baseName + createdSuffix)
			converter.AddTimeSeriesIfNeeded(labels, startTimestamp, pt.Timestamp())
		}
	}
}

type ExemplarType interface {
	pmetric.ExponentialHistogramDataPoint | pmetric.HistogramDataPoint | pmetric.NumberDataPoint
	Exemplars() pmetric.ExemplarSlice
}

// ConvertExemplars converts pt's exemplars to an ExemplarSet.
func ConvertExemplars[OT ExemplarType, TS any](pt OT, converter MetricsConverter[TS]) ExemplarSet {
	exemplars := converter.AllocateExemplars(pt.Exemplars().Len())
	for i := 0; i < pt.Exemplars().Len(); i++ {
		exemplar := pt.Exemplars().At(i)
		exemplarRunes := 0

		lbls := converter.AllocateLabels(0)
		if traceID := exemplar.TraceID(); !traceID.IsEmpty() {
			val := hex.EncodeToString(traceID[:])
			exemplarRunes += utf8.RuneCountInString(traceIDKey) + utf8.RuneCountInString(val)
			lbls = lbls.Append(traceIDKey, val)
		}
		if spanID := exemplar.SpanID(); !spanID.IsEmpty() {
			val := hex.EncodeToString(spanID[:])
			exemplarRunes += utf8.RuneCountInString(spanIDKey) + utf8.RuneCountInString(val)
			lbls = lbls.Append(spanIDKey, val)
		}

		attrs := exemplar.FilteredAttributes()
		labelsFromAttributes := converter.AllocateLabels(attrs.Len())
		attrs.Range(func(key string, value pcommon.Value) bool {
			val := value.AsString()
			exemplarRunes += utf8.RuneCountInString(key) + utf8.RuneCountInString(val)
			labelsFromAttributes = labelsFromAttributes.Append(key, val)

			return true
		})
		if exemplarRunes <= maxExemplarRunes {
			// only append filtered attributes if it does not cause exemplar
			// labels to exceed the max number of runes
			labelsFromAttributes.Range(0, func(_ int, name, value string) bool {
				lbls = lbls.Append(name, value)
				return true
			})
		}

		exemplars = exemplars.Append(timestamp.FromTime(exemplar.Timestamp().AsTime()), exemplar.DoubleValue(), lbls)
	}

	return exemplars
}

// MostRecentTimestampInMetric returns the latest timestamp in a batch of metrics
func MostRecentTimestampInMetric(metric pmetric.Metric) pcommon.Timestamp {
	var ts pcommon.Timestamp
	// handle individual metric based on type
	//exhaustive:enforce
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dataPoints := metric.Gauge().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	case pmetric.MetricTypeSum:
		dataPoints := metric.Sum().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	case pmetric.MetricTypeHistogram:
		dataPoints := metric.Histogram().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	case pmetric.MetricTypeExponentialHistogram:
		dataPoints := metric.ExponentialHistogram().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	case pmetric.MetricTypeSummary:
		dataPoints := metric.Summary().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	}
	return ts
}

func addSummaryDataPoints[TS any](converter MetricsConverter[TS], dataPoints pmetric.SummaryDataPointSlice, resource pcommon.Resource,
	settings Settings, baseName string) error {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		timestamp := ConvertTimestamp(pt.Timestamp())
		baseLabels := createLabels(converter, resource, pt.Attributes(), settings.ExternalLabels, nil, false)

		// treat sum as a sample in an individual TimeSeries
		val := pt.Sum()
		if pt.Flags().NoRecordedValue() {
			val = math.Float64frombits(value.StaleNaN)
		}
		// sum and count of the summary should append suffix to baseName
		sumlabels := baseLabels.CloneWithName(baseName + sumStr)
		converter.AddSample(timestamp, val, sumlabels)

		// treat count as a sample in an individual TimeSeries
		val = float64(pt.Count())
		if pt.Flags().NoRecordedValue() {
			val = math.Float64frombits(value.StaleNaN)
		}
		countlabels := baseLabels.CloneWithName(baseName + countStr)
		converter.AddSample(timestamp, val, countlabels)

		// process each percentile/quantile
		for i := 0; i < pt.QuantileValues().Len(); i++ {
			qt := pt.QuantileValues().At(i)
			val := qt.Value()
			if pt.Flags().NoRecordedValue() {
				val = math.Float64frombits(value.StaleNaN)
			}
			percentileStr := strconv.FormatFloat(qt.Quantile(), 'f', -1, 64)
			qtlabels := baseLabels.CloneWithName(baseName, quantileStr, percentileStr)
			converter.AddSample(timestamp, val, qtlabels)
		}

		startTimestamp := pt.StartTimestamp()
		if settings.ExportCreatedMetric && startTimestamp != 0 {
			createdLabels := baseLabels.CloneWithName(baseName + createdSuffix)
			converter.AddTimeSeriesIfNeeded(createdLabels, startTimestamp, pt.Timestamp())
		}
	}

	return nil
}

type labelsAdapter []prompb.Label

func (ls labelsAdapter) Sort() {
	slices.SortStableFunc(ls, func(a, b prompb.Label) int {
		return strings.Compare(a.Name, b.Name)
	})
}

func (ls labelsAdapter) Range(start int, cb func(i int, name, value string) bool) {
	for i := start; i < len(ls); i++ {
		l := ls[i]
		if !cb(i, l.Name, l.Value) {
			break
		}
	}
}

func (ls labelsAdapter) Append(name, value string) LabelSet {
	return append(ls, prompb.Label{Name: name, Value: value})
}

func (ls labelsAdapter) Reset() LabelSet {
	return ls[:0]
}

type promExemplars []prompb.Exemplar

func (es promExemplars) Append(timestamp int64, val float64, lbls LabelSet) ExemplarSet {
	return append(es, prompb.Exemplar{Timestamp: timestamp, Value: val, Labels: lbls.(labelsAdapter)})
}

// CloneWithName returns a copy of ls, appending to it the pair model.MetricNameLabel=name.
// If extras are provided, corresponding label pairs are also added to the returned slice.
// If extras is uneven length, the last (unpaired) extra will be ignored.
func (ls labelsAdapter) CloneWithName(name string, extras ...string) LabelSet {
	nameIdx := -1
	nameCount := 1
	for i, l := range ls {
		if l.Name == model.MetricNameLabel {
			nameIdx = i
			// Name already exists
			nameCount = 0
			break
		}
	}

	extraLabelCount := len(extras) / 2
	labels := make(labelsAdapter, len(ls), len(ls)+extraLabelCount+nameCount)
	copy(labels, ls)

	n := len(extras)
	n -= n % 2
	for extrasIdx := 0; extrasIdx < n; extrasIdx += 2 {
		labels = append(labels, prompb.Label{Name: extras[extrasIdx], Value: extras[extrasIdx+1]})
	}

	if nameIdx == -1 {
		return append(labels, prompb.Label{Name: model.MetricNameLabel, Value: name})
	}

	labels[nameIdx].Value = name
	return labels
}

// GetOrCreateTimeSeries returns the time series corresponding to the label set if existent, and false.
// Otherwise it creates a new one and returns that, and true.
func (c *PrometheusConverter) GetOrCreateTimeSeries(lbls LabelSet) (*prompb.TimeSeries, bool) {
	h := TimeSeriesSignature(lbls)
	ts := c.unique[h]
	if ts != nil {
		if isSameMetric(ts, lbls.(labelsAdapter)) {
			// We already have this metric
			return ts, false
		}

		// Look for a matching conflict
		for _, cTS := range c.conflicts[h] {
			if isSameMetric(cTS, lbls.(labelsAdapter)) {
				// We already have this metric
				return cTS, false
			}
		}

		// New conflict
		ts = &prompb.TimeSeries{
			Labels: lbls.(labelsAdapter),
		}
		c.conflicts[h] = append(c.conflicts[h], ts)
		return ts, true
	}

	// This metric is new
	ts = &prompb.TimeSeries{
		Labels: lbls.(labelsAdapter),
	}
	c.unique[h] = ts
	return ts, true
}

// AddTimeSeriesIfNeeded adds a corresponding time series if it doesn't already exist.
// If the time series doesn't already exist, it gets added with startTimestamp for its value and timestamp for its timestamp,
// both converted to milliseconds.
func (c *PrometheusConverter) AddTimeSeriesIfNeeded(lbls LabelSet, startTimestamp pcommon.Timestamp, timestamp pcommon.Timestamp) {
	ts, created := c.GetOrCreateTimeSeries(lbls.(labelsAdapter))
	if created {
		ts.Samples = []prompb.Sample{
			{
				// convert ns to ms
				Value:     float64(ConvertTimestamp(startTimestamp)),
				Timestamp: ConvertTimestamp(timestamp),
			},
		}
	}
}

func (c *PrometheusConverter) AllocateLabels(capacity int) LabelSet {
	return make(labelsAdapter, 0, capacity)
}

func (c *PrometheusConverter) AllocateExemplars(capacity int) ExemplarSet {
	return make(promExemplars, 0, capacity)
}

// addResourceTargetInfo converts the resource to the target info metric.
func addResourceTargetInfo[TS any](resource pcommon.Resource, settings Settings, timestamp pcommon.Timestamp, converter MetricsConverter[TS]) {
	if settings.DisableTargetInfo {
		return
	}

	attributes := resource.Attributes()
	identifyingAttrs := []string{
		conventions.AttributeServiceNamespace,
		conventions.AttributeServiceName,
		conventions.AttributeServiceInstanceID,
	}
	nonIdentifyingAttrsCount := attributes.Len()
	for _, a := range identifyingAttrs {
		_, haveAttr := attributes.Get(a)
		if haveAttr {
			nonIdentifyingAttrsCount--
		}
	}
	if nonIdentifyingAttrsCount == 0 {
		// If we only have job + instance, then target_info isn't useful, so don't add it.
		return
	}

	name := targetMetricName
	if len(settings.Namespace) > 0 {
		name = settings.Namespace + "_" + name
	}

	labels := createLabels(converter, resource, attributes, settings.ExternalLabels, identifyingAttrs, false, model.MetricNameLabel, name)
	converter.AddSample(ConvertTimestamp(timestamp), 1, labels)
}

// ConvertTimestamp converts OTLP timestamp in ns to timestamp in ms
func ConvertTimestamp(timestamp pcommon.Timestamp) int64 {
	return timestamp.AsTime().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}
