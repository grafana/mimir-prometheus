// DO NOT EDIT. COPIED AS-IS. SEE ../README.md

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite // import "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"

import (
	"fmt"
	"math"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const DefaultZeroThreshold = 1e-128

func addExponentialHistogramDataPoints[TS any](converter MetricsConverter[TS], dataPoints pmetric.ExponentialHistogramDataPointSlice,
	resource pcommon.Resource, settings Settings, baseName string) error {
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
			baseName,
		)
		ts, _ := converter.GetOrCreateTimeSeries(lbls)
		if err := convertExponentialHistogram(pt, ts, converter); err != nil {
			return err
		}

		converter.AddExemplars(ConvertExemplars(pt, converter), ts)
	}

	return nil
}

// convertExponentialHistogram translates an OTel Exponential Histogram data point
// to a native histogram and adds it to ts.
func convertExponentialHistogram[TS any](p pmetric.ExponentialHistogramDataPoint, ts TS, converter MetricsConverter[TS]) error {
	scale := p.Scale()
	if scale < -4 {
		return fmt.Errorf("cannot convert exponential to native histogram."+
			" Scale must be >= -4, was %d", scale)
	}

	var scaleDown int32
	if scale > 8 {
		scaleDown = scale - 8
		scale = 8
	}

	pSpans, pDeltas := convertBucketsLayout(converter, p.Positive(), scaleDown)
	nSpans, nDeltas := convertBucketsLayout(converter, p.Negative(), scaleDown)

	var sum float64
	var count uint64
	if p.Flags().NoRecordedValue() {
		sum = math.Float64frombits(value.StaleNaN)
		count = value.StaleNaN
	} else {
		if p.HasSum() {
			sum = p.Sum()
		}
		count = p.Count()
	}

	converter.AddHistogram(ts, p, scale, sum, count, pSpans, pDeltas, nSpans, nDeltas)
	return nil
}

// convertBucketsLayout translates OTel Exponential Histogram dense buckets
// representation to Prometheus Native Histogram sparse bucket representation.
//
// The translation logic is taken from the client_golang `histogram.go#makeBuckets`
// function, see `makeBuckets` https://github.com/prometheus/client_golang/blob/main/prometheus/histogram.go
// The bucket indexes conversion was adjusted, since OTel exp. histogram bucket
// index 0 corresponds to the range (1, base] while Prometheus bucket index 0
// to the range (base 1].
//
// scaleDown is the factor by which the buckets are scaled down. In other words 2^scaleDown buckets will be merged into one.
func convertBucketsLayout[TS any](converter MetricsConverter[TS], buckets pmetric.ExponentialHistogramDataPointBuckets, scaleDown int32) (BucketSpans, []int64) {
	bucketCounts := buckets.BucketCounts()
	if bucketCounts.Len() == 0 {
		return nil, nil
	}

	var (
		spans     BucketSpans
		deltas    []int64
		count     int64
		prevCount int64
	)

	appendDelta := func(count int64) {
		spans.IncrementLastLength()
		deltas = append(deltas, count-prevCount)
		prevCount = count
	}

	// Let the compiler figure out that this is const during this function by
	// moving it into a local variable.
	numBuckets := bucketCounts.Len()

	// The offset is scaled and adjusted by 1 as described above.
	bucketIdx := buckets.Offset()>>scaleDown + 1
	spans = spans.Append(bucketIdx)

	for i := 0; i < numBuckets; i++ {
		// The offset is scaled and adjusted by 1 as described above.
		nextBucketIdx := (int32(i)+buckets.Offset())>>scaleDown + 1
		if bucketIdx == nextBucketIdx { // We have not collected enough buckets to merge yet.
			count += int64(bucketCounts.At(i))
			continue
		}
		if count == 0 {
			count = int64(bucketCounts.At(i))
			continue
		}

		gap := nextBucketIdx - bucketIdx - 1
		if gap > 2 {
			// We have to create a new span, because we have found a gap
			// of more than two buckets. The constant 2 is copied from the logic in
			// https://github.com/prometheus/client_golang/blob/27f0506d6ebbb117b6b697d0552ee5be2502c5f2/prometheus/histogram.go#L1296
			spans = spans.Append(gap)
		} else {
			// We have found a small gap (or no gap at all).
			// Insert empty buckets as needed.
			for j := int32(0); j < gap; j++ {
				appendDelta(0)
			}
		}
		appendDelta(count)
		count = int64(bucketCounts.At(i))
		bucketIdx = nextBucketIdx
	}
	// Need to use the last item's index. The offset is scaled and adjusted by 1 as described above.
	gap := (int32(numBuckets)+buckets.Offset()-1)>>scaleDown + 1 - bucketIdx
	if gap > 2 {
		// We have to create a new span, because we have found a gap
		// of more than two buckets. The constant 2 is copied from the logic in
		// https://github.com/prometheus/client_golang/blob/27f0506d6ebbb117b6b697d0552ee5be2502c5f2/prometheus/histogram.go#L1296
		spans = spans.Append(gap)
	} else {
		// We have found a small gap (or no gap at all).
		// Insert empty buckets as needed.
		for j := int32(0); j < gap; j++ {
			appendDelta(0)
		}
	}
	appendDelta(count)

	return spans, deltas
}

func (c *PrometheusConverter) AddHistogram(ts *prompb.TimeSeries, p pmetric.ExponentialHistogramDataPoint, scale int32, sum float64, count uint64, pSpans BucketSpans, pDeltas []int64, nSpans BucketSpans, nDeltas []int64) {
	ts.Histograms = append(ts.Histograms, prompb.Histogram{
		// The counter reset detection must be compatible with Prometheus to
		// safely set ResetHint to NO. This is not ensured currently.
		// Sending a sample that triggers counter reset but with ResetHint==NO
		// would lead to Prometheus panic as it does not double check the hint.
		// Thus we're explicitly saying UNKNOWN here, which is always safe.
		// TODO: using created time stamp should be accurate, but we
		// need to know here if it was used for the detection.
		// Ref: https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/28663#issuecomment-1810577303
		// Counter reset detection in Prometheus: https://github.com/prometheus/prometheus/blob/f997c72f294c0f18ca13fa06d51889af04135195/tsdb/chunkenc/histogram.go#L232
		ResetHint: prompb.Histogram_UNKNOWN,
		Schema:    scale,

		ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: p.ZeroCount()},
		// TODO use zero_threshold, if set, see
		// https://github.com/open-telemetry/opentelemetry-proto/pull/441
		ZeroThreshold: DefaultZeroThreshold,

		PositiveSpans:  pSpans.(promBucketSpans),
		PositiveDeltas: pDeltas,
		NegativeSpans:  nSpans.(promBucketSpans),
		NegativeDeltas: nDeltas,

		Timestamp: ConvertTimestamp(p.Timestamp()),
	})
}

type promBucketSpans []prompb.BucketSpan

func (bs promBucketSpans) Append(offset int32) BucketSpans {
	return append(bs, prompb.BucketSpan{
		Offset: offset,
	})
}

func (bs promBucketSpans) IncrementLastLength() {
	bs[len(bs)-1].Length++
}
