package prometheusremotewrite

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// MetricsConverter defines the interface for converters from OTel metrics.
type MetricsConverter[TS any] interface {
	// AddSample adds a sample and returns the corresponding time series.
	AddSample(int64, float64, LabelSet) TS

	AddHistogram(TS, pmetric.ExponentialHistogramDataPoint, int32, float64, uint64, BucketSpans, []int64, BucketSpans, []int64)

	AddExemplars(ExemplarSet, TS)

	AddHistogramExemplars(ExemplarSet, []BucketBoundsData[TS])

	GetOrCreateTimeSeries(lbls LabelSet) (TS, bool)

	AddTimeSeriesIfNeeded(LabelSet, pcommon.Timestamp, pcommon.Timestamp)

	AllocateLabels(int) LabelSet

	AllocateExemplars(int) ExemplarSet
}

type LabelSet interface {
	Sort()
	Range(start int, cb func(i int, name, value string) bool)
	Append(string, string) LabelSet
	CloneWithName(name string, extras ...string) LabelSet
	// Reset the length to zero.
	Reset() LabelSet
}

type ExemplarSet interface {
	Append(int64, float64, LabelSet) ExemplarSet
}

type BucketSpans interface {
	Append(int32) BucketSpans
	IncrementLastLength()
}
