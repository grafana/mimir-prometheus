package tsdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
)

type tsValue struct {
	Ts                 int64
	V                  int64
	CounterResetHeader histogram.CounterResetHint
}

type sampleTypeScenario struct {
	sampleType string
	appendFunc func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, error, sample)
	sampleFunc func(ts, value int64) sample
}

var sampleTypeScenarios = map[string]sampleTypeScenario{
	"float": {
		sampleType: sampleMetricTypeFloat,
		appendFunc: func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, error, sample) {
			s := sample{t: ts, f: float64(value)}
			ref, err := appender.Append(0, lbls, ts, s.f)
			return ref, err, s
		},
		sampleFunc: func(ts, value int64) sample {
			return sample{t: ts, f: float64(value)}
		},
	},
	"integer histogram": {
		sampleType: sampleMetricTypeHistogram,
		appendFunc: func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, error, sample) {
			s := sample{t: ts, h: tsdbutil.GenerateTestHistogram(int(value))}
			ref, err := appender.AppendHistogram(0, lbls, ts, s.h, nil)
			return ref, err, s
		},
		sampleFunc: func(ts, value int64) sample {
			return sample{t: ts, h: tsdbutil.GenerateTestHistogram(int(value))}
		},
	},
	"float histogram": {
		sampleType: sampleMetricTypeHistogram,
		appendFunc: func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, error, sample) {
			s := sample{t: ts, fh: tsdbutil.GenerateTestFloatHistogram(int(value))}
			ref, err := appender.AppendHistogram(0, lbls, ts, nil, s.fh)
			return ref, err, s
		},
		sampleFunc: func(ts, value int64) sample {
			return sample{t: ts, fh: tsdbutil.GenerateTestFloatHistogram(int(value))}
		},
	},
}

// requireEqualSamples checks that the actual series are equal to the expected ones. It ignores the counter reset hints for histograms.
// TODO(fionaliao): understand counter reset behaviour, might want to unignore hints later
func requireEqualSamples(t *testing.T, expected, actual map[string][]chunks.Sample) {
	for name, expectedItem := range expected {
		actualItem, ok := actual[name]
		require.True(t, ok, "Expected series %s not found", name)
		require.Equal(t, len(expectedItem), len(actualItem), "Length not expected for %s", name)
		for i, s := range expectedItem {
			expectedSample := s
			actualSample := actualItem[i]
			require.Equal(t, expectedSample.Type().String(), actualSample.Type().String(), "Different types for %s[%d]", name, i)
			switch {
			case s.H() != nil:
				expectedHist := expectedSample.H()
				actualHist := actualSample.H()
				expectedHist.CounterResetHint = histogram.UnknownCounterReset
				actualHist.CounterResetHint = histogram.UnknownCounterReset
				require.Equal(t, expectedHist, actualHist, "Unexpected sample for %s[%d]", name, i)
			case s.FH() != nil:
				expectedHist := expectedSample.FH()
				actualHist := actualSample.FH()
				expectedHist.CounterResetHint = histogram.UnknownCounterReset
				actualHist.CounterResetHint = histogram.UnknownCounterReset
				require.Equal(t, expectedHist, actualHist, "Unexpected sample for %s[%d]", name, i)
			default:
				require.Equal(t, expectedSample, expectedSample, "Unexpected sample for %s[%d]", name, i)
			}
		}
	}
	for name := range actual {
		_, ok := expected[name]
		require.True(t, ok, "Unexpected series %s", name)
	}
}
