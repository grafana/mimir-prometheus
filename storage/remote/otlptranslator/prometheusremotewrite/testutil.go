package prometheusremotewrite

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/prometheus/prometheus/mimir/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/mimir/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/util/testutil"
)

type mockCombinedAppender struct {
	pendingSamples    []combinedSample
	pendingHistograms []combinedHistogram

	samples    []combinedSample
	histograms []combinedHistogram
}

type combinedSample struct {
	metricFamilyName string
	ls               labels.Labels
	meta             metadata.Metadata
	t                int64
	ct               int64
	v                float64
	es               []exemplar.Exemplar
}

type combinedHistogram struct {
	metricFamilyName string
	ls               labels.Labels
	meta             metadata.Metadata
	t                int64
	ct               int64
	h                *histogram.Histogram
	es               []exemplar.Exemplar
}

func (m *mockCombinedAppender) AppendSample(metricFamilyName string, ls labels.Labels, meta metadata.Metadata, t, ct int64, v float64, es []exemplar.Exemplar) error {
	m.pendingSamples = append(m.pendingSamples, combinedSample{
		metricFamilyName: metricFamilyName,
		ls:               ls,
		meta:             meta,
		t:                t,
		ct:               ct,
		v:                v,
		es:               es,
	})
	return nil
}

func (m *mockCombinedAppender) AppendHistogram(metricFamilyName string, ls labels.Labels, meta metadata.Metadata, t, ct int64, h *histogram.Histogram, es []exemplar.Exemplar) error {
	m.pendingHistograms = append(m.pendingHistograms, combinedHistogram{
		metricFamilyName: metricFamilyName,
		ls:               ls,
		meta:             meta,
		t:                t,
		ct:               ct,
		h:                h,
		es:               es,
	})
	return nil
}

func (m *mockCombinedAppender) Commit() error {
	m.samples = append(m.samples, m.pendingSamples...)
	m.pendingSamples = m.pendingSamples[:0]
	m.histograms = append(m.histograms, m.pendingHistograms...)
	m.pendingHistograms = m.pendingHistograms[:0]
	return nil
}

func requireEqual(t testing.TB, expected, actual interface{}, msgAndArgs ...interface{}) {
	testutil.RequireEqualWithOptions(t, expected, actual, []cmp.Option{cmp.AllowUnexported(combinedSample{}, combinedHistogram{})}, msgAndArgs...)
}