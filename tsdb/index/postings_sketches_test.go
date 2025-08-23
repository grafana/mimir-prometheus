package index

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// create a postings object to test
// added this many series with this label, expect sketch to be updated (with at _least_ that many series)
// pass empty label
// look for label values or names that don't exist

// TestLabelsValuesSketches_LabelName tests getting cardinality and value counts for label names,
// but not specific values for a given label name, for small-count cases.
func TestLabelsValuesSketches_LabelName(t *testing.T) {
	type expectedValuesForLabelName struct {
		labelName   string
		cardinality uint64
		valuesCount uint64
	}
	tests := []struct {
		name                        string
		expectedValuesForLabelNames []expectedValuesForLabelName
		seriesRefToLabels           map[storage.SeriesRef]labels.Labels
	}{
		{
			name:                        "empty postings should return no values",
			seriesRefToLabels:           map[storage.SeriesRef]labels.Labels{},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"", 0, 0}},
		},
		{
			name:                        "one series with empty label name should return no values",
			seriesRefToLabels:           map[storage.SeriesRef]labels.Labels{1: labels.FromStrings(allPostingsKey.Name, allPostingsKey.Value)},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"", 0, 0}},
		},
		{
			name:                        "empty label value contributes",
			seriesRefToLabels:           map[storage.SeriesRef]labels.Labels{1: labels.FromStrings("test", allPostingsKey.Value)},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"test", 1, 1}},
		},
		{
			name: "multiple label names on a single series",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1", "label2", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 1, 1},
				{"label2", 1, 1},
			},
		},
		{
			name: "multiple series with the same label name and value",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label1", "value1"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 2, 1},
			},
		},
		{
			name: "multiple series with the same label name and different values",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label1", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 2, 2},
			},
		},
		{
			name: "multiple series with overlapping label names",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1", "label2", "value1"),
				2: labels.FromStrings("label2", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 1, 1},
				{"label2", 2, 2},
			},
		},
		{
			name: "multiple series with non-overlapping label names",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1", "label3", "value1"),
				2: labels.FromStrings("label2", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 1, 1},
				{"label2", 1, 1},
				{"label3", 1, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewMemPostings()
			for seriesRef, ls := range tt.seriesRefToLabels {
				p.Add(seriesRef, ls)
			}
			sketches := p.LabelsValuesSketches()
			ctx := context.Background()

			for _, ev := range tt.expectedValuesForLabelNames {
				valuesCount := sketches.LabelValuesCount(ctx, ev.labelName)
				valuesCard := sketches.LabelValuesCardinality(ctx, ev.labelName)

				require.Equal(t, ev.valuesCount, valuesCount)
				require.Equal(t, ev.cardinality, valuesCard)
			}
		})
	}
}

// TestLabelsValuesSketches_LabelValue tests the cardinality calculation when looking at specific label values,
// for small-count cases.
func TestLabelsValuesSketches_LabelValue(t *testing.T) {
	type expectedValuesForLabelNameValues struct {
		labelName   string
		labelValues []string
		cardinality uint64
	}
	tests := []struct {
		name                        string
		expectedValuesForLabelNames []expectedValuesForLabelNameValues
		seriesRefToLabels           map[storage.SeriesRef]labels.Labels
	}{
		{
			name:              "empty label value should match",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{1: labels.FromStrings("test", allPostingsKey.Value)},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"test", []string{allPostingsKey.Value}, 1},
			},
		},
		{
			name: "multiple series with matching label values",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label1", "value1"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"label1", []string{"value1"}, 2},
				{"label1", []string{allPostingsKey.Value}, 0},
			},
		},
		{
			name: "multiple series with non-matching label values",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label1", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"label1", []string{"value1"}, 1},
				{"label1", []string{"value2"}, 1},
				{"label1", []string{"value1", "value2"}, 2},
				{"label2", []string{"value1", "value2"}, 0},
			},
		},
		{
			name: "multiple series with non-matching label names",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label2", "value2"),
				3: labels.FromStrings("label2", "value1"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"label1", []string{"value1"}, 1},
				{"label2", []string{"value1"}, 1},
				{"label2", []string{"value2"}, 1},
				{"label2", []string{"value1", "value2"}, 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewMemPostings()
			for seriesRef, ls := range tt.seriesRefToLabels {
				p.Add(seriesRef, ls)
			}
			sketches := p.LabelsValuesSketches()
			ctx := context.Background()

			for _, ev := range tt.expectedValuesForLabelNames {
				valuesCard := sketches.LabelValuesCardinality(ctx, ev.labelName, ev.labelValues...)
				require.Equal(t, ev.cardinality, valuesCard)
			}
		})
	}
}

// TestLabelName_ManySeries tests the accuracy of label value sketches at high volume.
// It evenly distributes 6M series across 1k labels, and expects the result to be within 60k (i.e., 1% of 6M)
// of 6000 (6M / 1k).
func TestLabelName_ManySeries(t *testing.T) {
	labelName := "test_label"
	p := NewMemPostings()
	numSeries := int(6e6)
	numLabelValues := int(1e3)
	for i := 0; i < numSeries; i++ {
		p.addFor(storage.SeriesRef(i), labels.Label{
			Name: labelName,
			// Set the label value to something [0-numLabelValues]
			Value: strconv.Itoa(i % numLabelValues),
		})
	}

	ctx := context.Background()
	s := p.LabelsValuesSketches()

	require.Equal(t, uint64(numLabelValues), s.LabelValuesCount(ctx, labelName))
	require.Equal(t, uint64(numSeries), s.LabelValuesCardinality(ctx, labelName))

	for i := 0; i < numLabelValues; i++ {
		// The cardinality for every label should be within 1% of the total number of series to the expected cardinality.
		// Technically, it should be within 1% of the total increments seen by the count-min sketch,
		// but that's more opaque to understand. The total increments seen will always be equal or greater than the number of series.
		require.InDeltaf(t, uint64(numSeries/numLabelValues), s.LabelValuesCardinality(ctx, labelName, strconv.Itoa(i)),
			float64(numSeries)*.01, // 1% of total number of series
			"Cardinality for label %d is not within %d of expected", i, numSeries/100,
		)
	}
}
