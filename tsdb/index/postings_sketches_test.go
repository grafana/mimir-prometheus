package index

import (
	"context"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"testing"
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
		seriesRefToLabels           map[storage.SeriesRef][]labels.Label
	}{
		{
			name:                        "empty postings should return no values",
			seriesRefToLabels:           map[storage.SeriesRef][]labels.Label{},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"", 0, 0}},
		},
		{
			name:                        "one series with empty label name should return no values",
			seriesRefToLabels:           map[storage.SeriesRef][]labels.Label{1: {{allPostingsKey.Name, allPostingsKey.Value}}},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"", 0, 0}},
		},
		{
			name:                        "empty label value contributes",
			seriesRefToLabels:           map[storage.SeriesRef][]labels.Label{1: {{"test", allPostingsKey.Value}}},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"test", 1, 1}},
		},
		{
			name: "multiple label names on a single series",
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{
				1: {
					{"label1", "value1"},
					{"label2", "value2"},
				}},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 1, 1},
				{"label2", 1, 1},
			},
		},
		{
			name: "multiple series with the same label name and value",
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{
				1: {{"label1", "value1"}},
				2: {{"label1", "value1"}},
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 2, 1},
			},
		},
		{
			name: "multiple series with the same label name and different values",
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{
				1: {{"label1", "value1"}},
				2: {{"label1", "value2"}},
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 2, 2},
			},
		},
		{
			name: "multiple series with overlapping label names",
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{
				1: {{"label1", "value1"}, {"label2", "value1"}},
				2: {{"label2", "value2"}},
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 1, 1},
				{"label2", 2, 2},
			},
		},
		{
			name: "multiple series with non-overlapping label names",
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{
				1: {{"label1", "value1"}, {"label3", "value1"}},
				2: {{"label2", "value2"}},
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
				for _, l := range ls {
					p.addFor(seriesRef, l)
				}
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
// for small-count cases
func TestLabelsValuesSketches_LabelValue(t *testing.T) {
	type expectedValuesForLabelNameValues struct {
		labelName   string
		labelValues []string
		cardinality uint64
	}
	tests := []struct {
		name                        string
		expectedValuesForLabelNames []expectedValuesForLabelNameValues
		seriesRefToLabels           map[storage.SeriesRef][]labels.Label
	}{
		{
			name:              "empty label value should match",
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{1: {{"test", allPostingsKey.Value}}},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"test", []string{allPostingsKey.Value}, 1},
			},
		},
		{
			name: "multiple series with matching label values",
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{
				1: {{"label1", "value1"}},
				2: {{"label1", "value1"}},
			},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"label1", []string{"value1"}, 2},
				{"label1", []string{allPostingsKey.Value}, 0},
			},
		},
		{
			name: "multiple series with non-matching label values",
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{
				1: {{"label1", "value1"}},
				2: {{"label1", "value2"}},
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
			seriesRefToLabels: map[storage.SeriesRef][]labels.Label{
				1: {{"label1", "value1"}},
				2: {{"label2", "value2"}},
				3: {{"label2", "value1"}},
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
				for _, l := range ls {
					p.addFor(seriesRef, l)
				}
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
