// Copyright 2025 Grafana Labs
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

package index

import (
	"context"
	"math"
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
// It evenly distributes 6M series across 1k labels,
// and expects the result to be within 30k (i.e., 0.5% or countMinEpsilon of 6M) of 6000 (6M / 1k).
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
		// The cardinality for every label should be within epsilon of the total number of series to the expected cardinality.
		// Technically, it should be within epsilon of the total increments seen by the count-min sketch,
		// but that's more opaque to understand. The total increments seen will always be equal or greater than the number of series.
		require.InDeltaf(t, uint64(numSeries/numLabelValues), s.LabelValuesCardinality(ctx, labelName, strconv.Itoa(i)),
			float64(numSeries)*countMinEpsilon,
			"Cardinality for label %d is not within %d of expected", i, float64(numSeries)*countMinEpsilon,
		)
	}
}

// TestLabelName_NonUniformValueDistribution tests that for a given label, if one value is much lower-cardinality
// than all others, the resulting count-min sketch reflects that difference in order of magnitude against all
// higher-cardinality label values.
func TestLabelName_NonUniformValueDistribution(t *testing.T) {
	labelName := "test_label"
	numSeries := int(6e6)
	lowCard := 10
	lowCardValue := "low"
	numHighOccurrenceValues := int(1e3)

	p := NewMemPostings()
	require.Less(t, lowCard+numHighOccurrenceValues, numSeries)

	for i := 0; i < numSeries-lowCard; i++ {
		p.addFor(storage.SeriesRef(i), labels.Label{
			Name: labelName,
			// Set the label value to something [0-numLabelValues]
			Value: strconv.Itoa(i % numHighOccurrenceValues),
		})
	}

	// cardinality of "low" value will be 10
	for i := numSeries - lowCard; i < numSeries; i++ {
		p.addFor(storage.SeriesRef(i), labels.Label{
			Name:  labelName,
			Value: lowCardValue,
		})
	}

	ctx := context.Background()
	s := p.LabelsValuesSketches()

	lowValCard := s.LabelValuesCardinality(ctx, labelName, lowCardValue)

	// The cardinality of every other value should be ≥6000. We care about these values being correct in magnitude,
	// i.e., floor(log(highOccurrenceCardinality) / log(lowOccurrenceCardinality)) should be consistent every time.
	// We add a little margin since the margin of error (30k) is enough to push us one power up, but not two.
	for i := 0; i < numHighOccurrenceValues; i++ {
		card := s.LabelValuesCardinality(ctx, labelName, strconv.Itoa(i))
		mag := math.Log(float64(card)) / math.Log(float64(lowValCard))
		require.GreaterOrEqual(t, int(math.Floor(mag)), 3)
		require.Less(t, int(math.Floor(mag)), 5)
	}
}
