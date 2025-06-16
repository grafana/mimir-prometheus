// Copyright 2025 The Prometheus Authors
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

package labels

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestIsValidMetricName(t *testing.T) {
	testCases := []struct {
		metricName  string
		legacyValid bool
		utf8Valid   bool
	}{
		{
			metricName:  "Avalid_23name",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			metricName:  "_Avalid_23name",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			metricName:  "1valid_23name",
			legacyValid: false,
			utf8Valid:   true,
		},
		{
			metricName:  "avalid_23name",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			metricName:  "Ava:lid_23name",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			metricName:  "a lid_23name",
			legacyValid: false,
			utf8Valid:   true,
		},
		{
			metricName:  ":leading_colon",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			metricName:  "colon:in:the:middle",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			metricName:  "",
			legacyValid: false,
			utf8Valid:   false,
		},
		{
			metricName:  "a\xc5z",
			legacyValid: false,
			utf8Valid:   false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.metricName, func(t *testing.T) {
			require.Equal(t, tc.legacyValid, IsValidMetricName(tc.metricName, model.LegacyValidation))
			require.Equal(t, tc.utf8Valid, IsValidMetricName(tc.metricName, model.UTF8Validation))
		})
	}
}

func TestIsValidLabelName(t *testing.T) {
	testCases := []struct {
		labelName   string
		legacyValid bool
		utf8Valid   bool
	}{
		{
			labelName:   "Avalid_23name",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			labelName:   "_Avalid_23name",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			labelName:   "1valid_23name",
			legacyValid: false,
			utf8Valid:   true,
		},
		{
			labelName:   "avalid_23name",
			legacyValid: true,
			utf8Valid:   true,
		},
		{
			labelName:   "Ava:lid_23name",
			legacyValid: false,
			utf8Valid:   true,
		},
		{
			labelName:   "a lid_23name",
			legacyValid: false,
			utf8Valid:   true,
		},
		{
			labelName:   ":leading_colon",
			legacyValid: false,
			utf8Valid:   true,
		},
		{
			labelName:   "colon:in:the:middle",
			legacyValid: false,
			utf8Valid:   true,
		},
		{
			labelName:   "a\xc5z",
			legacyValid: false,
			utf8Valid:   false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.labelName, func(t *testing.T) {
			require.Equal(t, tc.legacyValid, IsValidLabelName(tc.labelName, model.LegacyValidation))
			require.Equal(t, tc.utf8Valid, IsValidLabelName(tc.labelName, model.UTF8Validation))
		})
	}
}
