// Copyright 2013 The Prometheus Authors
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

package rules

import (
	"testing"
	"time"

	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestNewGroup(t *testing.T) {
	g := NewGroup(GroupOptions{
		File: "test-file",
		Name: "test-name",
	})
	require.Equal(t, promslog.NewNopLogger().With("file", "test-file", "group", "test-name"), g.logger)
}

func TestGroup_Equals(t *testing.T) {
	testExpression, err := parser.ParseExpr("up")
	require.NoError(t, err)

	tests := map[string]struct {
		first    *Group
		second   *Group
		expected bool
	}{
		"no query offset set on both groups": {
			first: &Group{
				name:     "group-1",
				file:     "file-1",
				interval: time.Minute,
			},
			second: &Group{
				name:     "group-1",
				file:     "file-1",
				interval: time.Minute,
			},
			expected: true,
		},
		"query offset set only on the first group": {
			first: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](time.Minute),
			},
			second: &Group{
				name:     "group-1",
				file:     "file-1",
				interval: time.Minute,
			},
			expected: false,
		},
		"query offset set on both groups to the same value": {
			first: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](time.Minute),
			},
			second: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](time.Minute),
			},
			expected: true,
		},
		"query offset set on both groups to different value": {
			first: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](time.Minute),
			},
			second: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](2 * time.Minute),
			},
			expected: false,
		},
		"identical configs": {
			first: &Group{
				name: "example_group",
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			second: &Group{
				name: "example_group",
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			expected: true,
		},
		"differently ordered source tenants (should still be equivalent)": {
			first: &Group{
				name:          "example_group",
				sourceTenants: []string{"tenant-2", "tenant-1"},
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			second: &Group{
				name:          "example_group",
				sourceTenants: []string{"tenant-1", "tenant-2"},
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			expected: true,
		},
		"different rule length": {
			first: &Group{
				name: "example_group",
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			second: &Group{
				name: "example_group",
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			expected: false,
		},
		"different rule labels": {
			first: &Group{
				name: "example_group",
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			second: &Group{
				name: "example_group",
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"1": "2", "3": "4"}),
					},
				},
			},
			expected: false,
		},
		"different source tenants": {
			first: &Group{
				name:          "example_group",
				sourceTenants: []string{"tenant-1", "tenant-3"},
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			second: &Group{
				name:          "example_group",
				sourceTenants: []string{"tenant-1", "tenant-2"},
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			expected: false,
		},
		"repeating source tenants": {
			first: &Group{
				name:          "example_group",
				sourceTenants: []string{"tenant-1", "tenant-2"},
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			second: &Group{
				name:          "example_group",
				sourceTenants: []string{"tenant-1", "tenant-1"},
				rules: []Rule{
					&RecordingRule{
						name:   "one",
						vector: testExpression,
						labels: labels.FromMap(map[string]string{"a": "b", "c": "d"}),
					},
				},
			},
			expected: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, testData.expected, testData.first.Equals(testData.second))
			require.Equal(t, testData.expected, testData.second.Equals(testData.first))
		})
	}
}

func pointerOf[T any](value T) *T {
	return &value
}
