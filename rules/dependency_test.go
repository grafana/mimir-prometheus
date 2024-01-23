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
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestDependencyMap(t *testing.T) {
	ctx := context.Background()
	opts := &ManagerOptions{
		Context: ctx,
		Logger:  log.NewNopLogger(),
	}

	expr, err := parser.ParseExpr("sum by (user) (rate(requests[1m]))")
	require.NoError(t, err)
	rule := NewRecordingRule("user:requests:rate1m", expr, labels.Labels{})

	expr, err = parser.ParseExpr("user:requests:rate1m <= 0")
	require.NoError(t, err)
	rule2 := NewAlertingRule("ZeroRequests", expr, 0, 0, labels.Labels{}, labels.Labels{}, labels.EmptyLabels(), "", true, log.NewNopLogger())

	expr, err = parser.ParseExpr("sum by (user) (rate(requests[5m]))")
	require.NoError(t, err)
	rule3 := NewRecordingRule("user:requests:rate5m", expr, labels.Labels{})

	expr, err = parser.ParseExpr("increase(user:requests:rate1m[1h])")
	require.NoError(t, err)
	rule4 := NewRecordingRule("user:requests:increase1h", expr, labels.Labels{})

	group := NewGroup(GroupOptions{
		Name:     "rule_group",
		Interval: time.Second,
		Rules:    []Rule{rule, rule2, rule3, rule4},
		Opts:     opts,
	})

	depMap := buildDependencyMap(group.rules)

	require.Zero(t, depMap.dependencies(rule))
	require.Equal(t, 2, depMap.dependents(rule))
	require.False(t, depMap.isIndependent(rule))

	require.Zero(t, depMap.dependents(rule2))
	require.Equal(t, 1, depMap.dependencies(rule2))
	require.False(t, depMap.isIndependent(rule2))

	require.Zero(t, depMap.dependents(rule3))
	require.Zero(t, depMap.dependencies(rule3))
	require.True(t, depMap.isIndependent(rule3))

	require.Zero(t, depMap.dependents(rule4))
	require.Equal(t, 1, depMap.dependencies(rule4))
	require.False(t, depMap.isIndependent(rule4))
}

func TestNoDependency(t *testing.T) {
	ctx := context.Background()
	opts := &ManagerOptions{
		Context: ctx,
		Logger:  log.NewNopLogger(),
	}

	expr, err := parser.ParseExpr("sum by (user) (rate(requests[1m]))")
	require.NoError(t, err)
	rule := NewRecordingRule("user:requests:rate1m", expr, labels.Labels{})

	group := NewGroup(GroupOptions{
		Name:     "rule_group",
		Interval: time.Second,
		Rules:    []Rule{rule},
		Opts:     opts,
	})

	depMap := buildDependencyMap(group.rules)
	// A group with only one rule cannot have dependencies.
	require.Empty(t, depMap)
}

func TestDependenciesEdgeCases(t *testing.T) {
	ctx := context.Background()
	opts := &ManagerOptions{
		Context: ctx,
		Logger:  log.NewNopLogger(),
	}

	t.Run("empty group", func(t *testing.T) {
		group := NewGroup(GroupOptions{
			Name:     "rule_group",
			Interval: time.Second,
			Rules:    []Rule{}, // empty group
			Opts:     opts,
		})

		expr, err := parser.ParseExpr("sum by (user) (rate(requests[1m]))")
		require.NoError(t, err)
		rule := NewRecordingRule("user:requests:rate1m", expr, labels.Labels{})

		depMap := buildDependencyMap(group.rules)
		// A group with no rules has no dependency map, but doesn't panic if the map is queried.
		require.Nil(t, depMap)
		require.False(t, depMap.isIndependent(rule))
	})

	t.Run("rules which reference no series", func(t *testing.T) {
		expr, err := parser.ParseExpr("one")
		require.NoError(t, err)
		rule1 := NewRecordingRule("1", expr, labels.Labels{})

		expr, err = parser.ParseExpr("two")
		require.NoError(t, err)
		rule2 := NewRecordingRule("2", expr, labels.Labels{})

		group := NewGroup(GroupOptions{
			Name:     "rule_group",
			Interval: time.Second,
			Rules:    []Rule{rule1, rule2},
			Opts:     opts,
		})

		depMap := buildDependencyMap(group.rules)
		// A group with rules which reference no series will still produce a dependency map
		require.True(t, depMap.isIndependent(rule1))
		require.True(t, depMap.isIndependent(rule2))
	})
}

func TestNoMetricSelector(t *testing.T) {
	ctx := context.Background()
	opts := &ManagerOptions{
		Context: ctx,
		Logger:  log.NewNopLogger(),
	}

	expr, err := parser.ParseExpr("sum by (user) (rate(requests[1m]))")
	require.NoError(t, err)
	rule := NewRecordingRule("user:requests:rate1m", expr, labels.Labels{})

	expr, err = parser.ParseExpr(`count({user="bob"})`)
	require.NoError(t, err)
	rule2 := NewRecordingRule("user:requests:rate1m", expr, labels.Labels{})

	group := NewGroup(GroupOptions{
		Name:     "rule_group",
		Interval: time.Second,
		Rules:    []Rule{rule, rule2},
		Opts:     opts,
	})

	depMap := buildDependencyMap(group.rules)
	// A rule with no metric selector cannot be reliably determined to have no dependencies on other rules, and therefore
	// all rules are not considered independent.
	require.False(t, depMap.isIndependent(rule))
	require.False(t, depMap.isIndependent(rule2))
}

func TestDependentRulesWithNonMetricExpression(t *testing.T) {
	ctx := context.Background()
	opts := &ManagerOptions{
		Context: ctx,
		Logger:  log.NewNopLogger(),
	}

	expr, err := parser.ParseExpr("sum by (user) (rate(requests[1m]))")
	require.NoError(t, err)
	rule := NewRecordingRule("user:requests:rate1m", expr, labels.Labels{})

	expr, err = parser.ParseExpr("user:requests:rate1m <= 0")
	require.NoError(t, err)
	rule2 := NewAlertingRule("ZeroRequests", expr, 0, 0, labels.Labels{}, labels.Labels{}, labels.EmptyLabels(), "", true, log.NewNopLogger())

	expr, err = parser.ParseExpr("3")
	require.NoError(t, err)
	rule3 := NewRecordingRule("three", expr, labels.Labels{})

	group := NewGroup(GroupOptions{
		Name:     "rule_group",
		Interval: time.Second,
		Rules:    []Rule{rule, rule2, rule3},
		Opts:     opts,
	})

	depMap := buildDependencyMap(group.rules)
	require.False(t, depMap.isIndependent(rule))
	require.False(t, depMap.isIndependent(rule2))
	require.True(t, depMap.isIndependent(rule3))
}

func TestRulesDependentOnMetaMetrics(t *testing.T) {
	ctx := context.Background()
	opts := &ManagerOptions{
		Context: ctx,
		Logger:  log.NewNopLogger(),
	}

	// This rule is not dependent on any other rules in its group but it does depend on `ALERTS`, which is produced by
	// the rule engine, and is therefore not independent.
	expr, err := parser.ParseExpr("count(ALERTS)")
	require.NoError(t, err)
	rule := NewRecordingRule("alert_count", expr, labels.Labels{})

	// Create another rule so a dependency map is built (no map is built if a group contains one or fewer rules).
	expr, err = parser.ParseExpr("1")
	require.NoError(t, err)
	rule2 := NewRecordingRule("one", expr, labels.Labels{})

	group := NewGroup(GroupOptions{
		Name:     "rule_group",
		Interval: time.Second,
		Rules:    []Rule{rule, rule2},
		Opts:     opts,
	})

	depMap := buildDependencyMap(group.rules)
	require.False(t, depMap.isIndependent(rule))
}
