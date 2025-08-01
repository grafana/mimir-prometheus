// Copyright 2017 The Prometheus Authors
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

package rulefmt

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestParseFileSuccess(t *testing.T) {
	_, errs := ParseFile("testdata/test.yaml")
	require.Empty(t, errs, "unexpected errors parsing file")

	_, errs = ParseFile("testdata/utf-8_lname.good.yaml")
	require.Empty(t, errs, "unexpected errors parsing file")
	_, errs = ParseFile("testdata/utf-8_annotation.good.yaml")
	require.Empty(t, errs, "unexpected errors parsing file")
	_, errs = ParseFile("testdata/legacy_validation_annotation.good.yaml", WithValidationScheme(model.LegacyValidation))
	require.Empty(t, errs, "unexpected errors parsing file")
}

func TestParseFileSuccessWithAliases(t *testing.T) {
	exprString := `sum without(instance) (rate(errors_total[5m]))
/
sum without(instance) (rate(requests_total[5m]))
`
	rgs, errs := ParseFile("testdata/test_aliases.yaml")
	require.Empty(t, errs, "unexpected errors parsing file")
	for _, rg := range rgs.Groups {
		require.Equal(t, "HighAlert", rg.Rules[0].Alert)
		require.Equal(t, "critical", rg.Rules[0].Labels["severity"])
		require.Equal(t, "stuff's happening with {{ $.labels.service }}", rg.Rules[0].Annotations["description"])

		require.Equal(t, "new_metric", rg.Rules[1].Record)

		require.Equal(t, "HighAlert", rg.Rules[2].Alert)
		require.Equal(t, "critical", rg.Rules[2].Labels["severity"])
		require.Equal(t, "stuff's happening with {{ $.labels.service }}", rg.Rules[2].Annotations["description"])

		require.Equal(t, "HighAlert2", rg.Rules[3].Alert)
		require.Equal(t, "critical", rg.Rules[3].Labels["severity"])

		for _, rule := range rg.Rules {
			require.Equal(t, exprString, rule.Expr)
		}
	}
}

func TestParseFileFailure(t *testing.T) {
	for _, c := range []struct {
		filename string
		errMsg   string
		opts     []ParseOption
	}{
		{
			filename: "duplicate_grp.bad.yaml",
			errMsg:   "groupname: \"yolo\" is repeated in the same file",
		},
		{
			filename: "bad_expr.bad.yaml",
			errMsg:   "parse error",
		},
		{
			filename: "record_and_alert.bad.yaml",
			errMsg:   "only one of 'record' and 'alert' must be set",
		},
		{
			filename: "no_rec_alert.bad.yaml",
			errMsg:   "one of 'record' or 'alert' must be set",
		},
		{
			filename: "noexpr.bad.yaml",
			errMsg:   "field 'expr' must be set in rule",
		},
		{
			filename: "invalid_record_name.bad.yaml",
			errMsg:   "braces present in the recording rule name; should it be in expr?: strawberry{flavor=\"sweet\"}",
		},
		{
			filename: "bad_field.bad.yaml",
			errMsg:   "field annotation not found",
		},
		{
			filename: "invalid_label_name.bad.yaml",
			errMsg:   "invalid label name",
		},
		{
			filename: "record_and_for.bad.yaml",
			errMsg:   "invalid field 'for' in recording rule",
		},
		{
			filename: "record_and_keep_firing_for.bad.yaml",
			errMsg:   "invalid field 'keep_firing_for' in recording rule",
		},
		{
			filename: "legacy_validation_annotation.bad.yaml",
			opts:     []ParseOption{WithValidationScheme(model.LegacyValidation)},
			errMsg:   "invalid annotation name: ins-tance",
		},
	} {
		t.Run(c.filename, func(t *testing.T) {
			_, errs := ParseFile(filepath.Join("testdata", c.filename), c.opts...)
			require.NotEmpty(t, errs, "Expected error parsing %s but got none", c.filename)
			require.ErrorContainsf(t, errs[0], c.errMsg, "Expected error for %s.", c.filename)
		})
	}
}

func TestTemplateParsing(t *testing.T) {
	tests := []struct {
		ruleString string
		shouldPass bool
	}{
		{
			ruleString: `
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
`,
			shouldPass: true,
		},
		{
			ruleString: `
groups:
- name: example
  labels:
    team: myteam
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
`,
			shouldPass: true,
		},
		{
			// `$label` instead of `$labels`.
			ruleString: `
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $label.instance }} down"
`,
			shouldPass: false,
		},
		{
			// `$this_is_wrong`.
			ruleString: `
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "{{$this_is_wrong}}"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
`,
			shouldPass: false,
		},
		{
			// `$labels.quantile * 100`.
			ruleString: `
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
      description: "{{$labels.quantile * 100}}"
`,
			shouldPass: false,
		},
	}

	for _, tst := range tests {
		rgs, errs := Parse([]byte(tst.ruleString))
		require.NotNil(t, rgs, "Rule parsing, rule=\n"+tst.ruleString)
		passed := (tst.shouldPass && len(errs) == 0) || (!tst.shouldPass && len(errs) > 0)
		require.True(t, passed, "Rule validation failed, rule=\n"+tst.ruleString)
	}
}

func TestUniqueErrorNodes(t *testing.T) {
	group := `
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up ===== 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
  - alert: InstanceUp
    expr: up ===== 1
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} up"
`
	_, errs := Parse([]byte(group))
	require.Len(t, errs, 2, "Expected two errors")
	var err00 *Error
	require.ErrorAs(t, errs[0], &err00)
	err0 := err00.Err.node
	var err01 *Error
	require.ErrorAs(t, errs[1], &err01)
	err1 := err01.Err.node
	require.NotEqual(t, err0, err1, "Error nodes should not be the same")
}

func TestError(t *testing.T) {
	tests := []struct {
		name  string
		error *Error
		want  string
	}{
		{
			name: "with alternative node provided in WrappedError",
			error: &Error{
				Group:    "some group",
				Rule:     1,
				RuleName: "some rule name",
				Err: WrappedError{
					err: errors.New("some error"),
					node: &yaml.Node{
						Line:   10,
						Column: 20,
					},
					nodeAlt: &yaml.Node{
						Line:   11,
						Column: 21,
					},
				},
			},
			want: `10:20: 11:21: group "some group", rule 1, "some rule name": some error`,
		},
		{
			name: "with node provided in WrappedError",
			error: &Error{
				Group:    "some group",
				Rule:     1,
				RuleName: "some rule name",
				Err: WrappedError{
					err: errors.New("some error"),
					node: &yaml.Node{
						Line:   10,
						Column: 20,
					},
				},
			},
			want: `10:20: group "some group", rule 1, "some rule name": some error`,
		},
		{
			name: "with only err provided in WrappedError",
			error: &Error{
				Group:    "some group",
				Rule:     1,
				RuleName: "some rule name",
				Err: WrappedError{
					err: errors.New("some error"),
				},
			},
			want: `group "some group", rule 1, "some rule name": some error`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.EqualError(t, tt.error, tt.want)
		})
	}
}

func TestWrappedError(t *testing.T) {
	tests := []struct {
		name         string
		wrappedError *WrappedError
		want         string
	}{
		{
			name: "with alternative node provided",
			wrappedError: &WrappedError{
				err: errors.New("some error"),
				node: &yaml.Node{
					Line:   10,
					Column: 20,
				},
				nodeAlt: &yaml.Node{
					Line:   11,
					Column: 21,
				},
			},
			want: `10:20: 11:21: some error`,
		},
		{
			name: "with node provided",
			wrappedError: &WrappedError{
				err: errors.New("some error"),
				node: &yaml.Node{
					Line:   10,
					Column: 20,
				},
			},
			want: `10:20: some error`,
		},
		{
			name: "with only err provided",
			wrappedError: &WrappedError{
				err: errors.New("some error"),
			},
			want: `some error`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.EqualError(t, tt.wrappedError, tt.want)
		})
	}
}

func TestErrorUnwrap(t *testing.T) {
	err1 := errors.New("test error")

	tests := []struct {
		wrappedError   *Error
		unwrappedError error
	}{
		{
			wrappedError:   &Error{Err: WrappedError{err: err1}},
			unwrappedError: err1,
		},
		{
			wrappedError:   &Error{Err: WrappedError{err: io.ErrClosedPipe}},
			unwrappedError: io.ErrClosedPipe,
		},
	}

	for _, tt := range tests {
		t.Run(tt.wrappedError.Error(), func(t *testing.T) {
			require.ErrorIs(t, tt.wrappedError, tt.unwrappedError)
		})
	}
}

func TestRuleGroup_UnmarshalYaml(t *testing.T) {
	ruleStringWithEvalDelay := `
groups:
- name: example
  interval: 1m
  evaluation_delay: 5m
`

	ruleStringWithQueryOffset := `
groups:
- name: example
  interval: 1m
  query_offset: 2m
`

	ruleStringwithBoth := `
groups:
- name: example
  interval: 1m
  evaluation_delay: 5m
  query_offset: 2m
`

	dur5m := model.Duration(5 * time.Minute)
	dur2m := model.Duration(2 * time.Minute)

	tc := []struct {
		name       string
		ruleString string
		output     RuleGroup
	}{
		{
			name:       "with evaluation delay",
			ruleString: ruleStringWithEvalDelay,
			output: RuleGroup{
				Name:            "example",
				Interval:        model.Duration(1 * time.Minute),
				EvaluationDelay: &dur5m,
			},
		},
		{
			name:       "with query offset",
			ruleString: ruleStringWithQueryOffset,
			output: RuleGroup{
				Name:        "example",
				Interval:    model.Duration(1 * time.Minute),
				QueryOffset: &dur2m,
			},
		},
		{
			name:       "with query offset and evaluation delay",
			ruleString: ruleStringwithBoth,
			output: RuleGroup{
				Name:            "example",
				Interval:        model.Duration(1 * time.Minute),
				EvaluationDelay: &dur5m,
				QueryOffset:     &dur2m,
			},
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			rgs, errs := Parse([]byte(tt.ruleString))
			require.Empty(t, errs)

			require.Equal(t, tt.output, rgs.Groups[0])
		})
	}
}

func TestRuleGroup_MarshalYaml(t *testing.T) {
	dur5m := model.Duration(5 * time.Minute)
	dur2m := model.Duration(2 * time.Minute)

	tc := []struct {
		name       string
		input      RuleGroup
		ruleString string
	}{
		{
			name: "with evaluation delay",
			input: RuleGroup{
				Name:            "example",
				Interval:        model.Duration(1 * time.Minute),
				EvaluationDelay: &dur5m,
			},
			ruleString: `name: example
interval: 1m
evaluation_delay: 5m
rules: []
`,
		},
		{
			name: "with query offset",
			input: RuleGroup{
				Name:        "example",
				Interval:    model.Duration(1 * time.Minute),
				QueryOffset: &dur2m,
			},
			ruleString: `name: example
interval: 1m
query_offset: 2m
rules: []
`,
		},
		{
			name: "with query offset and evaluation delay",
			input: RuleGroup{
				Name:            "example",
				Interval:        model.Duration(1 * time.Minute),
				EvaluationDelay: &dur5m,
				QueryOffset:     &dur2m,
			},
			ruleString: `name: example
interval: 1m
evaluation_delay: 5m
query_offset: 2m
rules: []
`,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			out, err := yaml.Marshal(&tt.input)
			require.NoError(t, err)

			require.Equal(t, tt.ruleString, string(out))
		})
	}
}
