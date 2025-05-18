// Copyright 2015 The Prometheus Authors
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

package parser

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/model/labels"
)

// Tree returns a string of the tree structure of the given node.
func Tree(node Node) string {
	return tree(node, "")
}

func tree(node Node, level string) string {
	if node == nil {
		return fmt.Sprintf("%s |---- %T\n", level, node)
	}
	typs := strings.Split(fmt.Sprintf("%T", node), ".")[1]

	t := fmt.Sprintf("%s |---- %s :: %s\n", level, typs, node)

	level += " · · ·"

	for _, e := range Children(node) {
		t += tree(e, level)
	}

	return t
}

func (node *EvalStmt) String() string {
	return "EVAL " + node.Expr.String()
}

func (es Expressions) String() (s string) {
	b := bytes.NewBuffer(make([]byte, 0, 128))
	es.WriteTo(b)
	return b.String()
}

func (es Expressions) WriteTo(b *bytes.Buffer) {
	if len(es) == 0 {
		return
	}
	es[0].WriteTo(b)
	for _, e := range es[1:] {
		b.WriteString(", ")
		e.WriteTo(b)
	}
}

func (node *AggregateExpr) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 128))
	node.WriteTo(b)
	return b.String()
}

func (node *AggregateExpr) WriteTo(b *bytes.Buffer) {
	node.writeAggOpStr(b)
	b.WriteString("(")
	if node.Op.IsAggregatorWithParam() {
		b.WriteString(node.Param.String())
		b.WriteString(", ")
	}
	node.Expr.WriteTo(b)
	b.WriteString(")")
}

func (node *AggregateExpr) ShortString() string {
	b := bytes.NewBuffer(make([]byte, 0, 1024))
	node.writeAggOpStr(b)
	return b.String()
}

func (node *AggregateExpr) writeAggOpStr(b *bytes.Buffer) {
	b.WriteString(node.Op.String())

	switch {
	case node.Without:
		b.WriteString(" without (")
		writeLabels(b, node.Grouping)
		b.WriteString(") ")
	case len(node.Grouping) > 0:
		b.WriteString(" by (")
		writeLabels(b, node.Grouping)
		b.WriteString(") ")
	}
}

func writeLabels(b *bytes.Buffer, ss []string) {
	for i, s := range ss {
		if i > 0 {
			b.WriteString(", ")
		}
		if !model.IsValidLegacyMetricName(string(model.LabelValue(s))) {
			b.Write(strconv.AppendQuote(b.AvailableBuffer(), s))
		} else {
			b.WriteString(s)
		}
	}
}

// writeStringsJoin is like strings.Join but appending to a bytes.Buffer.
func writeStringsJoin(b *bytes.Buffer, elems []string, sep string) {
	if len(elems) == 0 {
		return
	}
	b.WriteString(elems[0])
	for _, s := range elems[1:] {
		b.WriteString(sep)
		b.WriteString(s)
	}
}

func (node *BinaryExpr) returnBool() string {
	if node.ReturnBool {
		return " bool"
	}
	return ""
}

func (node *BinaryExpr) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 128))
	node.WriteTo(b)
	return b.String()
}

func (node *BinaryExpr) WriteTo(b *bytes.Buffer) {
	node.LHS.WriteTo(b)
	b.WriteByte(' ')
	b.WriteString(node.Op.String())
	b.WriteString(node.returnBool())
	node.writeMatchingStr(b)
	b.WriteByte(' ')
	node.RHS.WriteTo(b)
}

func (node *BinaryExpr) ShortString() string {
	b := bytes.NewBuffer(make([]byte, 0, 128))
	b.WriteString(node.Op.String())
	b.WriteString(node.returnBool())
	node.writeMatchingStr(b)
	return b.String()
}

func (node *BinaryExpr) writeMatchingStr(b *bytes.Buffer) {
	vm := node.VectorMatching
	if vm != nil && (len(vm.MatchingLabels) > 0 || vm.On) {
		if vm.On {
			b.WriteString(" on (")
		} else {
			b.WriteString(" ignoring (")
		}
		writeStringsJoin(b, vm.MatchingLabels, ", ")
		b.WriteByte(')')

		if vm.Card == CardManyToOne || vm.Card == CardOneToMany {
			if vm.Card == CardManyToOne {
				b.WriteString(" group_left (")
			} else {
				b.WriteString(" group_right (")
			}
			writeStringsJoin(b, vm.Include, ", ")
			b.WriteByte(')')
		}
	}
}

func (node *DurationExpr) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 1024))
	node.WriteTo(b)
	return b.String()
}

func (node *DurationExpr) WriteTo(b *bytes.Buffer) {
	if node.Wrapped {
		b.WriteByte('(')
	}
	if node.LHS == nil {
		// This is a unary negative duration expression.
		b.WriteString(node.Op.String())
		b.WriteString(node.RHS.String())
	} else {
		b.WriteString(node.LHS.String())
		b.WriteByte(' ')
		b.WriteString(node.Op.String())
		b.WriteByte(' ')
		b.WriteString(node.RHS.String())
	}

	if node.Wrapped {
		b.WriteByte(')')
	}
}

func (node *DurationExpr) ShortString() string {
	return node.Op.String()
}

func (node *Call) String() string {
	return node.Func.Name + "(" + node.Args.String() + ")"
}

func (node *Call) WriteTo(b *bytes.Buffer) {
	b.WriteString(node.Func.Name)
	b.WriteByte('(')
	node.Args.WriteTo(b)
	b.WriteByte(')')
}

func (node *Call) ShortString() string {
	return node.Func.Name
}

func (node *MatrixSelector) atOffset() (string, string) {
	// Copy the Vector selector before changing the offset
	vecSelector := node.VectorSelector.(*VectorSelector)
	offset := ""
	switch {
	case vecSelector.OriginalOffsetExpr != nil:
		offset = fmt.Sprintf(" offset %s", vecSelector.OriginalOffsetExpr)
	case vecSelector.OriginalOffset > time.Duration(0):
		offset = fmt.Sprintf(" offset %s", model.Duration(vecSelector.OriginalOffset))
	case vecSelector.OriginalOffset < time.Duration(0):
		offset = fmt.Sprintf(" offset -%s", model.Duration(-vecSelector.OriginalOffset))
	}
	at := ""
	switch {
	case vecSelector.Timestamp != nil:
		at = fmt.Sprintf(" @ %.3f", float64(*vecSelector.Timestamp)/1000.0)
	case vecSelector.StartOrEnd == START:
		at = " @ start()"
	case vecSelector.StartOrEnd == END:
		at = " @ end()"
	}
	return at, offset
}

func (node *MatrixSelector) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 128))
	node.WriteTo(b)
	return b.String()
}

func (node *MatrixSelector) WriteTo(b *bytes.Buffer) {
	at, offset := node.atOffset()
	// Copy the Vector selector before changing the offset
	vecSelector := *node.VectorSelector.(*VectorSelector)
	// Do not print the @ and offset twice.
	offsetVal, offsetExprVal, atVal, preproc := vecSelector.OriginalOffset, vecSelector.OriginalOffsetExpr, vecSelector.Timestamp, vecSelector.StartOrEnd
	vecSelector.OriginalOffset = 0
	vecSelector.OriginalOffsetExpr = nil
	vecSelector.Timestamp = nil
	vecSelector.StartOrEnd = 0

	rangeStr := model.Duration(node.Range).String()
	if node.RangeExpr != nil {
		rangeStr = node.RangeExpr.String()
	}
	vecSelector.WriteTo(b)
	b.WriteByte('[')
	b.WriteString(rangeStr)
	b.WriteByte(']')
	b.WriteString(at)
	b.WriteString(offset)

	vecSelector.OriginalOffset, vecSelector.OriginalOffsetExpr, vecSelector.Timestamp, vecSelector.StartOrEnd = offsetVal, offsetExprVal, atVal, preproc
}

func (node *MatrixSelector) ShortString() string {
	at, offset := node.atOffset()
	rangeStr := model.Duration(node.Range).String()
	if node.RangeExpr != nil {
		rangeStr = node.RangeExpr.String()
	}
	return fmt.Sprintf("[%s]%s%s", rangeStr, at, offset)
}

func (node *SubqueryExpr) String() string {
	return fmt.Sprintf("%s%s", node.Expr.String(), node.getSubqueryTimeSuffix())
}

func (node *SubqueryExpr) WriteTo(b *bytes.Buffer) {
	node.Expr.WriteTo(b)
	b.WriteString(node.getSubqueryTimeSuffix())
}

func (node *SubqueryExpr) ShortString() string {
	return node.getSubqueryTimeSuffix()
}

// getSubqueryTimeSuffix returns the '[<range>:<step>] @ <timestamp> offset <offset>' suffix of the subquery.
func (node *SubqueryExpr) getSubqueryTimeSuffix() string {
	step := ""
	if node.Step != 0 {
		step = model.Duration(node.Step).String()
	} else if node.StepExpr != nil {
		step = node.StepExpr.String()
	}
	offset := ""
	switch {
	case node.OriginalOffsetExpr != nil:
		offset = fmt.Sprintf(" offset %s", node.OriginalOffsetExpr)
	case node.OriginalOffset > time.Duration(0):
		offset = fmt.Sprintf(" offset %s", model.Duration(node.OriginalOffset))
	case node.OriginalOffset < time.Duration(0):
		offset = fmt.Sprintf(" offset -%s", model.Duration(-node.OriginalOffset))
	}
	at := ""
	switch {
	case node.Timestamp != nil:
		at = fmt.Sprintf(" @ %.3f", float64(*node.Timestamp)/1000.0)
	case node.StartOrEnd == START:
		at = " @ start()"
	case node.StartOrEnd == END:
		at = " @ end()"
	}
	rangeStr := model.Duration(node.Range).String()
	if node.RangeExpr != nil {
		rangeStr = node.RangeExpr.String()
	}
	return fmt.Sprintf("[%s:%s]%s%s", rangeStr, step, at, offset)
}

func (node *NumberLiteral) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 128))
	node.WriteTo(b)
	return b.String()
}

func (node *NumberLiteral) WriteTo(b *bytes.Buffer) {
	if node.Duration {
		if node.Val < 0 {
			b.WriteByte('-')
			b.WriteString(model.Duration(-node.Val * 1e9).String())
			return
		}
		b.WriteString(model.Duration(node.Val * 1e9).String())
		return
	}
	b.Write(strconv.AppendFloat(b.AvailableBuffer(), node.Val, 'f', -1, 64))
}

func (node *ParenExpr) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 128))
	node.WriteTo(b)
	return b.String()
}

func (node *ParenExpr) WriteTo(b *bytes.Buffer) {
	b.WriteByte('(')
	node.Expr.WriteTo(b)
	b.WriteByte(')')
}

func (node *StringLiteral) String() string {
	return strconv.Quote(node.Val)
}

func (node *StringLiteral) WriteTo(b *bytes.Buffer) {
	b.Write(strconv.AppendQuote(b.AvailableBuffer(), node.Val))
}

func (node *UnaryExpr) String() string {
	return node.Op.String() + node.Expr.String()
}

func (node *UnaryExpr) WriteTo(b *bytes.Buffer) {
	b.WriteString(node.Op.String())
	node.Expr.WriteTo(b)
}

func (node *UnaryExpr) ShortString() string {
	return node.Op.String()
}

func (node *VectorSelector) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 128))
	node.WriteTo(b)
	return b.String()
}

func (node *VectorSelector) WriteTo(b *bytes.Buffer) {
	var labelStrings []string
	if len(node.LabelMatchers) > 1 {
		labelStrings = make([]string, 0, len(node.LabelMatchers)-1)
	}
	for _, matcher := range node.LabelMatchers {
		// Only include the __name__ label if its equality matching and matches the name, but don't skip if it's an explicit empty name matcher.
		if matcher.Name == labels.MetricName && matcher.Type == labels.MatchEqual && matcher.Value == node.Name && matcher.Value != "" {
			continue
		}
		labelStrings = append(labelStrings, matcher.String())
	}
	b.WriteString(node.Name)
	if len(labelStrings) != 0 {
		b.WriteByte('{')
		sort.Strings(labelStrings)
		writeStringsJoin(b, labelStrings, ",")
		b.WriteByte('}')
	}
	switch {
	case node.Timestamp != nil:
		b.WriteString(" @ ")
		fmt.Fprintf(b, "%.3f", float64(*node.Timestamp)/1000.0)
	case node.StartOrEnd == START:
		b.WriteString(" @ start()")
	case node.StartOrEnd == END:
		b.WriteString(" @ end()")
	}
	switch {
	case node.OriginalOffsetExpr != nil:
		b.WriteString(" offset ")
		node.OriginalOffsetExpr.WriteTo(b)
	case node.OriginalOffset > time.Duration(0):
		b.WriteString(" offset ")
		b.WriteString(model.Duration(node.OriginalOffset).String())
	case node.OriginalOffset < time.Duration(0):
		b.WriteString(" offset -")
		b.WriteString(model.Duration(-node.OriginalOffset).String())
	}
}
