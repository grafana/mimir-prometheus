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

//go:build uniquelabels

package labels

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var expectedSizeOfLabels = []uint64{ // Values must line up with testCaseLabels.
	72,
	0,
	97,
	326,
	327,
	549,
}

var expectedByteSize = expectedSizeOfLabels // They are identical

func TestLabels_FromSymbols(t *testing.T) {
	labels := FromSymbols(NewSymbol("aaa"), NewSymbol("111"), NewSymbol("bbb"), NewSymbol("222"))
	x := 0
	labels.Range(func(l Label) {
		switch x {
		case 0:
			require.Equal(t, Label{Name: "aaa", Value: "111"}, l, "unexpected value")
		case 1:
			require.Equal(t, Label{Name: "bbb", Value: "222"}, l, "unexpected value")
		default:
			t.Fatalf("unexpected labelset value %d: %v", x, l)
		}
		x++
	})

	require.Panics(t, func() { FromStrings("aaa", "111", "bbb") }) //nolint:staticcheck // Ignore SA5012, error is intentional test.
}
