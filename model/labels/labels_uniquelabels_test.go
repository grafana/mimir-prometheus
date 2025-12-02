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
	"fmt"
	"strings"
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

var benchmarkLabelsUnstableHashResult UnstableHash

func BenchmarkLabels_UnstableHash(b *testing.B) {
	for _, tcase := range []struct {
		name string
		lbls Labels
	}{
		{
			name: "typical labels under 1KB",
			lbls: func() Labels {
				b := NewBuilder(EmptyLabels())
				for i := range 10 {
					// Label ~20B name, 50B value.
					b.Set(fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i))
				}
				return b.Labels()
			}(),
		},
		{
			name: "bigger labels over 1KB",
			lbls: func() Labels {
				b := NewBuilder(EmptyLabels())
				for i := range 10 {
					// Label ~50B name, 50B value.
					b.Set(fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i), fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i))
				}
				return b.Labels()
			}(),
		},
		{
			name: "extremely large label value 10MB",
			lbls: func() Labels {
				lbl := &strings.Builder{}
				lbl.Grow(1024 * 1024 * 10) // 10MB.
				word := "abcdefghij"
				for i := 0; i < lbl.Cap()/len(word); i++ {
					_, _ = lbl.WriteString(word)
				}
				return FromStrings("__name__", lbl.String())
			}(),
		},
	} {
		b.Run(tcase.name, func(b *testing.B) {
			var h UnstableHash

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				h = tcase.lbls.UnstableHash()
			}
			benchmarkLabelsUnstableHashResult = h
		})
	}
}

func TestLabels_UnstableHash(t *testing.T) {
	lbls1 := FromStrings("foo", "bar", "baz", "qux")
	lbls2 := FromStrings("foo", "bar", "baz", "qux")
	hash1, hash2 := lbls1.UnstableHash(), lbls2.UnstableHash()
	require.Equal(t, hash1, hash2)
	require.NotEqual(t, lbls1.UnstableHash(), FromStrings("foo", "bar", "baz", "quX").UnstableHash(), "different labels match.")
}
