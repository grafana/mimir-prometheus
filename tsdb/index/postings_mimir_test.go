// Copyright 2026 Grafana Labs
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestMemPostings_PostingsListAccessors(t *testing.T) {
	mp := NewMemPostings()
	mp.Add(5, labels.FromStrings("pod", "a", "job", "z"))
	mp.Add(1, labels.FromStrings("pod", "a", "job", "x"))
	mp.Add(2, labels.FromStrings("pod", "a", "job", "y"))
	mp.Add(3, labels.FromStrings("pod", "b", "job", "y"))

	for _, tc := range []struct {
		name, value string
		allRefs     []storage.SeriesRef
	}{
		{name: "pod", value: "a", allRefs: []storage.SeriesRef{1, 2, 5}},
		{name: "pod", value: "b", allRefs: []storage.SeriesRef{3}},
		{name: "job", value: "y", allRefs: []storage.SeriesRef{2, 3}},
		{name: "pod", value: "missing"},
		{name: "missing", value: "a"},
	} {
		t.Run(tc.name+"="+tc.value, func(t *testing.T) {
			require.Equal(t, len(tc.allRefs), mp.PostingsLength(tc.name, tc.value))

			randomRefs := mp.AppendRandomPostingsRefs(tc.name, tc.value, 10, nil)
			if len(tc.allRefs) == 0 {
				require.Empty(t, randomRefs)
				return
			}
			require.Len(t, randomRefs, 10)
			for _, ref := range randomRefs {
				require.Contains(t, tc.allRefs, ref)
			}
		})
	}

	t.Run("AppendRandomPostingsRefs appends to the buffer", func(t *testing.T) {
		randomRefs := mp.AppendRandomPostingsRefs("pod", "b", 2, []storage.SeriesRef{42})
		require.Equal(t, []storage.SeriesRef{42, 3, 3}, randomRefs)
	})
}
