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

package tsdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/compression"
)

func TestHead_LastSeriesRefAndPostingsListAccessors(t *testing.T) {
	head, _ := newTestHead(t, DefaultBlockDuration, compression.None, false)
	require.Equal(t, storage.SeriesRef(0), head.LastSeriesRef())

	app := head.AppenderV2(t.Context())
	firstRef, err := app.Append(0, labels.FromStrings("__name__", "up", "pod", "a"), 0, 100, 1, nil, nil, storage.AOptions{})
	require.NoError(t, err)
	secondRef, err := app.Append(0, labels.FromStrings("__name__", "up", "pod", "b"), 0, 100, 1, nil, nil, storage.AOptions{})
	require.NoError(t, err)
	_, err = app.Append(firstRef, labels.FromStrings("__name__", "up", "pod", "a"), 0, 200, 1, nil, nil, storage.AOptions{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	require.Greater(t, secondRef, firstRef)
	require.Equal(t, secondRef, head.LastSeriesRef())

	require.Equal(t, 2, head.PostingsLength("__name__", "up"))
	randomRefs := head.AppendRandomPostingsRefs("__name__", "up", 4, nil)
	require.Len(t, randomRefs, 4)
	for _, ref := range randomRefs {
		require.Contains(t, []storage.SeriesRef{firstRef, secondRef}, ref)
	}

	require.Equal(t, 1, head.PostingsLength("pod", "b"))
	require.Equal(t, []storage.SeriesRef{secondRef}, head.AppendRandomPostingsRefs("pod", "b", 1, nil))

	require.Zero(t, head.PostingsLength("pod", "missing"))
	require.Empty(t, head.AppendRandomPostingsRefs("pod", "missing", 1, nil))
}
