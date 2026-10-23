// Copyright 2024 Grafana Labs
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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
)

func TestHead_MustIndex(t *testing.T) {
	series := []labels.Labels{
		labels.FromStrings("n", "1"),
		labels.FromStrings("n", "1", "i", "a"),
		labels.FromStrings("n", "1", "i", "b"),
		labels.FromStrings("n", "2"),
		labels.FromStrings("n", "2.5"),
	}
	opts := DefaultHeadOptions()
	opts.ChunkRange = 1000
	opts.ChunkDirRoot = t.TempDir()
	h, err := NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})
	app := h.Appender(context.Background())
	for _, s := range series {
		app.Append(0, s, 0, 0)
	}
	require.NoError(t, app.Commit())

	ir, err := h.Index()
	require.NoError(t, err)

	imr := h.MustIndex()
	require.Equal(t, ir, imr)
}
