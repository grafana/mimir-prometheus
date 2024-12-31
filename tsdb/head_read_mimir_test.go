package tsdb

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
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
