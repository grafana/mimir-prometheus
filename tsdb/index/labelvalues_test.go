package index

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestReader_LabelValuesFor(t *testing.T) {
	ctx := context.Background()

	fn := filepath.Join(t.TempDir(), indexFilename)

	iw, err := NewWriter(ctx, fn)
	require.NoError(t, err)

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "1", "b", "3"),
		labels.FromStrings("a", "1", "b", "4"),
		labels.FromStrings("a", "2", "b", "5"),
	}

	require.NoError(t, iw.AddSymbol("1"))
	require.NoError(t, iw.AddSymbol("2"))
	require.NoError(t, iw.AddSymbol("3"))
	require.NoError(t, iw.AddSymbol("4"))
	require.NoError(t, iw.AddSymbol("5"))
	require.NoError(t, iw.AddSymbol("a"))
	require.NoError(t, iw.AddSymbol("b"))

	// Postings lists are only written if a series with the respective
	// reference was added before.
	for i, s := range series {
		require.NoError(t, iw.AddSeries(storage.SeriesRef(i+1), s))
	}
	require.NoError(t, iw.Close())

	ir, err := NewFileReader(fn)
	require.NoError(t, err)

	// Obtain postings where a=1
	p, err := ir.Postings(ctx, "a", "1")
	require.NoError(t, err)

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		it := ir.LabelValuesFor(p, "b")
		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that b=5 is filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"1", "2", "3", "4"}, vals)
	})

	t.Run("empty result set", func(t *testing.T) {
		it := ir.LabelValuesFor(p, "c")
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}

func TestReader_LabelValuesExcluding(t *testing.T) {
	ctx := context.Background()

	fn := filepath.Join(t.TempDir(), indexFilename)

	iw, err := NewWriter(ctx, fn)
	require.NoError(t, err)

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "1", "b", "3"),
		labels.FromStrings("a", "1", "b", "4"),
		// Include this to verify that b=5 is still found, even if series with a=1 are excluded
		labels.FromStrings("a", "1", "b", "5"),
		// This should be the only value of 5 found, since a!=1
		labels.FromStrings("a", "2", "b", "5"),
	}

	require.NoError(t, iw.AddSymbol("1"))
	require.NoError(t, iw.AddSymbol("2"))
	require.NoError(t, iw.AddSymbol("3"))
	require.NoError(t, iw.AddSymbol("4"))
	require.NoError(t, iw.AddSymbol("5"))
	require.NoError(t, iw.AddSymbol("a"))
	require.NoError(t, iw.AddSymbol("b"))

	// Postings lists are only written if a series with the respective
	// reference was added before.
	for i, s := range series {
		require.NoError(t, iw.AddSeries(storage.SeriesRef(i+1), s))
	}
	require.NoError(t, iw.Close())

	ir, err := NewFileReader(fn)
	require.NoError(t, err)

	// Obtain postings where a=1
	p, err := ir.Postings(ctx, "a", "1")
	require.NoError(t, err)

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		it := ir.LabelValuesExcluding(p, "b")
		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that only b=5 is not filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"5"}, vals)
	})

	t.Run("empty result set", func(t *testing.T) {
		it := ir.LabelValuesExcluding(p, "c")
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}

func TestMemPostings_LabelValuesFor(t *testing.T) {
	mp := NewMemPostings()
	mp.Add(1, labels.FromStrings("a", "1", "b", "1"))
	mp.Add(2, labels.FromStrings("a", "1", "b", "2"))
	mp.Add(3, labels.FromStrings("a", "1", "b", "3"))
	mp.Add(4, labels.FromStrings("a", "1", "b", "4"))
	mp.Add(5, labels.FromStrings("a", "2", "b", "5"))
	p := mp.Get("a", "1")

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		it := mp.LabelValuesFor(p, "b")

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that b=5 is filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"1", "2", "3", "4"}, vals)
	})

	t.Run("empty result set", func(t *testing.T) {
		it := mp.LabelValuesFor(p, "c")
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}

func TestMemPostings_LabelValuesExcluding(t *testing.T) {
	mp := NewMemPostings()
	mp.Add(1, labels.FromStrings("a", "1", "b", "1"))
	mp.Add(2, labels.FromStrings("a", "1", "b", "2"))
	mp.Add(3, labels.FromStrings("a", "1", "b", "3"))
	mp.Add(4, labels.FromStrings("a", "1", "b", "4"))
	// Include this to verify that b=5 is still found, even if series with a=1 are excluded
	mp.Add(5, labels.FromStrings("a", "1", "b", "5"))
	// This should be the only value of 5 found, since a!=1
	mp.Add(6, labels.FromStrings("a", "2", "b", "5"))
	p := mp.Get("a", "1")

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		it := mp.LabelValuesExcluding(p, "b")

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that only b=5 is not filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"5"}, vals)
	})

	t.Run("empty result set", func(t *testing.T) {
		it := mp.LabelValuesExcluding(p, "c")
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}
