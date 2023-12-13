package index

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
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
	require.NoError(t, iw.AddSeries(1, series[0]))
	require.NoError(t, iw.AddSeries(2, series[1]))
	require.NoError(t, iw.AddSeries(3, series[2]))
	require.NoError(t, iw.AddSeries(4, series[3]))
	require.NoError(t, iw.AddSeries(5, series[4]))

	require.NoError(t, iw.Close())

	ir, err := NewFileReader(fn)
	require.NoError(t, err)

	// Obtain postings where a=1
	p, err := ir.Postings(ctx, "a", "1")
	require.NoError(t, err)

	it := ir.LabelValuesFor(p, "b")
	var vals []string
	for it.Next() {
		vals = append(vals, it.At())
	}
	require.NoError(t, it.Err())
	require.Empty(t, it.Warnings())
	// Note that b=5 is filtered out, since it has a=2, while our postings have a=1
	require.Equal(t, []string{"1", "2", "3", "4"}, vals)

	t.Run("empty result set", func(t *testing.T) {
		it := ir.LabelValuesFor(p, "c")
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}

func TestMemPostings_LabelValuesFor(t *testing.T) {
	mp := NewMemPostings()
	mp.Add(1, labels.FromStrings("a", "1"))
	mp.Add(1, labels.FromStrings("b", "1"))
	mp.Add(2, labels.FromStrings("a", "1"))
	mp.Add(2, labels.FromStrings("b", "2"))
	mp.Add(3, labels.FromStrings("a", "1"))
	mp.Add(3, labels.FromStrings("b", "3"))
	mp.Add(4, labels.FromStrings("a", "1"))
	mp.Add(4, labels.FromStrings("b", "4"))
	mp.Add(5, labels.FromStrings("a", "2"))
	mp.Add(5, labels.FromStrings("b", "5"))
	p := mp.Get("a", "1")

	it := mp.LabelValuesFor(p, "b")

	var vals []string
	for it.Next() {
		vals = append(vals, it.At())
	}
	require.NoError(t, it.Err())
	require.Empty(t, it.Warnings())
	// Note that b=5 is filtered out, since it has a=2, while our postings have a=1
	require.Equal(t, []string{"1", "2", "3", "4"}, vals)

	t.Run("empty result set", func(t *testing.T) {
		it := mp.LabelValuesFor(p, "c")
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}

func TestMemPostings_LabelValuesStream(t *testing.T) {
	ctx := context.Background()
	mp := NewMemPostings()
	mp.Add(1, labels.FromStrings("a", "1"))
	mp.Add(1, labels.FromStrings("b", "1"))
	mp.Add(2, labels.FromStrings("a", "1"))
	mp.Add(2, labels.FromStrings("b", "2"))
	mp.Add(3, labels.FromStrings("a", "1"))
	mp.Add(3, labels.FromStrings("b", "3"))
	mp.Add(4, labels.FromStrings("a", "1"))
	mp.Add(4, labels.FromStrings("b", "4"))
	mp.Add(5, labels.FromStrings("a", "2"))
	mp.Add(5, labels.FromStrings("b", "5"))

	t.Run("without matchers", func(t *testing.T) {
		it := mp.LabelValuesStream(context.Background(), "b")

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		require.Equal(t, []string{"1", "2", "3", "4", "5"}, vals)
	})

	t.Run("with matchers", func(t *testing.T) {
		it := mp.LabelValuesStream(context.Background(), "b", labels.MustNewMatcher(labels.MatchRegexp, "b", "[2,3]"))

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		require.Equal(t, []string{"2", "3"}, vals)
	})

	// Matchers for other labels should be ignored.
	t.Run("with matchers for another label", func(t *testing.T) {
		it := mp.LabelValuesStream(context.Background(), "b", labels.MustNewMatcher(labels.MatchEqual, "a", "1"))

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		require.Equal(t, []string{"1", "2", "3", "4", "5"}, vals)
	})

	t.Run("non-existent label", func(t *testing.T) {
		it := mp.LabelValuesStream(ctx, "c")
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})
}
