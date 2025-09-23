// Copyright 2023 Grafana Labs
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
		labels.FromStrings("d", "1"),
	}

	require.NoError(t, iw.AddSymbol("1"))
	require.NoError(t, iw.AddSymbol("2"))
	require.NoError(t, iw.AddSymbol("3"))
	require.NoError(t, iw.AddSymbol("4"))
	require.NoError(t, iw.AddSymbol("5"))
	require.NoError(t, iw.AddSymbol("a"))
	require.NoError(t, iw.AddSymbol("b"))
	require.NoError(t, iw.AddSymbol("d"))

	// Postings lists are only written if a series with the respective
	// reference was added before.
	for i, s := range series {
		require.NoError(t, iw.AddSeries(storage.SeriesRef(i+1), s))
	}
	require.NoError(t, iw.Close())

	ir, err := NewFileReader(fn, DecodePostingsRaw)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ir.Close())
	})

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		// Obtain postings where a=1
		p, err := ir.Postings(ctx, "a", "1")
		require.NoError(t, err)

		it := ir.LabelValuesFor(p, "b")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})
		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that b=5 is filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"1", "2", "3", "4"}, vals)
	})

	t.Run("requesting a non-existent label value", func(t *testing.T) {
		// Obtain postings where a=1
		p, err := ir.Postings(ctx, "a", "1")
		require.NoError(t, err)

		it := ir.LabelValuesFor(p, "c")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})

	t.Run("filtering based on empty postings", func(t *testing.T) {
		it := ir.LabelValuesFor(EmptyPostings(), "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})

	t.Run("filtering based on a postings set missing the label", func(t *testing.T) {
		// Obtain postings where d=1
		p, err := ir.Postings(ctx, "d", "1")
		require.NoError(t, err)

		it := ir.LabelValuesFor(p, "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})
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
		labels.FromStrings("d", "1"),
	}

	require.NoError(t, iw.AddSymbol("1"))
	require.NoError(t, iw.AddSymbol("2"))
	require.NoError(t, iw.AddSymbol("3"))
	require.NoError(t, iw.AddSymbol("4"))
	require.NoError(t, iw.AddSymbol("5"))
	require.NoError(t, iw.AddSymbol("a"))
	require.NoError(t, iw.AddSymbol("b"))
	require.NoError(t, iw.AddSymbol("d"))

	// Postings lists are only written if a series with the respective
	// reference was added before.
	for i, s := range series {
		require.NoError(t, iw.AddSeries(storage.SeriesRef(i+1), s))
	}
	require.NoError(t, iw.Close())

	ir, err := NewFileReader(fn, DecodePostingsRaw)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ir.Close())
	})

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		// Obtain postings where a=1
		p, err := ir.Postings(ctx, "a", "1")
		require.NoError(t, err)

		it := ir.LabelValuesExcluding(p, "b")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})
		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that only b=5 is not filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"5"}, vals)
	})

	t.Run("requesting a non-existent label value", func(t *testing.T) {
		// Obtain postings where a=1
		p, err := ir.Postings(ctx, "a", "1")
		require.NoError(t, err)

		it := ir.LabelValuesExcluding(p, "c")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})

	t.Run("filtering based on empty postings", func(t *testing.T) {
		it := ir.LabelValuesExcluding(EmptyPostings(), "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		require.Equal(t, []string{"1", "2"}, vals)
	})

	t.Run("filtering based on a postings set missing the label", func(t *testing.T) {
		// Obtain postings where d=1
		p, err := ir.Postings(ctx, "d", "1")
		require.NoError(t, err)

		it := ir.LabelValuesExcluding(p, "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		require.Equal(t, []string{"1", "2"}, vals)
	})
}

func TestMemPostings_LabelValuesFor(t *testing.T) {
	mp := NewMemPostings()
	mp.Add(1, labels.FromStrings("a", "1", "b", "1"))
	mp.Add(2, labels.FromStrings("a", "1", "b", "2"))
	mp.Add(3, labels.FromStrings("a", "1", "b", "3"))
	mp.Add(4, labels.FromStrings("a", "1", "b", "4"))
	mp.Add(5, labels.FromStrings("a", "2", "b", "5"))
	mp.Add(6, labels.FromStrings("d", "1"))

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		p := mp.Postings(context.Background(), "a", "1")

		it := mp.LabelValuesFor(p, "b")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that b=5 is filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"1", "2", "3", "4"}, vals)
	})

	t.Run("requesting a non-existent label value", func(t *testing.T) {
		p := mp.Postings(context.Background(), "a", "1")

		it := mp.LabelValuesFor(p, "c")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})

	t.Run("filtering based on empty postings", func(t *testing.T) {
		it := mp.LabelValuesFor(EmptyPostings(), "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})

	t.Run("filtering based on a postings set missing the label", func(t *testing.T) {
		p := mp.Postings(context.Background(), "d", "1")

		it := mp.LabelValuesFor(p, "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

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
	mp.Add(7, labels.FromStrings("d", "1"))

	t.Run("filtering based on non-empty postings", func(t *testing.T) {
		p := mp.Postings(context.Background(), "a", "1")

		it := mp.LabelValuesExcluding(p, "b")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		// Note that only b=5 is not filtered out, since it has a=2, while our postings have a=1
		require.Equal(t, []string{"5"}, vals)
	})

	t.Run("requesting a non-existent label value", func(t *testing.T) {
		p := mp.Postings(context.Background(), "a", "1")

		it := mp.LabelValuesExcluding(p, "c")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
	})

	t.Run("filtering based on empty postings", func(t *testing.T) {
		it := mp.LabelValuesExcluding(EmptyPostings(), "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		require.Equal(t, []string{"1", "2"}, vals)
	})

	t.Run("filtering based on a postings set missing the label", func(t *testing.T) {
		p := mp.Postings(context.Background(), "d", "1")

		it := mp.LabelValuesExcluding(p, "a")
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}
		require.NoError(t, it.Err())
		require.Empty(t, it.Warnings())
		require.Equal(t, []string{"1", "2"}, vals)
	})
}
