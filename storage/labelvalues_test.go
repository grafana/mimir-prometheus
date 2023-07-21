package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListLabelValues(t *testing.T) {
	t.Run("lets you traverse a slice of label values", func(t *testing.T) {
		input := []string{"a", "b", "c", "d"}
		it := NewListLabelValues(input, nil)
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})
		var vals []string
		for it.Next() {
			vals = append(vals, it.At())
		}

		require.Equal(t, input, vals)
		require.Empty(t, it.Warnings())
		require.NoError(t, it.Err())
	})

	t.Run("can be initialized with an empty slice", func(t *testing.T) {
		it := NewListLabelValues([]string{}, nil)
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.Empty(t, it.Warnings())
		require.NoError(t, it.Err())
	})

	t.Run("can be initialized with a nil slice", func(t *testing.T) {
		it := NewListLabelValues(nil, nil)
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.Empty(t, it.Warnings())
		require.NoError(t, it.Err())
	})
}

func TestErrLabelValues(t *testing.T) {
	t.Run("is initialized with an error", func(t *testing.T) {
		it := ErrLabelValues(fmt.Errorf("test"))
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.EqualError(t, it.Err(), "test")
		require.Empty(t, it.Warnings())
	})

	t.Run("cannot be initialized with a nil error", func(t *testing.T) {
		it := ErrLabelValues(nil)
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.EqualError(t, it.Err(), "nil error provided")
		require.Empty(t, it.Warnings())
	})
}

func TestEmptyLabelValues(t *testing.T) {
	it := EmptyLabelValues()
	t.Cleanup(func() {
		require.NoError(t, it.Close())
	})

	require.False(t, it.Next())
	require.Empty(t, it.Warnings())
	require.NoError(t, it.Err())
}
