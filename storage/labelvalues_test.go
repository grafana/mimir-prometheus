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

package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListLabelValues(t *testing.T) {
	t.Run("lets you traverse a slice of label values", func(t *testing.T) {
		input := []string{"a", "b", "c", "d"}
		it := NewListLabelValues(input)
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
		it := NewListLabelValues([]string{})
		t.Cleanup(func() {
			require.NoError(t, it.Close())
		})

		require.False(t, it.Next())
		require.Empty(t, it.Warnings())
		require.NoError(t, it.Err())
	})

	t.Run("can be initialized with a nil slice", func(t *testing.T) {
		it := NewListLabelValues(nil)
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
		it := ErrLabelValues(errors.New("test"))
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
