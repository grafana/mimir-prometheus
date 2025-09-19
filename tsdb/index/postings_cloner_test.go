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

// This file contains methods and types in addition to what's already in postings.go to provide compatibility
// with non-prometheus code. These are in a separate file to reduce the diff with Prometheus and make conflicts less likely.

package index

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/storage"
)

func TestPostingsCloner(t *testing.T) {
	for _, tc := range []struct {
		name  string
		check func(testing.TB, *PostingsCloner)
	}{
		{
			name: "seek beyond highest value of postings, then other clone seeks higher",
			check: func(t testing.TB, pc *PostingsCloner) {
				p1 := pc.Clone(context.TODO())
				require.False(t, p1.Seek(9))

				p2 := pc.Clone(context.TODO())
				require.False(t, p2.Seek(10))
			},
		},
		{
			name: "seek beyond highest value of postings, then other clone seeks lower",
			check: func(t testing.TB, pc *PostingsCloner) {
				p1 := pc.Clone(context.TODO())
				require.False(t, p1.Seek(9))

				p2 := pc.Clone(context.TODO())
				require.True(t, p2.Seek(2))
				require.Equal(t, storage.SeriesRef(2), p2.At())
			},
		},
		{
			name: "seek to posting with value 3 or higher",
			check: func(t testing.TB, pc *PostingsCloner) {
				p := pc.Clone(context.TODO())
				require.True(t, p.Seek(3))
				require.Equal(t, storage.SeriesRef(4), p.At())
				require.True(t, p.Seek(4))
				require.Equal(t, storage.SeriesRef(4), p.At())
			},
		},
		{
			name: "seek alternatively on different postings",
			check: func(t testing.TB, pc *PostingsCloner) {
				p1 := pc.Clone(context.TODO())
				require.True(t, p1.Seek(1))
				require.Equal(t, storage.SeriesRef(1), p1.At())

				p2 := pc.Clone(context.TODO())
				require.True(t, p2.Seek(2))
				require.Equal(t, storage.SeriesRef(2), p2.At())

				p3 := pc.Clone(context.TODO())
				require.True(t, p3.Seek(4))
				require.Equal(t, storage.SeriesRef(4), p3.At())

				p4 := pc.Clone(context.TODO())
				require.True(t, p4.Seek(5))
				require.Equal(t, storage.SeriesRef(8), p4.At())

				require.True(t, p1.Seek(3))
				require.Equal(t, storage.SeriesRef(4), p1.At())
				require.True(t, p1.Seek(4))
				require.Equal(t, storage.SeriesRef(4), p1.At())
			},
		},
		{
			name: "iterate through the postings",
			check: func(t testing.TB, pc *PostingsCloner) {
				p1 := pc.Clone(context.TODO())
				p2 := pc.Clone(context.TODO())

				// both one step
				require.True(t, p1.Next())
				require.Equal(t, storage.SeriesRef(1), p1.At())
				require.True(t, p2.Next())
				require.Equal(t, storage.SeriesRef(1), p2.At())

				require.True(t, p1.Next())
				require.Equal(t, storage.SeriesRef(2), p1.At())
				require.True(t, p1.Next())
				require.Equal(t, storage.SeriesRef(4), p1.At())
				require.True(t, p1.Next())
				require.Equal(t, storage.SeriesRef(8), p1.At())
				require.False(t, p1.Next())

				require.True(t, p2.Next())
				require.Equal(t, storage.SeriesRef(2), p2.At())
				require.True(t, p2.Next())
				require.Equal(t, storage.SeriesRef(4), p2.At())
			},
		},
		{
			name: "ensure a failed seek doesn't allow more next calls",
			check: func(t testing.TB, pc *PostingsCloner) {
				p := pc.Clone(context.TODO())
				require.False(t, p.Seek(9))
				require.False(t, p.Next())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc := NewPostingsCloner(newListPostings(1, 2, 4, 8))
			tc.check(t, pc)
		})
	}

	t.Run("cloning an err postings", func(t *testing.T) {
		expectedErr := errors.New("foobar")
		pc := NewPostingsCloner(ErrPostings(expectedErr))
		p := pc.Clone(context.TODO())
		require.False(t, p.Next())
		require.Equal(t, expectedErr, p.Err())
	})
}
