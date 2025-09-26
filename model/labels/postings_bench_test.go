// Copyright 2025 Grafana Labs
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

package labels_test

import (
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

// Benchmark iterating over postings lists of different sizes
func BenchmarkIteratePostings(b *testing.B) {
	// Helper function to create a slice of SeriesRef with given size
	createSeriesRefs := func(size int) []storage.SeriesRef {
		refs := make([]storage.SeriesRef, size)
		for i := 0; i < size; i++ {
			refs[i] = storage.SeriesRef(i + 1) // SeriesRef 0 is invalid, start from 1
		}
		return refs
	}

	benchmarks := []struct {
		name string
		size int
	}{
		{"Size_1", 1},
		{"Size_32", 32},
		{"Size_128", 128},
		{"Size_512", 512},
		{"Size_2048", 2048},
		{"Size_32K", 32 * 1024},
		{"Size_64K", 64 * 1024},
		{"Size_128K", 128 * 1024},
		{"Size_256K", 256 * 1024},
		{"Size_512K", 512 * 1024},
		{"Size_1M", 1024 * 1024},
	}

	for _, bm := range benchmarks {
		refs := createSeriesRefs(bm.size)

		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			var count int
			for i := 0; i < b.N; i++ {
				// Reset postings for each iteration
				p := index.NewListPostings(refs)
				count = 0
				for p.Next() {
					_ = p.At() // Access the current value
					count++
				}
			}
			if count != bm.size {
				b.Fatalf("unexpected count: got %d, want %d", count, bm.size)
			}
		})
	}
}
