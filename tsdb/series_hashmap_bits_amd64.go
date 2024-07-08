// Copyright 2024 The Prometheus Authors
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
//
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/f4b2babd2bc1cf0a2d66bab4e579ca35b6202338/bits_amd64.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright 2023 Dolthub, Inc.

//go:build amd64 && !nosimd

package tsdb

import (
	"math/bits"
	_ "unsafe"

	"github.com/prometheus/prometheus/tsdb/simd"
)

const (
	groupSize                    = 16
	maxAvgSeriesHashmapGroupLoad = 14
)

type bitset uint16

func metaMatchH2(m *hashMeta, h h2) bitset {
	b := simd.MatchMetadata((*[groupSize]uint8)(m), uint8(h))
	return bitset(b)
}

func metaMatchEmpty(m *hashMeta) bitset {
	b := simd.MatchEmpty((*[groupSize]uint8)(m))
	return bitset(b)
}

func nextMatch(b *bitset) (s uint32) {
	s = uint32(bits.TrailingZeros16(uint16(*b)))
	*b &= ^(1 << s) // clear bit |s|
	return
}
