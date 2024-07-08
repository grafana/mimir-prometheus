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
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/f4b2babd2bc1cf0a2d66bab4e579ca35b6202338/bits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright 2023 Dolthub, Inc.

//go:build !amd64 || nosimd

package tsdb

import (
	"math/bits"
	"unsafe"
)

const (
	groupSize                    = 8
	maxAvgSeriesHashmapGroupLoad = 7

	loBits uint64 = 0x0101010101010101
	hiBits uint64 = 0x8080808080808080
)

type bitset uint64

func metaMatchH2(m *hashMeta, h h2) bitset {
	// https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
	return hasZeroByte(castUint64(m) ^ (loBits * uint64(h)))
}

func metaMatchEmpty(m *hashMeta) bitset {
	return hasZeroByte(castUint64(m))
}

func nextMatch(b *bitset) uint32 {
	s := uint32(bits.TrailingZeros64(uint64(*b)))
	*b &= ^(1 << s) // clear bit |s|
	return s >> 3   // div by 8
}

func hasZeroByte(x uint64) bitset {
	return bitset(((x - loBits) & ^(x)) & hiBits)
}

func castUint64(m *hashMeta) uint64 {
	return *(*uint64)((unsafe.Pointer)(m))
}
