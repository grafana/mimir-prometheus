// Copyright 2021 Grafana Labs
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

package hashcache

import (
	"sync"

	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/storage"
)

const (
	numGenerations = 4

	// approxBytesPerEntry is the estimated memory footprint (in bytes) of 1 cache
	// entry, measured with TestSeriesHashCache_MeasureApproximateSizePerEntry().
	approxBytesPerEntry = 36
)

// SeriesHashCache is a bounded cache mapping the per-block series ID with
// its labels hash.
type SeriesHashCache struct {
	maxEntriesPerGeneration uint64

	generations cacheGeneration
}

func NewSeriesHashCache(maxBytes uint64) *SeriesHashCache {
	maxEntriesPerGeneration := maxBytes / approxBytesPerEntry / numGenerations
	if maxEntriesPerGeneration < 1 {
		maxEntriesPerGeneration = 1
	}

	c := &SeriesHashCache{maxEntriesPerGeneration: maxEntriesPerGeneration}

	// Init generations.
	for idx := 0; idx < numGenerations; idx++ {
		c.generations.blocks = &sync.Map{}
		c.generations.length = atomic.NewUint64(0)
	}

	return c
}

// GetBlockCache returns a reference to the series hash cache for the provided blockID.
// The returned cache reference should be retained only for a short period (ie. the duration
// of the execution of 1 single query).
func (c *SeriesHashCache) GetBlockCache(blockID string) *BlockSeriesHashCache {
	blockCache := &BlockSeriesHashCache{}

	gen := c.generations

	if value, ok := gen.blocks.Load(blockID); ok {
		blockCache.generations = value.(*blockCacheGeneration)
	} else {
		// Create a new per-block cache only for the current generation.
		// If the cache for the older generation doesn't exist, then its
		// value will be null and skipped when reading.
		value, _ := gen.blocks.LoadOrStore(blockID, newBlockCacheGeneration(gen.length))
		blockCache.generations = value.(*blockCacheGeneration)
	}

	return blockCache
}

// GetBlockCacheProvider returns a cache provider bounded to the provided blockID.
func (c *SeriesHashCache) GetBlockCacheProvider(blockID string) *BlockSeriesHashCacheProvider {
	return NewBlockSeriesHashCacheProvider(c, blockID)
}

// cacheGeneration holds a multi-blocks cache generation.
type cacheGeneration struct {
	// blocks maps the block ID with blockCacheGeneration.
	blocks *sync.Map

	// Keeps track of the number of items added to the cache. This counter
	// is passed to each blockCacheGeneration belonging to this generation.
	length *atomic.Uint64
}

// blockCacheGeneration holds a per-block cache generation.
type blockCacheGeneration struct {
	// hashes maps per-block series ID with its hash.
	hashes map[storage.SeriesRef]uint64

	// Keeps track of the number of items added to the cache. This counter is
	// shared with all blockCacheGeneration in the "parent" cacheGeneration.
	length *atomic.Uint64
}

func newBlockCacheGeneration(length *atomic.Uint64) *blockCacheGeneration {
	return &blockCacheGeneration{
		hashes: make(map[storage.SeriesRef]uint64),
		length: length,
	}
}

type BlockSeriesHashCache struct {
	generations *blockCacheGeneration
}

// Fetch the hash of the given seriesID from the cache and returns a boolean
// whether the series was found in the cache or not.
func (c *BlockSeriesHashCache) Fetch(seriesID storage.SeriesRef) (uint64, bool) {
	gen := c.generations

	// Skip if the cache doesn't exist for this generation.
	if gen == nil {
		return 0, false
	}

	value, ok := gen.hashes[seriesID]

	if ok {
		return value, true
	}

	return 0, false
}

// Store the hash of the given seriesID in the cache.
func (c *BlockSeriesHashCache) Store(seriesID storage.SeriesRef, hash uint64) {
	// Store it in the most recent generation (index 0).
	gen := c.generations

	gen.hashes[seriesID] = hash

	gen.length.Add(1)
}

type BlockSeriesHashCacheProvider struct {
	cache   *SeriesHashCache
	blockID string
}

// NewBlockSeriesHashCacheProvider makes a new BlockSeriesHashCacheProvider.
func NewBlockSeriesHashCacheProvider(cache *SeriesHashCache, blockID string) *BlockSeriesHashCacheProvider {
	return &BlockSeriesHashCacheProvider{
		cache:   cache,
		blockID: blockID,
	}
}

// SeriesHashCache returns a reference to the cache bounded to block provided
// to NewBlockSeriesHashCacheProvider().
func (p *BlockSeriesHashCacheProvider) SeriesHashCache() *BlockSeriesHashCache {
	return p.cache.GetBlockCache(p.blockID)
}
