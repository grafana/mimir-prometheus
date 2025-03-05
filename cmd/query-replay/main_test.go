package main

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// FilteredQueryLogIterator wraps a slice of queries and only returns every Nth entry
type FilteredQueryLogIterator struct {
	entries  []ParsedQuery
	n        int // Return every nth entry
	counter  int // Current count
	position int // Current position in entries
}

// NewFilteredQueryLogIterator creates a new iterator that returns every nth entry
func NewFilteredQueryLogIterator(entries []ParsedQuery, n int) *FilteredQueryLogIterator {
	return &FilteredQueryLogIterator{
		entries: entries,
		n:       n,
	}
}

// Next advances to the next nth entry
func (it *FilteredQueryLogIterator) Next() bool {
	for it.position < len(it.entries) {
		it.counter++
		it.position++
		if it.counter == it.n {
			it.counter = 0
			return true
		}
	}
	return false
}

// At returns the current entry
func (it *FilteredQueryLogIterator) At() ParsedQuery {
	return it.entries[it.position-1]
}

// Err returns any error that occurred during iteration
func (it *FilteredQueryLogIterator) Err() error {
	return nil
}

var metrics = tsdb.NewPostingsForMatchersCacheMetrics(prometheus.DefaultRegisterer)

// Create TSDB options similar to Mimir's configuration
var tsdbOpts = &tsdb.Options{
	// Core settings
	RetentionDuration: int64(15 * 24 * time.Hour / time.Millisecond), // 15 days retention
	MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),       // 2h blocks
	MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),       // 2h blocks
	NoLockfile:        true,

	// Performance-related settings
	StripeSize:        16384,
	IsolationDisabled: true,
	EnableSharding:    true,

	// Caching settings
	HeadPostingsForMatchersCacheTTL:       5 * time.Minute,
	HeadPostingsForMatchersCacheMaxItems:  1,                 // Disable cache
	HeadPostingsForMatchersCacheMaxBytes:  350 * 1024 * 1024, // 350MB
	HeadPostingsForMatchersCacheForce:     true,
	BlockPostingsForMatchersCacheTTL:      5 * time.Minute,
	BlockPostingsForMatchersCacheMaxItems: 1,
	BlockPostingsForMatchersCacheMaxBytes: 350 * 1024 * 1024, // 350MB
	BlockPostingsForMatchersCacheForce:    true,

	// Disable features we don't need for benchmarking
	EnableMemorySnapshotOnShutdown: false,
	EnableOverlappingCompaction:    false,

	HeadChunksWriteBufferSize:            4 * 1024 * 1024,
	HeadChunksEndTimeVariance:            0,
	WALCompression:                       wlog.CompressionNone,
	WALSegmentSize:                       128 * 1024 * 1024,
	WALReplayConcurrency:                 0,
	EnableExemplarStorage:                true, // enable for everyone so we can raise the limit later
	MaxExemplars:                         int64(10000),
	EnableBiggerOOOBlockForOldSamples:    false,
	HeadChunksWriteQueueSize:             1000000,
	OutOfOrderTimeWindow:                 (24 * time.Hour).Milliseconds(), // The unit must be same as our timestamps.
	OutOfOrderCapMax:                     32,
	TimelyCompaction:                     false,
	HeadPostingsForMatchersCacheMetrics:  metrics,
	BlockPostingsForMatchersCacheMetrics: metrics,
	EnableNativeHistograms:               true,
	EnableOOONativeHistograms:            true,
	SecondaryHashFunction: func(ls labels.Labels) uint32 {
		const offset32 = 2166136261
		const prime32 = 16777619
		HashAdd32 := func(h uint32, s string) uint32 {
			for i := 0; i < len(s); i++ {
				h *= prime32
				h ^= uint32(s[i])
			}
			return h
		}

		const userID = "417760"
		h := HashAdd32(offset32, userID)
		ls.Range(func(l labels.Label) {
			h = HashAdd32(h, l.Name)
			h = HashAdd32(h, l.Value)
		})
		return h
	},
}

func BenchmarkQueryReplay(b *testing.B) {

	const testDataDir = "/users/dimitar/proba/2025-03-postings-for-matchers-hackathon/"

	// Open TSDB
	db, err := tsdb.Open(testDataDir+"/ingester-zone-a-48", nil, nil, tsdbOpts, nil)
	if err != nil {
		b.Fatalf("Error opening TSDB: %v", err)
	}
	db.DisableCompactions() // don't compact while we're running the benchmark
	defer db.Close()
	//b.Log("TSDB opened")

	// Pre-load all queries once
	var queries []ParsedQuery
	baseIterator, err := NewQueryLogIterator(testDataDir + "/query-logs.json")
	if err != nil {
		b.Fatalf("Error creating iterator: %v", err)
	}

	for baseIterator.Next() {
		queries = append(queries, baseIterator.At())
	}
	if err := baseIterator.Err(); err != nil {
		b.Fatalf("Error during iteration: %v", err)
	}
	//b.Log("Loaded", len(queries), "queries")
	testCases := map[string]struct {
		sampleRate int // How often to sample (every Nth entry)
	}{
		"0.1pct":   {sampleRate: 100},
		"0.01pct":  {sampleRate: 1000},
		"0.001pct": {sampleRate: 10000},
	}

	for name, bc := range testCases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				queryCount := 0
				seriesCount := 0

				iterator := NewFilteredQueryLogIterator(queries, bc.sampleRate)
				for iterator.Next() {
					entry := iterator.At()
					queryCount++

					querier, err := db.Querier(entry.Start.UnixMilli(), entry.End.UnixMilli())
					if err != nil {
						b.Fatalf("Error creating querier: %v", err)
					}

					// Execute each vector selector query
					for _, matchers := range entry.VectorSelectors {
						count, err := runQuery(querier, matchers)
						if err != nil {
							b.Fatalf("Error iterating series set: %v", err)
						}
						seriesCount += count
					}

					querier.Close()
				}
			}
		})
	}
}

// Helper function to run a single query and return the number of series
func runQuery(querier storage.Querier, matchers []*labels.Matcher) (int, error) {
	seriesSet := querier.Select(context.Background(), true, nil, matchers...)
	count := 0
	for seriesSet.Next() {
		count++
	}
	if err := seriesSet.Err(); err != nil {
		return 0, err
	}
	return count, nil
}
