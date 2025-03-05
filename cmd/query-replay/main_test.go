package main

import (
	"context"
	"fmt"
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

const (
	queryLogPath = "/users/dimitar/proba/2025-03-postings-for-matchers-hackathon/query-logs.json"
	//queryLogPath = "/Users/dimitar/proba/2025-03-postings-for-matchers-hackathon/selected-queries/8add3a5e7eadacedae63c401ea4668ec9e754a21d5a03c886f0752391154cc0f.json"
	tsdbPath = "/users/dimitar/proba/2025-03-postings-for-matchers-hackathon/ingester-zone-a-48"
)

func BenchmarkQueryReplay(b *testing.B) {
	//runtime.MemProfileRate = 16 * 1024

	// Pre-load all queries once
	var queries []ParsedQuery
	//baseIterator, err := NewQueryLogIterator(testDataDir + "/singe-bad-query.json")
	baseIterator, err := NewQueryLogIterator(queryLogPath)
	if err != nil {
		b.Fatalf("Error creating iterator: %v", err)
	}

	for baseIterator.Next() {
		queries = append(queries, baseIterator.At())
	}
	if err := baseIterator.Err(); err != nil {
		b.Fatalf("Error during iteration: %v", err)
	}
	// Open TSDB
	db, err := tsdb.Open(tsdbPath, nil, nil, tsdbOpts, nil)
	if err != nil {
		b.Fatalf("Error opening TSDB: %v", err)
	}
	db.DisableCompactions() // don't compact while we're running the benchmark
	defer db.Close()
	b.Log("TSDB opened")

	//exploreStats(b, db)
	//labelValuesDistribution(b, db)
	//return

	//b.Log("Loaded", len(queries), "queries")
	testCases := map[string]struct {
		sampleRate int // How often to sample (every Nth entry)
	}{
		//"1pct": {sampleRate: 1},
		"0.1pct": {sampleRate: 10},
		//"0.01pct":   {sampleRate: 100},
		//"0.001pct":  {sampleRate: 1000},
		//"0.0001pct": {sampleRate: 10000},
	}

	for name, bc := range testCases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				queryCount := 0

				iterator := NewFilteredQueryLogIterator(queries, bc.sampleRate)
				seriesCount := 0
				for iterator.Next() {
					entry := iterator.At()
					queryCount++

					querier, err := db.Querier(entry.Start.UnixMilli(), entry.End.UnixMilli())
					if err != nil {
						b.Fatalf("Error creating querier: %v", err)
					}

					//startTime := time.Now()
					// Execute each vector selector query
					for _, matchers := range entry.VectorSelectors {
						count, err := runQuery(nil, querier, matchers)
						if err != nil {
							b.Fatalf("Error iterating series set: %v", err)
						}
						seriesCount += count
					}
					//b.Logf("[%s]\tseries=%d\tduration=%s\n", entry.Fingerprint, seriesCount, time.Since(startTime))

					querier.Close()
				}
			}
		})
	}
}

func labelValuesDistribution(b *testing.B, db *tsdb.DB) {
	generalStats := db.Head().Stats("", 100) // Keep this for the top label names

	// For each top label name
	for _, s := range generalStats.IndexPostingStats.CardinalityLabelStats {
		// Get ALL values for this label name
		stats := db.Head().Stats(s.Name, 18000)

		// Print label name first

		// Print counts for each value, comma-separated
		for stadIdx, stat := range stats.IndexPostingStats.CardinalityMetricsStats {
			fmt.Printf("%s,%d,%d\n", s.Name, stadIdx, stat.Count)
		}
		fmt.Printf("\n")
	}
}

func exploreStats(b *testing.B, db *tsdb.DB) {
	// ix, err := db.Head().Index()
	// require.NoError(b, err)

	// labelNames, err := ix.LabelNames(context.Background())
	// require.NoError(b, err)

	generalStats := db.Head().Stats("", 100)

	printStats(generalStats)
	for _, s := range generalStats.IndexPostingStats.CardinalityLabelStats {
		topValuesCount := max(5, s.SupplimentaryCount/100)
		stats := db.Head().Stats(s.Name, int(topValuesCount))
		stats.IndexPostingStats.LabelValueStats = nil
		stats.IndexPostingStats.LabelValuePairsStats = nil
		stats.IndexPostingStats.CardinalityLabelStats = nil
		fmt.Printf("\nLabel Name: %s\n", s.Name)

		topValuesSeries := uint64(0)
		for _, stat := range stats.IndexPostingStats.CardinalityMetricsStats {
			topValuesSeries += stat.Count
		}
		fmt.Printf("Top 1%% Values Count: %d\n", topValuesCount)
		fmt.Printf("Top 1%% Values Series Count: %d (%.2f%%)\n", topValuesSeries, 100*float64(topValuesSeries)/float64(s.Count))
		fmt.Printf("Remaining Series Count: %d (%.2f%%)\n", s.Count-topValuesSeries, 100*float64(s.Count-topValuesSeries)/float64(s.Count))
		fmt.Printf("Remaining Labels Count: %d (%.2f%%)\n", s.SupplimentaryCount-topValuesCount, 100*float64(s.SupplimentaryCount-topValuesCount)/float64(s.SupplimentaryCount))
		fmt.Printf("Remaining Series Per Label: %.2f\n", float64(s.Count-topValuesSeries)/float64(s.SupplimentaryCount-topValuesCount))
		printStats(stats)
	}
}

func printStats(stats *tsdb.Stats) {
	if stats == nil {
		fmt.Println("No stats available")
		return
	}

	fmt.Printf("\nTSDB Stats Analysis:\n")

	if stats.IndexPostingStats == nil {
		return
	}
	if len(stats.IndexPostingStats.CardinalityMetricsStats) > 0 {
		fmt.Println("Top Label Values by Series Count:")
		for _, stat := range stats.IndexPostingStats.CardinalityMetricsStats {
			fmt.Printf("  %-50s %d\n", stat.Name, stat.Count)
		}
		fmt.Println()
	}

	if len(stats.IndexPostingStats.CardinalityLabelStats) > 0 {
		fmt.Println("Top Labels by Value Count:")
		for _, stat := range stats.IndexPostingStats.CardinalityLabelStats {
			fmt.Printf("  %-50s %d\n", stat.Name, stat.Count)
		}
		fmt.Println()
	}

	if len(stats.IndexPostingStats.LabelValueStats) > 0 {
		fmt.Println("Top Label Names by Series Count:")
		for _, stat := range stats.IndexPostingStats.LabelValueStats {
			fmt.Printf("  %-50s %d\n", stat.Name, stat.Count)
		}
		fmt.Println()
	}

	if len(stats.IndexPostingStats.LabelValuePairsStats) > 0 {
		fmt.Println("Top Label-Value Pairs by Series Count:")
		for _, stat := range stats.IndexPostingStats.LabelValuePairsStats {
			fmt.Printf("  %-50s %d\n", stat.Name, stat.Count)
		}
		fmt.Println()
	}
}

// Helper function to run a single query and return the number of series
func runQuery(stats *tsdb.Stats, querier storage.Querier, matchers []*labels.Matcher) (int, error) {
	seriesSet := querier.Select(context.WithValue(context.Background(), "stats", stats), true, nil, matchers...)
	count := 0
	for seriesSet.Next() {
		count++
	}
	if err := seriesSet.Err(); err != nil {
		return 0, err
	}
	return count, nil
}
