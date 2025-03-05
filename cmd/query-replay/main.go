package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// LogEntry represents the structure of our log entries
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Labels    struct {
		ParamQuery       string `json:"param_query"`
		TimeSinceMinTime string `json:"time_since_min_time"`
		TimeSinceMaxTime string `json:"time_since_max_time"`
	} `json:"labels"`
}

// ParsedQuery represents all the parsed information from a log entry
type ParsedQuery struct {
	Query           string
	Start           time.Time
	End             time.Time
	VectorSelectors [][]*labels.Matcher
	Fingerprint     string

	UnparsedEntry LogEntry
}

// ParseLogEntry processes a LogEntry and returns a structured ParsedQuery
func ParseLogEntry(entry LogEntry) (ParsedQuery, error) {
	// Parse the timestamp as our reference time
	refTime, err := time.Parse(time.RFC3339Nano, entry.Timestamp)
	if err != nil {
		return ParsedQuery{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// Parse the duration strings
	minOffset, err := time.ParseDuration(entry.Labels.TimeSinceMinTime)
	if err != nil {
		return ParsedQuery{}, fmt.Errorf("failed to parse time_since_min_time: %w", err)
	}

	maxOffset, err := time.ParseDuration(entry.Labels.TimeSinceMaxTime)
	if err != nil {
		return ParsedQuery{}, fmt.Errorf("failed to parse time_since_max_time: %w", err)
	}

	// Calculate start and end times
	start := refTime.Add(-minOffset)
	end := refTime.Add(-maxOffset)

	// Extract vector selectors from the query
	selectors, err := ExtractVectorSelectors(entry.Labels.ParamQuery)
	if err != nil {
		return ParsedQuery{}, fmt.Errorf("failed to extract vector selectors: %w", err)
	}

	fingerprint := fmt.Sprintf("%s-%s-%s", entry.Labels.ParamQuery, start.Format(time.RFC3339), end.Format(time.RFC3339))
	sha := sha256.Sum256([]byte(fingerprint))
	fingerprint = hex.EncodeToString(sha[:])

	return ParsedQuery{
		Fingerprint:     fingerprint,
		Query:           entry.Labels.ParamQuery,
		Start:           start,
		End:             end,
		VectorSelectors: selectors,
		UnparsedEntry:   entry,
	}, nil
}

// ExtractVectorSelectors takes a PromQL query string and returns a list of vector selectors
func ExtractVectorSelectors(query string) ([][]*labels.Matcher, error) {
	// Parse the PromQL query
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PromQL query: %w", err)
	}

	// Extract vector selectors
	selectors := parser.ExtractSelectors(expr)

	return selectors, nil
}

// QueryLogIterator provides iteration over query log entries
type QueryLogIterator struct {
	scanner *bufio.Scanner
	current ParsedQuery
	err     error
}

// NewQueryLogIterator creates a new iterator for the given file path
func NewQueryLogIterator(filepath string) (*QueryLogIterator, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &QueryLogIterator{
		scanner: bufio.NewScanner(file),
	}, nil
}

// Next advances the iterator to the next entry
func (it *QueryLogIterator) Next() bool {
	if !it.scanner.Scan() {
		return false
	}

	var entry LogEntry
	if err := json.Unmarshal([]byte(it.scanner.Text()), &entry); err != nil {
		it.err = fmt.Errorf("failed to parse JSON: %w", err)
		return false
	}

	parsed, err := ParseLogEntry(entry)
	if err != nil {
		it.err = fmt.Errorf("failed to parse log entry: %w", err)
		return false
	}

	it.current = parsed
	return true
}

// At returns the current entry
func (it *QueryLogIterator) At() ParsedQuery {
	return it.current
}

// Err returns any error that occurred during iteration
func (it *QueryLogIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.scanner.Err()
}

func main() {
	// Define command line flags
	inputFile := flag.String("file", "", "Path to the log file to parse")
	compareMode := flag.Bool("compare", false, "Run in comparison mode")
	file1 := flag.String("file1", "", "First performance log file (for comparison)")
	file2 := flag.String("file2", "", "Second performance log file (for comparison)")
	flag.Parse()

	if *compareMode {
		if *file1 == "" || *file2 == "" || *inputFile == "" {
			fmt.Println("In compare mode, please provide -file1, -file2, and -file (query log)")
			os.Exit(1)
		}
		if err := compareQueryPerformance(*file1, *file2, *inputFile); err != nil {
			fmt.Printf("Error comparing performance: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *inputFile == "" {
		fmt.Println("Please provide an input file using -file flag")
		os.Exit(1)
	}

	// Create iterator
	iterator, err := NewQueryLogIterator(*inputFile)
	if err != nil {
		fmt.Printf("Error creating iterator: %v\n", err)
		os.Exit(1)
	}

	// Iterate over entries
	for iterator.Next() {
		entry := iterator.At()

		fmt.Printf("Query: %s\n", entry.Query)
		fmt.Printf("Time Range: %s -> %s\n",
			entry.Start.Format(time.RFC3339),
			entry.End.Format(time.RFC3339))

		fmt.Println("Vector selectors found:")
		for _, selector := range entry.VectorSelectors {
			fmt.Printf("  %v\n", selector)
		}

		fmt.Println("---")
	}

	if err := iterator.Err(); err != nil {
		fmt.Printf("Error during iteration: %v\n", err)
		os.Exit(1)
	}
}
