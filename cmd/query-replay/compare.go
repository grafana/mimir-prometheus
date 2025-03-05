package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type QueryMetrics struct {
	Fingerprint string
	Series      int
	Duration    time.Duration
}

func parseLogLine(line string) (QueryMetrics, error) {
	// Extract fingerprint from between square brackets
	start := strings.Index(line, "[") + 1
	end := strings.Index(line, "]")
	if start == -1 || end == -1 {
		return QueryMetrics{}, fmt.Errorf("invalid log line format: %s", line)
	}
	fingerprint := line[start:end]

	// Parse series count and duration
	var series int
	var durationStr string
	_, err := fmt.Sscanf(line[end+1:], "\tseries=%d\tduration=%s", &series, &durationStr)
	if err != nil {
		return QueryMetrics{}, fmt.Errorf("error parsing metrics: %w", err)
	}

	// Parse duration string
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return QueryMetrics{}, fmt.Errorf("error parsing duration: %w", err)
	}

	return QueryMetrics{
		Fingerprint: fingerprint,
		Series:      series,
		Duration:    duration,
	}, nil
}

func loadMetrics(filename string) (map[string]QueryMetrics, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	metrics := make(map[string]QueryMetrics)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, "main_test.go:190:") {
			continue
		}

		metric, err := parseLogLine(line)
		if err != nil {
			return nil, err
		}
		metrics[metric.Fingerprint] = metric
	}

	return metrics, scanner.Err()
}

func findSlowerQueries(file1, file2 string, threshold float64) ([]string, error) {
	metrics1, err := loadMetrics(file1)
	if err != nil {
		return nil, fmt.Errorf("error loading first file: %w", err)
	}

	metrics2, err := loadMetrics(file2)
	if err != nil {
		return nil, fmt.Errorf("error loading second file: %w", err)
	}

	var slowerQueries []string
	for fingerprint, metric1 := range metrics1 {
		metric2, exists := metrics2[fingerprint]
		if !exists {
			continue
		}

		// Calculate percentage increase
		increase := float64(metric2.Duration-metric1.Duration) / float64(metric1.Duration)
		if increase > threshold {
			slowerQueries = append(slowerQueries, fingerprint)
		}
	}

	return slowerQueries, nil
}

func findQueryByFingerprint(queryLogPath string, fingerprint string) (*ParsedQuery, error) {
	iterator, err := NewQueryLogIterator(queryLogPath)
	if err != nil {
		return nil, fmt.Errorf("error creating iterator: %w", err)
	}

	for iterator.Next() {
		query := iterator.At()
		if query.Fingerprint == fingerprint {
			return &query, nil
		}
	}

	if err := iterator.Err(); err != nil {
		return nil, fmt.Errorf("error during iteration: %w", err)
	}

	return nil, fmt.Errorf("query not found for fingerprint: %s", fingerprint)
}

func saveQueryToFile(query *ParsedQuery, outputDir string) error {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("error creating directory: %w", err)
	}

	// Create a file named after the fingerprint
	filename := filepath.Join(outputDir, query.Fingerprint+".json")
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	// Write the original unparsed JSON entry
	if err := json.NewEncoder(file).Encode(query.UnparsedEntry); err != nil {
		return fmt.Errorf("error writing query: %w", err)
	}

	return nil
}

func compareQueryPerformance(file1, file2, queryLogPath string) error {
	metrics1, err := loadMetrics(file1)
	if err != nil {
		return fmt.Errorf("error loading first file: %w", err)
	}

	metrics2, err := loadMetrics(file2)
	if err != nil {
		return fmt.Errorf("error loading second file: %w", err)
	}

	slowerQueries, err := findSlowerQueries(file1, file2, 0.1) // 10% threshold
	if err != nil {
		return fmt.Errorf("error finding slower queries: %w", err)
	}

	outputDir := filepath.Join(os.Getenv("HOME"), "proba/2025-03-postings-for-matchers-hackathon/selected-queries")

	fmt.Printf("Found %d queries that are more than 10%% slower\n", len(slowerQueries))
	for _, fingerprint := range slowerQueries {
		query, err := findQueryByFingerprint(queryLogPath, fingerprint)
		if err != nil {
			fmt.Printf("Error finding query for fingerprint %s: %v\n", fingerprint, err)
			continue
		}

		// Save query to JSON file
		if err := saveQueryToFile(query, outputDir); err != nil {
			fmt.Printf("Error saving query %s to file: %v\n", fingerprint, err)
			continue
		}

		metric1 := metrics1[fingerprint]
		metric2 := metrics2[fingerprint]
		increase := float64(metric2.Duration-metric1.Duration) / float64(metric1.Duration) * 100

		fmt.Printf("\nFingerprint: %s\n", fingerprint)
		fmt.Printf("Query: %s\n", query.Query)
		fmt.Printf("Time Range: %s -> %s\n", query.Start.Format(time.RFC3339), query.End.Format(time.RFC3339))
		fmt.Printf("File 1: %v (%d series)\n", metric1.Duration, metric1.Series)
		fmt.Printf("File 2: %v (%d series)\n", metric2.Duration, metric2.Series)
		fmt.Printf("Increase: %.1f%%\n", increase)
		fmt.Printf("Vector selectors:\n")
		for _, selector := range query.VectorSelectors {
			fmt.Printf("  %v\n", selector)
		}
		fmt.Printf("Saved to: %s\n", filepath.Join(outputDir, fingerprint+".json"))
		fmt.Println("---")
	}

	return nil
}
