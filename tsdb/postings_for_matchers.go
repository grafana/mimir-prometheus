package tsdb

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

// IndexForPostings is a subset of IndexReader methods, the minimum required to evaluate PostingsForMatchers
type IndexForPostings interface {
	// LabelValues returns possible label values which may not be sorted.
	LabelValues(name string, matchers ...*labels.Matcher) ([]string, error)

	// Postings returns the postings list iterator for the label pairs.
	// The Postings here contain the offsets to the series inside the index.
	// Found IDs are not strictly required to point to a valid Series, e.g.
	// during background garbage collections. Input values must be sorted.
	Postings(name string, values ...string) (index.Postings, error)
}

// indexReaderWithPostingsForMatchers adapts an index.Reader to be an IndexReader by implementing the PostingsForMatchers method
type indexReaderWithPostingsForMatchers struct {
	*index.Reader
}

var _ IndexReader = indexReaderWithPostingsForMatchers{}

func (idx indexReaderWithPostingsForMatchers) PostingsForMatchers(ms ...*labels.Matcher) (index.Postings, error) {
	return PostingsForMatchers(idx.Reader, ms...)
}
