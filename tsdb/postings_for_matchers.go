package tsdb

import (
	"strings"
	"sync"

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

// PostingsForMatchersProvider provides a PostingsForMatcher method implementations, some of them might just call
// PostingsForMatchers, others might deduplicate concurrent calls, or even cache the results.
type PostingsForMatchersProvider interface {
	// PostingsForMatchers assembles a single postings iterator based on the
	// given matchers. The resulting postings are not ordered by series.
	PostingsForMatchers(ms ...*labels.Matcher) (index.Postings, error)
}

func NewPromisePostingsForMatchersProvider(ifp IndexForPostings) *PromisePostingsForMatchersProvider {
	return &PromisePostingsForMatchersProvider{
		indexForPostings:    ifp,
		postingsForMatchers: PostingsForMatchers,
		calls:               &sync.Map{},
	}
}

type PromisePostingsForMatchersProvider struct {
	indexForPostings    IndexForPostings
	postingsForMatchers func(ix IndexForPostings, ms ...*labels.Matcher) (index.Postings, error)

	calls *sync.Map
}

func (p *PromisePostingsForMatchersProvider) PostingsForMatchers(ms ...*labels.Matcher) (index.Postings, error) {
	type call struct {
		sync.Once
		sync.WaitGroup

		postings *index.PostingsCloner
		err      error
	}

	promise := &call{}
	promise.Add(1)

	key := matchersKey(ms)
	wi, loaded := p.calls.LoadOrStore(key, promise)
	if loaded {
		promise = wi.(*call)
	} else {
		// TODO oleg: this is where we can put a timer to convert this into a cache
		defer p.calls.Delete(key)
	}

	promise.Do(func() {
		defer promise.Done()
		postings, err := p.postingsForMatchers(p.indexForPostings, ms...)
		if err != nil {
			promise.err = err
			return
		}
		promise.postings = index.NewPostingsCloner(postings)
	})
	promise.Wait()

	if promise.err != nil {
		return nil, promise.err
	}

	return promise.postings.Clone(), nil
}

// matchersKey provides a unique string key for the given matchers slice
// NOTE: different orders of matchers will produce different keys,
// but it's unlikely that we'll receive same matchers in different orders at the same time
func matchersKey(ms []*labels.Matcher) string {
	sb := strings.Builder{}
	for _, m := range ms {
		sb.WriteString(m.Name)
		sb.WriteString(m.Type.String())
		sb.WriteString(m.Value)
		sb.WriteByte(0)
	}
	return sb.String()
}

// indexReaderWithPostingsForMatchers adapts an index.Reader to be an IndexReader by implementing the PostingsForMatchers method
type indexReaderWithPostingsForMatchers struct {
	*index.Reader
	pfm func(ms ...*labels.Matcher) (index.Postings, error)
}

var _ IndexReader = indexReaderWithPostingsForMatchers{}

func (idx indexReaderWithPostingsForMatchers) PostingsForMatchers(ms ...*labels.Matcher) (index.Postings, error) {
	return idx.pfm(ms...)
}
