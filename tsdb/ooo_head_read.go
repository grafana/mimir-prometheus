package tsdb

import (
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

var _ IndexReader = &oooHeadIndexReader{}

// oooHeadIndexReader implements IndexReader so ooo samples in the head can be
// accessed.
type oooHeadIndexReader struct {
	head *Head
	headIndexReader *headIndexReader // A reference to the headIndexReader so we can reuse as many interface implementation as possible.

	// mint and maxt are tracked because when a query is handled we only want
	// the timerange of the query and having preexisting pointers to the first
	// and last timestamp help with that. They are also useful to find the block
	mint, maxt int64
}

func (oh *oooHeadIndexReader) Symbols() index.StringIter {
	return oh.headIndexReader.Symbols()
}

func (oh *oooHeadIndexReader) SortedLabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	return oh.headIndexReader.SortedLabelValues(name, matchers...)
}

func (oh *oooHeadIndexReader) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	return oh.headIndexReader.LabelValues(name, matchers...)
}

func (oh *oooHeadIndexReader) Series(ref storage.SeriesRef, lbls *labels.Labels, chks *[]chunks.Meta) error {
	s := oh.head.series.getByID(chunks.HeadSeriesRef(ref))

	if s == nil {
		oh.head.metrics.seriesNotFound.Inc()
		return storage.ErrNotFound
	}
	*lbls = append((*lbls)[:0], s.lset...)

	if chks == nil {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	*chks = (*chks)[:0]

	for i, c := range s.oooMmappedChunks {
		// Do not expose chunks that are outside of the specified range.
		if !c.OverlapsClosedInterval(oh.mint, oh.maxt) {
			continue
		}
		*chks = append(*chks, chunks.Meta{
			MinTime: c.minTime,
			MaxTime: c.maxTime,
			Ref:     chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.oooHeadChunkID(i))),
		})
	}

	if s.oooHeadChunk != nil && s.oooHeadChunk.OverlapsClosedInterval(oh.mint, oh.maxt) {
		*chks = append(*chks, chunks.Meta{
			MinTime: s.oooHeadChunk.minTime,
			MaxTime: math.MaxInt64, // Set the head chunks as open (being appended to).
			Ref:     chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.oooHeadChunkID(len(s.oooMmappedChunks)))),
		})
	}

	return nil
}

func (oh *oooHeadIndexReader) Postings(name string, values ...string) (index.Postings, error) {
	switch len(values) {
	case 0:
		return index.EmptyPostings(), nil
	case 1:
		return oh.head.postings.Get(name, values[0]), nil // TODO(ganesh) Also call GetOOOPostings
	default:
		// TODO(ganesh) We want to only return postings for out of order series.
		res := make([]index.Postings, 0, len(values))
		for _, value := range values {
			res = append(res, oh.head.postings.Get(name, value)) // TODO(ganesh) Also call GetOOOPostings
		}
		return index.Merge(res...), nil
	}
}

func (oh *oooHeadIndexReader) PostingsForMatchers(concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	return oh.headIndexReader.PostingsForMatchers(concurrent, ms...)
}

func (oh *oooHeadIndexReader) SortedPostings(postings index.Postings) index.Postings {
	return oh.headIndexReader.SortedPostings(postings)
}

func (oh *oooHeadIndexReader) ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings {
	return oh.headIndexReader.ShardedPostings(p, shardIndex, shardCount)
}


func (oh *oooHeadIndexReader) LabelNames(matchers ...*labels.Matcher) ([]string, error) {
	return oh.headIndexReader.LabelNames(matchers...)
}

func (oh *oooHeadIndexReader) LabelValueFor(id storage.SeriesRef, label string) (string, error) {
	return oh.headIndexReader.LabelValueFor(id, label)
}

func (oh *oooHeadIndexReader) LabelNamesFor(ids ...storage.SeriesRef) ([]string, error) {
	return oh.headIndexReader.LabelNamesFor(ids...)
}

func (oh *oooHeadIndexReader) Close() error {
	return nil
}
