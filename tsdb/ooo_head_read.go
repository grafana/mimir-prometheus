package tsdb

import (
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

var _ IndexReader = &OOOHeadIndexReader{}

// OOOHeadIndexReader implements IndexReader so ooo samples in the head can be
// accessed.
// It also has a reference to headIndexReader so we can leverage on its
// IndexReader implementation for all the methods that remain the same. We
// decided to do this to avoid code duplication.
// The only methods that change are the ones about getting Series and Postings.
type OOOHeadIndexReader struct {
	*headIndexReader // A reference to the headIndexReader so we can reuse as many interface implementation as possible.
}

func NewOOOHeadIndexReader(head *Head, mint, maxt int64) *OOOHeadIndexReader {
	hr := &headIndexReader{
		head: head,
		mint: mint,
		maxt: maxt,
	}
	return &OOOHeadIndexReader{hr}
}

func (oh *OOOHeadIndexReader) Series(ref storage.SeriesRef, lbls *labels.Labels, chks *[]chunks.Meta) error {
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

	// TODO We need to gather all the chunks that overlap in mint and maxt and
	// combine overlapping chunks into a single chunk.
	// For example:
	// Given the following chunks 1:(100, 200) 2:(500, 600) 3:(150, 250) 4:(550, 650)
	// The result would be: [ (combines 1,3), (combines(2, 4) ] -> [ (100, 250), (500, 650) ]
	// The resulting combined chunks should be identified by an unique reference

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

func (oh *OOOHeadIndexReader) Postings(name string, values ...string) (index.Postings, error) {
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
