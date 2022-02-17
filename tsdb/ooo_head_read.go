package tsdb

import (
	"math"
	"sort"

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

	tmpChks := make([]chunks.Meta, 0, len(s.oooMmappedChunks))

	// We define these markers to track the last chunk reference while we
	// fill the chunk meta.
	// These markers are useful to give consistent responses to repeated queries
	// even if new chunks that might be overlapping or not are added afterwards.
	// Also, lastMinT and lastMaxT are initialized to the max int as a sentinel
	// value to know they are unset.
	var lastChunkRef chunks.ChunkRef
	lastMinT, lastMaxT := int64(math.MaxInt64), int64(math.MaxInt64)

	// We first collect the oooHeadChunk, if any. And if it exists we set value
	// for the markers defined above.
	if s.oooHeadChunk != nil && s.oooHeadChunk.OverlapsClosedInterval(oh.mint, oh.maxt) {
		lastMinT = s.oooHeadChunk.minTime
		lastMaxT = s.oooHeadChunk.maxTime
		lastChunkRef = chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.oooHeadChunkID(len(s.oooMmappedChunks))))

		tmpChks = append(tmpChks, chunks.Meta{
			MinTime:        s.oooHeadChunk.minTime,
			MaxTime:        math.MaxInt64, // Set the head chunks as open (being appended to).
			Ref:            lastChunkRef,
			OOOLastRef:     lastChunkRef,
			OOOLastMinTime: lastMinT,
			OOOLastMaxTime: lastMaxT,
		})

	}

	// Next collect the memory mapped chunks in reverse order.
	// In case there was no head chunk before, we want the last memory mapped
	// chunk to be our last reference for the markers and the chunk meta.
	for i := len(s.oooMmappedChunks); i >= 0; i-- {
		c := s.oooMmappedChunks[i]

		// Do not expose chunks that are outside of the specified range.
		if !c.OverlapsClosedInterval(oh.mint, oh.maxt) {
			continue
		}

		if lastMinT == int64(math.MaxInt64) {
			lastChunkRef = chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.oooHeadChunkID(i)))
			lastMinT = c.minTime
			lastMaxT = c.maxTime
		}

		tmpChks = append(tmpChks, chunks.Meta{
			MinTime:        c.minTime,
			MaxTime:        c.maxTime,
			Ref:            chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.oooHeadChunkID(i))),
			OOOLastRef:     lastChunkRef,
			OOOLastMinTime: lastMinT,
			OOOLastMaxTime: lastMaxT,
		})
	}

	// There is nothing to do if we did not collect any chunk
	if len(tmpChks) == 0 {
		return nil
	}

	// Next we want to sort all the collected chunks by min time so we can find
	// those that overlap.
	sort.Sort(byMinTime(tmpChks))

	// Next we want to iterate the sorted collected chunks and only return the
	// chunks Meta the first chunk that overlaps with others.
	// Example chunks of a series: 5:(100, 200) 6:(500, 600) 7:(150, 250) 8:(550, 650)
	// In the example 5 overlaps with 7 and 6 overlaps with 8 so we only want to
	// to return chunk Metas for chunk 5 and chunk 6
	*chks = append(*chks, tmpChks[0])
	maxTime := tmpChks[0].MaxTime
	for _, c := range tmpChks[1:] {
		if c.MinTime > maxTime {
			*chks = append(*chks, c)
			maxTime = c.MaxTime
		} else if c.MaxTime > maxTime {
			maxTime = c.MaxTime
		}
	}

	return nil
}

type byMinTime []chunks.Meta

func (b byMinTime) Len() int           { return len(b) }
func (b byMinTime) Less(i, j int) bool { return b[i].MinTime < b[j].MinTime }
func (b byMinTime) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

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
