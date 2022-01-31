package tsdb

import (
	"math"
	"sort"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

var _ IndexReader = &oooHeadIndexReader{}

// oooHeadIndexReader implemented IndexReader so ooo samples in the head can be
// accessed.
type oooHeadIndexReader struct {
	head *Head
}

func (oh *oooHeadIndexReader) Symbols() index.StringIter {
	return oh.head.postings.Symbols()
}

func (oh *oooHeadIndexReader) SortedLabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	values, err := oh.LabelValues(name, matchers...)
	if err == nil {
		sort.Strings(values)
	}
	return values, err
}

func (oh *oooHeadIndexReader) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	// TODO(jesus.vazquez) Clarify if we need this
	// if oh.maxt < oh.head.MinTime() || oh.mint > oh.head.MaxTime() {
	// 	return []string{}, nil
	// }

	if len(matchers) == 0 {
		return oh.head.postings.LabelValues(name), nil
	}

	return labelValuesWithMatchers(oh, name, matchers...)
}

func (oh *oooHeadIndexReader) Postings(name string, values ...string) (index.Postings, error) {
	switch len(values) {
	case 0:
		return index.EmptyPostings(), nil
	case 1:
		return oh.head.postings.Get(name, values[0]), nil
	default:
		res := make([]index.Postings, 0, len(values))
		for _, value := range values {
			res = append(res, oh.head.postings.Get(name, value))
		}
		return index.Merge(res...), nil
	}
}

func (oh *oooHeadIndexReader) PostingsForMatchers(concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	return oh.head.pfmc.PostingsForMatchers(oh, concurrent, ms...)
}

func (oh *oooHeadIndexReader) SortedPostings(postings index.Postings) index.Postings {
	series := make([]*memSeries, 0, 128)

	// Fetch all the series only once.
	for postings.Next() {
		s := oh.head.series.getByID(chunks.HeadSeriesRef(postings.At()))
		if s == nil {
			level.Debug(oh.head.logger).Log("msg", "Looked up series not found")
		} else {
			series = append(series, s)
		}
	}
	if err := postings.Err(); err != nil {
		return index.ErrPostings(errors.Wrap(err, "expand postings"))
	}

	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].lset, series[j].lset) < 0
	})

	// Convert back to list.
	ep := make([]storage.SeriesRef, 0, len(series))
	for _, p := range series {
		ep = append(ep, storage.SeriesRef(p.ref))
	}
	return index.NewListPostings(ep)
}

func (oh *oooHeadIndexReader) ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings {
	out := make([]storage.SeriesRef, 0, 128)

	for p.Next() {
		s := oh.head.series.getByID(chunks.HeadSeriesRef(p.At()))
		if s == nil {
			level.Debug(oh.head.logger).Log("msg", "Looked up series not found")
			continue
		}

		// Check if the series belong to the shard.
		if s.hash%shardCount != shardIndex {
			continue
		}

		out = append(out, storage.SeriesRef(s.ref))
	}

	return index.NewListPostings(out)
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
		// TODO(jesus.vazquez) This comprobation is not needed, right?
		// // Do not expose chunks that are outside of the specified range.
		// if !c.OverlapsClosedInterval(h.mint, h.maxt) {
		// 	continue
		// }
		*chks = append(*chks, chunks.Meta{
			MinTime: c.minTime,
			MaxTime: c.maxTime,
			Ref:     chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.headChunkID(i))),
		})
	}
	// TODO(jesus.vazquez) Review this
	// if s.headChunk != nil && s.headChunk.OverlapsClosedInterval(h.mint, h.maxt) {
	if s.oooHeadChunk != nil {
		*chks = append(*chks, chunks.Meta{
			MinTime: s.oooHeadChunk.minTime,
			MaxTime: math.MaxInt64, // Set the head chunks as open (being appended to).
			Ref:     chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.headChunkID(len(s.oooMmappedChunks)))), // TODO(jesus.vazquez) Is headChunkID going to work?
		})
	}

	return nil
}

func (oh *oooHeadIndexReader) LabelNames(matchers ...*labels.Matcher) ([]string, error) {
	// TODO(jesus.vazquez) Is this needed?
	// if oh.maxt < oh.head.MinTime() || oh.mint > oh.head.MaxTime() {
	// 	return []string{}, nil
	// }

	if len(matchers) == 0 {
		labelNames := oh.head.postings.LabelNames()
		sort.Strings(labelNames)
		return labelNames, nil
	}

	return labelNamesWithMatchers(oh, matchers...)
}

func (oh *oooHeadIndexReader) LabelValueFor(id storage.SeriesRef, label string) (string, error) {
	memSeries := oh.head.series.getByID(chunks.HeadSeriesRef(id))
	if memSeries == nil {
		return "", storage.ErrNotFound
	}

	value := memSeries.lset.Get(label)
	if value == "" {
		return "", storage.ErrNotFound
	}

	return value, nil
}

func (oh *oooHeadIndexReader) LabelNamesFor(ids ...storage.SeriesRef) ([]string, error) {
	namesMap := make(map[string]struct{})
	for _, id := range ids {
		memSeries := oh.head.series.getByID(chunks.HeadSeriesRef(id))
		if memSeries == nil {
			return nil, storage.ErrNotFound
		}
		for _, lbl := range memSeries.lset {
			namesMap[lbl.Name] = struct{}{}
		}
	}
	names := make([]string, 0, len(namesMap))
	for name := range namesMap {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (oh *oooHeadIndexReader) Close() error {
	return nil
}
