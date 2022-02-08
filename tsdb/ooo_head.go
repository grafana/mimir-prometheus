package tsdb

import (
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

var _ BlockReader = &OOOHead{}

// OOOHead allows querying Head out of order samples via BlockReader
// interface implementation.
type OOOHead struct {
	head       *Head
	// mint and maxt are tracked because when a query is handled we only want
	// the timerange of the query and having preexisting pointers to the first
	// and last timestamp help with that. They are also useful to find the block
	// range when compacting the head.
	mint, maxt int64
}

func NewOOOHead(head *Head, mint, maxt int64) *OOOHead {
	return &OOOHead{
		head: head,
		mint: mint,
		maxt: maxt,
	}
}

func (oh *OOOHead) Index() (IndexReader, error) {
	return &oooHeadIndexReader{ head: oh.head , mint: oh.mint, maxt: oh.maxt}, nil
}

func (oh *OOOHead) Chunks() (ChunkReader, error) {
	// TODO(jesus.vazquez) Need to create a oooHeadChunkReader that implements
	// ChunkReader. Check the above Index() method as an inspiration.
	panic("implement me")
}

func (oh *OOOHead) Tombstones() (tombstones.Reader, error) {
	// TODO(jesus.vazquez) We still need to clarify what to do with tombstones
	// here and in the design doc.
	panic("implement me")
}

func (oh *OOOHead) Meta() BlockMeta {
	var id [16]byte
	copy(id[:], "____ooo_head____")
	return BlockMeta{
		MinTime: oh.head.MinTime(), // TODO(ganesh) We might want to track in the head whats the mint and maxt for out of order samples
		MaxTime: oh.head.MaxTime(), // TODO(ganesh) We might want to track in the head whats the mint and maxt for out of order samples
		ULID:    id,
		Stats: BlockStats{
			NumSeries: oh.head.NumSeries(),
		},
	}
}

// Size returns 0 because the space taken by the out of order samples is taken
// into account by the RangeHead size.
func (oh *OOOHead) Size() int64 {
	// TODO(jesus.vazquez) Find what's the appropriate value here
	return 0
}
