package tsdb

import (
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

var _ BlockReader = &OOOHead{}

// OOOHead allows querying Head out of order samples via BlockReader
// interface implementation.
type OOOHead struct {
	head       *Head
	//TODO(ganesh) Maybe we want to keep mint and maxt because when you are querying you want to only query the timerange of the query
	// For compaction we can use min int64 and max int64.
}

// NewOOOHead returns a *OOOHead.
func NewOOOHead(head *Head) *OOOHead {
	return &OOOHead{
		head: head,
	}
}

func (oh *OOOHead) Index() (IndexReader, error) {
	return &oooHeadIndexReader{ head: oh.head }, nil
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
