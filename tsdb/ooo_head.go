package tsdb

import (
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

var _ BlockReader = &OOOHead{}

// OOOHead allows querying Head out of order samples via BlockReader
// interface implementation.
type OOOHead struct {
	head       *Head
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
		MinTime: oh.head.MinTime(),
		MaxTime: oh.head.MaxTime(),
		ULID:    id,
		Stats: BlockStats{
			NumSeries: oh.head.NumSeries(),
		},
	}
}

func (oh *OOOHead) Size() int64 {
	// TODO(jesus.vazquez) Find what's the appropriate value here
	return oh.head.Size()
}
