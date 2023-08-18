package hstream

import (
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
)

var (
	// EarliestOffset specifies that the data is read from the start of the shard/stream
	EarliestOffset = earliestOffset{}
	// LatestOffset specifies that the data is read from the current tail of the shard/stream
	LatestOffset = latestOffset{}
)

// ShardOffset is used to specify a specific offset for the shardReader.
type ShardOffset interface {
	toShardOffset() *hstreampb.ShardOffset
}

type StreamOffset interface {
	toStreamOffset() *hstreampb.StreamOffset
}

type earliestOffset struct{}

func (e earliestOffset) toShardOffset() *hstreampb.ShardOffset {
	offset := hstreampb.ShardOffset_SpecialOffset{
		SpecialOffset: hstreampb.SpecialOffset_EARLIEST,
	}
	return &hstreampb.ShardOffset{Offset: &offset}
}

func (e earliestOffset) toStreamOffset() *hstreampb.StreamOffset {
	offset := hstreampb.StreamOffset_SpecialOffset{
		SpecialOffset: hstreampb.SpecialOffset_EARLIEST,
	}
	return &hstreampb.StreamOffset{Offset: &offset}
}

type latestOffset struct{}

func (e latestOffset) toShardOffset() *hstreampb.ShardOffset {
	offset := hstreampb.ShardOffset_SpecialOffset{
		SpecialOffset: hstreampb.SpecialOffset_LATEST,
	}
	return &hstreampb.ShardOffset{Offset: &offset}
}

func (e latestOffset) toStreamOffset() *hstreampb.StreamOffset {
	offset := hstreampb.StreamOffset_SpecialOffset{
		SpecialOffset: hstreampb.SpecialOffset_LATEST,
	}
	return &hstreampb.StreamOffset{Offset: &offset}
}

type RecordOffset Record.RecordId

// NewRecordOffset create a RecordOffset of a shard
func NewRecordOffset(recordId Record.RecordId) RecordOffset {
	rid := RecordOffset(recordId)
	return rid
}

func (r RecordOffset) toShardOffset() *hstreampb.ShardOffset {
	offset := &hstreampb.ShardOffset_RecordOffset{
		RecordOffset: &hstreampb.RecordId{
			ShardId:    r.ShardId,
			BatchId:    r.BatchId,
			BatchIndex: r.BatchIndex,
		},
	}
	return &hstreampb.ShardOffset{Offset: offset}
}

type TimestampOffset int64

func NewTimestampOffset(timestamp int64) TimestampOffset {
	return TimestampOffset(timestamp)
}

func (t TimestampOffset) toStreamOffset() *hstreampb.StreamOffset {
	offset := &hstreampb.StreamOffset_TimestampOffset{
		TimestampOffset: &hstreampb.TimestampOffset{
			TimestampInMs:  int64(t),
			StrictAccuracy: true,
		},
	}
	return &hstreampb.StreamOffset{Offset: offset}
}

func (t TimestampOffset) toShardOffset() *hstreampb.ShardOffset {
	offset := &hstreampb.ShardOffset_TimestampOffset{
		TimestampOffset: &hstreampb.TimestampOffset{
			TimestampInMs:  int64(t),
			StrictAccuracy: true,
		},
	}
	return &hstreampb.ShardOffset{Offset: offset}
}
