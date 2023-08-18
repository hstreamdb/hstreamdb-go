package hstream

import (
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
)

var (
	// EarliestShardOffset specifies that the data is read from the start of the shard
	EarliestShardOffset ShardOffset = earliestShardOffset{}
	// LatestShardOffset specifies that the data is read from the current tail of the shard
	LatestShardOffset ShardOffset = latestShardOffset{}
)

// ShardOffset is used to specify a specific offset for the shardReader.
type ShardOffset interface {
	toShardOffset() *server.ShardOffset
}

type earliestShardOffset struct{}

func (e earliestShardOffset) toShardOffset() *server.ShardOffset {
	offset := server.ShardOffset_SpecialOffset{
		SpecialOffset: server.SpecialOffset_EARLIEST,
	}
	return &server.ShardOffset{Offset: &offset}
}

type latestShardOffset struct{}

func (e latestShardOffset) toShardOffset() *server.ShardOffset {
	offset := server.ShardOffset_SpecialOffset{
		SpecialOffset: server.SpecialOffset_LATEST,
	}
	return &server.ShardOffset{Offset: &offset}
}

type recordOffset Record.RecordId

// NewRecordOffset create a RecordOffset of a shard
func NewRecordOffset(recordId Record.RecordId) ShardOffset {
	rid := recordOffset(recordId)
	return rid
}

func (r recordOffset) toShardOffset() *server.ShardOffset {
	offset := &server.ShardOffset_RecordOffset{
		RecordOffset: &server.RecordId{
			ShardId:    r.ShardId,
			BatchId:    r.BatchId,
			BatchIndex: r.BatchIndex,
		},
	}
	return &server.ShardOffset{Offset: offset}
}
