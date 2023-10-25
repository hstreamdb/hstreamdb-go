package hstream

import (
	"fmt"
	"sync"

	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/hstreamdb/hstreamdb-go/hstream/compression"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

var (
	// EarliestOffset specifies that the data is read from the start of the shard/stream
	EarliestOffset = earliestOffset{}
	// LatestOffset specifies that the data is read from the current tail of the shard/stream
	LatestOffset = latestOffset{}
	// EmptyOffset will use the proto generated default value
	EmptyOffset = emptyOffset{}
)

// ShardOffset is used to specify a specific offset for the shardReader.
type ShardOffset interface {
	// toShardOffset converts offset to proto.ShardOffset
	toShardOffset() *hstreampb.ShardOffset
	ToString() string
}

type StreamOffset interface {
	// toStreamOffset converts offset to proto.StreamOffset
	toStreamOffset() *hstreampb.StreamOffset
	ToString() string
}

type emptyOffset struct{}

func (e emptyOffset) toShardOffset() *hstreampb.ShardOffset {
	return nil
}

func (e emptyOffset) toStreamOffset() *hstreampb.StreamOffset {
	return nil
}

func (e emptyOffset) ToString() string {
	return "empty"
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

func (e earliestOffset) ToString() string {
	return "earliest"
}

type latestOffset struct{}

func (l latestOffset) toShardOffset() *hstreampb.ShardOffset {
	offset := hstreampb.ShardOffset_SpecialOffset{
		SpecialOffset: hstreampb.SpecialOffset_LATEST,
	}
	return &hstreampb.ShardOffset{Offset: &offset}
}

func (l latestOffset) toStreamOffset() *hstreampb.StreamOffset {
	offset := hstreampb.StreamOffset_SpecialOffset{
		SpecialOffset: hstreampb.SpecialOffset_LATEST,
	}
	return &hstreampb.StreamOffset{Offset: &offset}
}

func (l latestOffset) ToString() string {
	return "latest"
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

func (r RecordOffset) ToString() string {
	return fmt.Sprintf("RecordId: %d-%d-%d", r.ShardId, r.BatchId, r.BatchIndex)
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

func (t TimestampOffset) ToString() string {
	return fmt.Sprintf("Timestamp: %d", t)
}

func decodeReceivedRecord(record *hstreampb.ReceivedRecord, decompressors *sync.Map) ([]*hstreampb.HStreamRecord, error) {
	batchedRecord := record.GetRecord()
	if batchedRecord == nil {
		return nil, nil
	}
	if batchedRecord.BatchSize != uint32(len(record.RecordIds)) {
		return nil, errors.New("BatchedRecord.BatchSize != len(RecordIds), data contaminated")
	}

	compressionTp := CompressionTypeFromPb(batchedRecord.CompressionType)
	decompressor, ok := decompressors.Load(compressionTp)
	if !ok {
		var newDecoder compression.Decompressor
		switch compressionTp {
		case compression.None:
			newDecoder = compression.NewNoneDeCompressor()
		case compression.Gzip:
			newDecoder = compression.NewGzipDeCompressor()
		case compression.Zstd:
			newDecoder = compression.NewZstdDeCompressor()
		}

		var loaded bool
		if decompressor, loaded = decompressors.LoadOrStore(compressionTp, newDecoder); loaded {
			// another thread already loaded this provider, so close the one we just initialized
			newDecoder.Close()
		}
	}
	decoder := decompressor.(compression.Decompressor)
	data := make([]byte, 0, len(batchedRecord.Payload))
	payloads, err := decoder.Decompress(data, batchedRecord.Payload)
	if err != nil {
		return nil, errors.WithMessage(err, "decompress receivedRecord error")
	}

	var batchHStreamRecords hstreampb.BatchHStreamRecords
	if err := proto.Unmarshal(payloads, &batchHStreamRecords); err != nil {
		return nil, errors.WithMessage(err, "decode batchHStreamRecords error")
	}

	return batchHStreamRecords.GetRecords(), nil
}
