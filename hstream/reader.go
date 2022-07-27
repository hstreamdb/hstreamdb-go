package hstream

import (
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	"github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type recordOffset RecordId

// NewRecordOffset create a RecordOffset of a shard
func NewRecordOffset(recordId RecordId) ShardOffset {
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

// ShardReader is used to read data from the specified shard
type ShardReader struct {
	client         *HStreamClient
	streamName     string
	readerId       string
	shardId        uint64
	shardOffset    ShardOffset
	timeout        uint32
	lastSendServer string
}

func defaultReader(client *HStreamClient, streamName string, readerId string, shardId uint64) *ShardReader {
	return &ShardReader{
		client:      client,
		streamName:  streamName,
		readerId:    readerId,
		shardId:     shardId,
		shardOffset: EarliestShardOffset,
		timeout:     0,
	}
}

// ShardReaderOpts is the option for the ShardReader.
type ShardReaderOpts func(reader *ShardReader)

// WithShardOffset is used to specify the read offset of the reader.
func WithShardOffset(offset ShardOffset) ShardReaderOpts {
	return func(reader *ShardReader) {
		reader.shardOffset = offset
	}
}

// WithReaderTimeout is used to specify read timeout.
func WithReaderTimeout(timeout uint32) ShardReaderOpts {
	return func(reader *ShardReader) {
		reader.timeout = timeout
	}
}

// NewShardReader create a shardReader to read data from specific shard
func (c *HStreamClient) NewShardReader(streamName string, readerId string, shardId uint64, opts ...ShardReaderOpts) (*ShardReader, error) {
	reader := defaultReader(c, streamName, readerId, shardId)
	for _, opt := range opts {
		opt(reader)
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateShardReader,
		Req: &server.CreateShardReaderRequest{
			StreamName:  streamName,
			ShardId:     shardId,
			ShardOffset: reader.shardOffset.toShardOffset(),
			ReaderId:    readerId,
			Timeout:     reader.timeout,
		},
	}
	address, err := c.randomServer()
	if err != nil {
		return nil, err
	}

	_, err = c.sendRequest(address, req)

	return reader, err
}

// Read read up to maxRecords from a shard
func (s *ShardReader) Read(maxRecords uint32) ([]ReceivedRecord, error) {
	return s.read(maxRecords, false)
}

func (s *ShardReader) read(maxRecords uint32, forceLookup bool) ([]ReceivedRecord, error) {
	var addr string
	var err error
	if forceLookup || len(s.lastSendServer) == 0 {
		addr, err = s.client.LookupShard(s.shardId)
		if err != nil {
			return nil, err
		}
		s.lastSendServer = addr
	}

	readReq := &hstreamrpc.Request{
		Type: hstreamrpc.ReadShard,
		Req: &server.ReadShardRequest{
			ReaderId:   s.readerId,
			MaxRecords: maxRecords,
		},
	}

	res, err := s.client.sendRequest(s.lastSendServer, readReq)
	if err != nil {
		if !forceLookup && status.Code(err) == codes.FailedPrecondition {
			return s.read(maxRecords, true)
		}
		return nil, err
	}

	records := res.Resp.(*server.ReadShardResponse).GetReceivedRecords()
	readRes := make([]ReceivedRecord, 0, len(records))
	for _, record := range records {
		receivedRecord, err := ReceivedRecordFromPb(record)
		if err != nil {
			return nil, err
		}
		readRes = append(readRes, receivedRecord)
	}
	return readRes, nil
}

// DeleteShardReader delete specific shardReader
func (s *ShardReader) DeleteShardReader() error {
	addr, err := s.client.LookupShard(s.shardId)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteShardReader,
		Req: &server.DeleteShardReaderRequest{
			ReaderId: s.readerId,
		},
	}
	if _, err = s.client.sendRequest(addr, req); err != nil {
		return err
	}
	return nil
}
