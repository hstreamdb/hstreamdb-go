package hstream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
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
	toShardOffset() *hstreampb.ShardOffset
}

type earliestShardOffset struct{}

func (e earliestShardOffset) toShardOffset() *hstreampb.ShardOffset {
	offset := hstreampb.ShardOffset_SpecialOffset{
		SpecialOffset: hstreampb.SpecialOffset_EARLIEST,
	}
	return &hstreampb.ShardOffset{Offset: &offset}
}

type latestShardOffset struct{}

func (e latestShardOffset) toShardOffset() *hstreampb.ShardOffset {
	offset := hstreampb.ShardOffset_SpecialOffset{
		SpecialOffset: hstreampb.SpecialOffset_LATEST,
	}
	return &hstreampb.ShardOffset{Offset: &offset}
}

type recordOffset Record.RecordId

// NewRecordOffset create a RecordOffset of a shard
func NewRecordOffset(recordId Record.RecordId) ShardOffset {
	rid := recordOffset(recordId)
	return rid
}

func (r recordOffset) toShardOffset() *hstreampb.ShardOffset {
	offset := &hstreampb.ShardOffset_RecordOffset{
		RecordOffset: &hstreampb.RecordId{
			ShardId:    r.ShardId,
			BatchId:    r.BatchId,
			BatchIndex: r.BatchIndex,
		},
	}
	return &hstreampb.ShardOffset{Offset: offset}
}

type shardResult struct {
	records []Record.ReceivedRecord
	err     error
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
	maxRead        uint32
	dataChan       chan shardResult
	decompressors  sync.Map

	// closed > 0 means the reader is closed
	closed uint32
}

func defaultReader(client *HStreamClient, streamName string, readerId string, shardId uint64) *ShardReader {
	return &ShardReader{
		client:        client,
		streamName:    streamName,
		readerId:      readerId,
		shardId:       shardId,
		shardOffset:   EarliestShardOffset,
		timeout:       0,
		dataChan:      make(chan shardResult, 10),
		closed:        0,
		maxRead:       1,
		decompressors: sync.Map{},
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

// WithMaxRecords is used to specify the maximum number of records that
// can be read in a single read.
func WithMaxRecords(cnt uint32) ShardReaderOpts {
	return func(reader *ShardReader) {
		reader.maxRead = cnt
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
		Req: &hstreampb.CreateShardReaderRequest{
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

	go reader.readLoop()

	return reader, err
}

// DeleteShardReader delete specific shardReader
// FIXME: Uniform interface: the delete operation should return an error ?
func (c *HStreamClient) DeleteShardReader(shardId uint64, readerId string) {
	addr, err := c.LookupShard(shardId)
	if err != nil {
		util.Logger().Error("lookup shardReader err", zap.Uint64("shardId", shardId), zap.String("readerId", readerId), zap.String("error", err.Error()))
		return
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteShardReader,
		Req: &hstreampb.DeleteShardReaderRequest{
			ReaderId: readerId,
		},
	}
	if _, err = c.sendRequest(addr, req); err != nil {
		util.Logger().Error("delete shardReader err", zap.Uint64("shardId", shardId), zap.String("readerId", readerId), zap.String("error", err.Error()))
		return
	}
}

func (s *ShardReader) readLoop() {
	readReq := &hstreamrpc.Request{
		Type: hstreamrpc.ReadShard,
		Req: &hstreampb.ReadShardRequest{
			ReaderId:   s.readerId,
			MaxRecords: s.maxRead,
		},
	}

	for {
		if atomic.LoadUint32(&s.closed) > 0 {
			close(s.dataChan)
			return
		}

		records, err := s.read(readReq, false)
		result := shardResult{
			records: records,
			err:     err,
		}
		s.dataChan <- result
	}
}

func (s *ShardReader) read(req *hstreamrpc.Request, forceLookup bool) ([]Record.ReceivedRecord, error) {
	var addr string
	var err error
	if forceLookup || len(s.lastSendServer) == 0 {
		addr, err = s.client.lookUpShardReader(s.readerId)
		if err != nil {
			return nil, err
		}
		s.lastSendServer = addr
	}

	res, err := s.client.sendRequest(s.lastSendServer, req)
	if err != nil {
		if !forceLookup && status.Code(err) == codes.FailedPrecondition {
			return s.read(req, true)
		}
		return nil, err
	}

	records := res.Resp.(*hstreampb.ReadShardResponse).GetReceivedRecords()
	readRes := make([]Record.ReceivedRecord, 0, len(records))
	for _, record := range records {
		hstreamReocrds, err := decodeReceivedRecord(record, &s.decompressors)
		if err != nil {
			return nil, err
		}
		recordIds := record.GetRecordIds()
		for i := 0; i < len(recordIds); i++ {
			receivedRecord, err := ReceivedRecordFromPb(hstreamReocrds[i], recordIds[i])
			if err != nil {
				return nil, err
			}
			readRes = append(readRes, receivedRecord)
		}

	}
	return readRes, nil
}

// Read read records from shard
func (s *ShardReader) Read(ctx context.Context) ([]Record.ReceivedRecord, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-s.dataChan:
		return res.records, res.err
	}
}

func (s *ShardReader) Close() {
	atomic.StoreUint32(&s.closed, 1)
}

func (c *HStreamClient) lookUpShardReader(readerId string) (string, error) {
	address, err := c.randomServer()
	if err != nil {
		return "", err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.LookupShardReader,
		Req: &hstreampb.LookupShardReaderRequest{
			ReaderId: readerId,
		},
	}

	var resp *hstreamrpc.Response
	if resp, err = c.sendRequest(address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupShardReaderResponse).GetServerNode()
	util.Logger().Debug("LookupShardReaderResponse", zap.String("readerId", readerId), zap.String("node", node.String()))
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}
