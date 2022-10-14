package hstream

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/hstream/compression"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"io"
	"sync"
	"time"

	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
)

const (
	MAX_BATCH_ACKIDS    = 100
	ACK_COLLECT_TIMEOUT = time.Second * 5
)

// FetchRecords encapsulates the results of StreamingFetch. StreamingFetch may consume one
// or more pieces of data at a time.
type FetchRecords struct {
	Result []*FetchResult
	Err    error
}

type FetchResult struct {
	Record.ReceivedRecord
	ackCh chan Record.RecordId
}

func newFetchRes(record Record.ReceivedRecord, ackCh chan Record.RecordId) *FetchResult {
	res := &FetchResult{
		ackCh: ackCh,
	}
	res.ReceivedRecord = record
	return res
}

func (f *FetchResult) Ack() {
	f.ackCh <- f.GetRecordId()
}

// Consumer will consume records from specific subscription.
type Consumer struct {
	client       *HStreamClient
	subId        string
	consumerName string
	dataChannel  chan FetchRecords
	ackChannel   chan Record.RecordId

	streamingCancel context.CancelFunc
	decompressors   sync.Map
	waitAck         sync.WaitGroup
}

func NewConsumer(client *HStreamClient, subId string, consumerName string) *Consumer {
	return &Consumer{
		client:        client,
		subId:         subId,
		consumerName:  consumerName,
		ackChannel:    make(chan Record.RecordId, 100),
		decompressors: sync.Map{},
	}
}

// Stop will stop the consumer.
func (c *Consumer) Stop() {
	close(c.ackChannel)
	c.waitAck.Wait()
	c.streamingCancel()
}

// StartFetch consumes data from the specified subscription. This method is an asynchronous method
// that allows the user to retrieve the return value from the result channel when consumption is complete.
func (c *Consumer) StartFetch() chan FetchRecords {
	address, err := c.client.lookUpSubscription(c.subId)
	if err != nil {
		return c.handleFetchError(err)
	}
	c.dataChannel = make(chan FetchRecords, 100)

	fetchReq := &hstreampb.StreamingFetchRequest{
		SubscriptionId: c.subId,
		ConsumerName:   c.consumerName,
		AckIds:         []*hstreampb.RecordId{},
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.StreamingFetch,
		Req:  fetchReq,
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	c.streamingCancel = cancel
	res, err := c.client.SendRequest(cancelCtx, address, req)
	if err != nil {
		util.Logger().Error("send fetch request error", zap.Error(err))
		return c.handleFetchError(err)
	}

	stream := res.Resp.(hstreampb.HStreamApi_StreamingFetchClient)
	// send an empty ack to trigger streaming fetch
	if err = stream.Send(fetchReq); err != nil {
		util.Logger().Error("send fetch request error", zap.Error(err))
		return c.handleFetchError(err)
	}

	// spawn a background goroutine to fetch data
	c.fetch(cancelCtx, stream)

	// spawn a background goroutine to handle ack
	c.waitAck.Add(1)
	go func() {
		util.Logger().Info("start ackChannel")
		defer c.waitAck.Done()
		for {
			ackIds, terminate := c.fetchPendingAcks()
			if len(ackIds) != 0 {
				c.sendAckReq(stream, ackIds)
				for i := 0; i < len(ackIds); i++ {
					ackIds[i] = nil
				}
				ackIds = ackIds[:0]
			}
			if terminate {
				break
			}
		}
	}()
	return c.dataChannel
}

// fetchPendingAcks will collect pending acks from the ack channel. Function will return when:
//  1. collect MAX_BATCH_ACKIDS: return (ackIds, false)
//  2. ACK_COLLECT_TIMEOUT trigger: return (ackIds, false)
//  3. consumer is stopped and ackChannel is closed: return (ackIds, true)
func (c *Consumer) fetchPendingAcks() ([]*hstreampb.RecordId, bool) {
	timer := time.NewTimer(ACK_COLLECT_TIMEOUT)
	ackIds := make([]*hstreampb.RecordId, 0, MAX_BATCH_ACKIDS)

	for {
		select {
		case id, ok := <-c.ackChannel:
			if !ok {
				util.Logger().Debug("ack channel closed, stop fetch pending acks.")
				return ackIds, true
			}
			ackIds = append(ackIds, RecordIdToPb(id))
			if len(ackIds) >= MAX_BATCH_ACKIDS {
				return ackIds, false
			}
		case <-timer.C:
			// when timeout, do an additional non-block try to collect as more AckIDs as possible
			for len(ackIds) < MAX_BATCH_ACKIDS {
				select {
				case id, ok := <-c.ackChannel:
					if !ok {
						util.Logger().Debug("ack channel closed, stop fetch pending acks.")
						return ackIds, true
					}
					ackIds = append(ackIds, RecordIdToPb(id))
				default:
				}
			}
			return ackIds, false
		}
	}
}

func (c *Consumer) sendAckReq(cli hstreampb.HStreamApi_StreamingFetchClient, ackIds []*hstreampb.RecordId) {
	ackReq := &hstreampb.StreamingFetchRequest{
		SubscriptionId: c.subId,
		ConsumerName:   c.consumerName,
		AckIds:         ackIds,
	}
	if err := cli.Send(ackReq); err != nil {
		util.Logger().Error("streaming fetch client send error", zap.String("subId", c.subId), zap.Error(err))
	}
}

func (c *Consumer) fetch(cancelCtx context.Context, stream hstreampb.HStreamApi_StreamingFetchClient) {
	go func() {
		defer close(c.dataChannel)
		for {
			select {
			case <-cancelCtx.Done():
				util.Logger().Info("cancel fetching from ctx", zap.String("subId", c.subId))
				return
			default:
			}

			records, err := stream.Recv()
			recordIds := records.GetReceivedRecords().GetRecordIds()
			recordSize := len(recordIds)
			util.Logger().Debug("receive records from stream",
				zap.String("subId", c.subId),
				zap.Int("count", recordSize))

			var result FetchRecords
			if err != nil && (err == io.EOF) {
				if err == io.EOF {
					util.Logger().Info("streamingFetch receive EOF", zap.String("subId", c.subId))
					return
				} else if status.Code(err) == codes.Canceled {
					util.Logger().Info("streamingFetch canceled", zap.String("subId", c.subId))
					return
				} else {
					util.Logger().Error("streamingFetch receive error", zap.String("subId", c.subId), zap.Error(err))
					result = FetchRecords{
						Err: err,
					}
					c.dataChannel <- result
					return
				}
			}

			hstreamRecords, err := decodeReceivedRecord(records.GetReceivedRecords(), &c.decompressors)
			if err != nil {
				util.Logger().Error("streamingFetch decode error", zap.String("subId", c.subId), zap.Error(err))
				result = FetchRecords{
					Err: err,
				}
				c.dataChannel <- result
				return
			}

			res := make([]*FetchResult, 0, recordSize)
			// FIXME: find a proper way to handle parse error
			var parseError error
			for i := 0; i < recordSize; i++ {
				receivedRecord, err := ReceivedRecordFromPb(hstreamRecords[i], recordIds[i])
				if err != nil {
					parseError = err
					break
				}
				fetchResult := newFetchRes(receivedRecord, c.ackChannel)
				res = append(res, fetchResult)
			}

			if parseError != nil {
				result = FetchRecords{
					Err: parseError,
				}
			} else {
				result = FetchRecords{
					Result: res,
				}
			}

			c.dataChannel <- result
		}
	}()
}

// handleFetchError is a helper function to set an error result.
func (c *Consumer) handleFetchError(err error) chan FetchRecords {
	errRes := FetchRecords{
		Err: err,
	}
	c.dataChannel <- errRes
	close(c.dataChannel)
	return c.dataChannel
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
	data := make([]byte, len(batchedRecord.Payload))
	payloads, err := decoder.Decompress(data, batchedRecord.Payload)
	if err != nil {
		return nil, errors.WithMessage(err, "decompress receivedRecord error")
	}

	var batchHStreamRecords hstreampb.BatchHStreamRecords
	if err := proto.Unmarshal(payloads, &batchHStreamRecords); err != nil {
		return nil, errors.WithMessage(err, "decode batchHStreamRecords error")
	}

	//res := make([]Record.ReceivedRecord, 0, batchedRecord.BatchSize)
	//for i := uint32(0); i < batchedRecord.BatchSize; i++ {
	//	hstreamRecord := batchHStreamRecords.Records[i]
	//	var rcv Record.ReceivedRecord
	//	switch hstreamRecord.GetHeader().GetFlag() {
	//	case hstreampb.HStreamRecordHeader_RAW:
	//		rcv, err = FromPbRawRecord(recordIds[i], hstreamRecord)
	//	case hstreampb.HStreamRecordHeader_JSON:
	//		rcv, err = FromPbHRecord(recordIds[i], hstreamRecord)
	//	default:
	//		return nil, errors.Errorf("unknown record type: %s", hstreamRecord.GetHeader().GetFlag())
	//	}
	//
	//	if err != nil {
	//		return nil, errors.WithMessage(err, fmt.Sprintf("convert hstreamRecord %+v to receivedRecord err", hstreamRecord))
	//	}
	//	res = append(res, rcv)
	//}
	return batchHStreamRecords.GetRecords(), nil
}
