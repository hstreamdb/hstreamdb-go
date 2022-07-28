package hstream

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"io"
	"sync"
	"time"

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
	waitAck         sync.WaitGroup
}

func NewConsumer(client *HStreamClient, subId string, consumerName string) *Consumer {
	return &Consumer{
		client:       client,
		subId:        subId,
		consumerName: consumerName,
		ackChannel:   make(chan Record.RecordId, 100),
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
//   1. collect MAX_BATCH_ACKIDS: return (ackIds, false)
//	 2. ACK_COLLECT_TIMEOUT trigger: return (ackIds, false)
//   3. consumer is stopped and ackChannel is closed: return (ackIds, true)
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
			util.Logger().Debug("receive records from stream",
				zap.String("subId", c.subId),
				zap.Int("count", len(records.GetReceivedRecords())))

			var result FetchRecords
			if err != nil && err == io.EOF {
				util.Logger().Info("streamingFetch receive EOF", zap.String("subId", c.subId))
				return
			} else if err != nil {
				util.Logger().Error("streamingFetch receive error", zap.String("subId", c.subId), zap.Error(err))
				result = FetchRecords{
					Err: err,
				}
			} else {
				recordSize := len(records.GetReceivedRecords())
				res := make([]*FetchResult, 0, recordSize)
				// FIXME: find a proper way to handle parse error
				var parseError error
				for _, record := range records.GetReceivedRecords() {
					receivedRecord, err := ReceivedRecordFromPb(record)
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
