package hstream

import (
	"context"
	"io"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
)

// FetchResult is a handler to process the results of streamingFetch.
type FetchResult interface {
	// GetResult will return when the fetch result is ready,
	// or an error if the fetch fails.
	GetResult() ([]ReceivedRecord, error)
	// Ack will send an acknowledgement to the server so that
	// server won't resend the acked record.
	Ack()
}

type rpcFetchRes struct {
	result     []ReceivedRecord
	Err        error
	ackChannel chan []RecordId
}

func newRpcFetchRes(result []ReceivedRecord, ackChannel chan []RecordId) *rpcFetchRes {
	return &rpcFetchRes{
		result:     result,
		ackChannel: ackChannel,
	}
}

func (r *rpcFetchRes) GetResult() ([]ReceivedRecord, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	return r.result, nil
}

func (r *rpcFetchRes) Ack() {
	ackIds := make([]RecordId, len(r.result))
	for i, record := range r.result {
		ackIds[i] = record.GetRecordId()
	}
	r.ackChannel <- ackIds
}

// Consumer will consume records from specific subscription.
type Consumer struct {
	client       *HStreamClient
	subId        string
	consumerName string
	dataChannel  chan FetchResult
	ackChannel   chan []RecordId

	streamingCancel context.CancelFunc
}

func NewConsumer(client *HStreamClient, subId string, consumerName string) *Consumer {
	return &Consumer{
		client:       client,
		subId:        subId,
		consumerName: consumerName,
		ackChannel:   make(chan []RecordId, 100),
	}
}

// Stop will stop the consumer.
func (c *Consumer) Stop() {
	c.streamingCancel()
	close(c.ackChannel)
}

// StartFetch consumes data from the specified subscription. This method is an asynchronous method
// that allows the user to retrieve the return value from the result channel when consumption is complete.
func (c *Consumer) StartFetch() chan FetchResult {
	address, err := c.client.lookUpSubscription(c.subId)
	if err != nil {
		return c.handleFetchError(err)
	}
	c.dataChannel = make(chan FetchResult, 100)

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
	go func() {
		util.Logger().Info("start ackChannel")
		for ids := range c.ackChannel {
			pbIds := make([]*hstreampb.RecordId, 0, len(ids))
			for _, id := range ids {
				pbIds = append(pbIds, RecordIdToPb(id))
			}
			ackReq := &hstreampb.StreamingFetchRequest{
				SubscriptionId: c.subId,
				ConsumerName:   c.consumerName,
				AckIds:         pbIds,
			}
			if err = stream.Send(ackReq); err != nil {
				util.Logger().Error("streaming fetch client send error", zap.String("subId", c.subId), zap.Error(err))
			}
		}
	}()
	return c.dataChannel
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

			var result *rpcFetchRes
			if err != nil && err == io.EOF {
				util.Logger().Info("streamingFetch receive EOF", zap.String("subId", c.subId))
				return
			} else if err != nil {
				util.Logger().Error("streamingFetch receive error", zap.String("subId", c.subId), zap.Error(err))
				result = newRpcFetchRes(nil, c.ackChannel)
				result.Err = err
			} else {
				recordSize := len(records.GetReceivedRecords())
				res := make([]ReceivedRecord, 0, recordSize)
				// FIXME: find a proper way to handle parse error
				var parseError error
				for _, record := range records.GetReceivedRecords() {
					receivedRecord, err := ReceivedRecordFromPb(record)
					if err != nil {
						parseError = err
						break
					}
					res = append(res, receivedRecord)
				}

				if parseError != nil {
					result = newRpcFetchRes(nil, c.ackChannel)
					result.Err = parseError
				} else {
					result = newRpcFetchRes(res, c.ackChannel)
				}
			}

			c.dataChannel <- result
		}
	}()
}

// handleFetchError is a helper function to set an error result.
func (c *Consumer) handleFetchError(err error) chan FetchResult {
	errRes := &rpcFetchRes{}
	errRes.Err = err
	c.dataChannel <- errRes
	close(c.dataChannel)
	return c.dataChannel
}
