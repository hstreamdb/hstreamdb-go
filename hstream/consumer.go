package hstream

import (
	"context"
	"io"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
)

type FetchResult interface {
	GetResult() ([]*hstreampb.ReceivedRecord, error)
	SetError(err error)
	Ack()
}

type rfcFetchRes struct {
	result     []*hstreampb.ReceivedRecord
	err        error
	ackChannel chan []*hstreampb.RecordId
}

func newRfcFetchRes(result []*hstreampb.ReceivedRecord, ackChannel chan []*hstreampb.RecordId) *rfcFetchRes {
	return &rfcFetchRes{
		result:     result,
		ackChannel: ackChannel,
	}
}

func (r *rfcFetchRes) SetError(err error) {
	r.err = err
}

func (r *rfcFetchRes) GetResult() ([]*hstreampb.ReceivedRecord, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.result, nil
}

func (r *rfcFetchRes) SetResult(res interface{}) {
	r.result = res.([]*hstreampb.ReceivedRecord)
}

func (r *rfcFetchRes) Ack() {
	ackIds := make([]*hstreampb.RecordId, len(r.result))
	for i, record := range r.result {
		ackIds[i] = record.GetRecordId()
	}
	r.ackChannel <- ackIds
}

type Consumer struct {
	client       *HStreamClient
	subId        string
	consumerName string
	dataChannel  chan FetchResult
	ackChannel   chan []*hstreampb.RecordId

	streamingCancel context.CancelFunc
}

func NewConsumer(client *HStreamClient, subId string, consumerName string) *Consumer {
	return &Consumer{
		client:       client,
		subId:        subId,
		consumerName: consumerName,
		ackChannel:   make(chan []*hstreampb.RecordId, 100),
	}
}

func (c *Consumer) Stop() {
	c.streamingCancel()
	close(c.ackChannel)
}

func (c *Consumer) StartFetch() chan FetchResult {
	address, err := c.client.lookUpSubscription(c.subId)
	c.dataChannel = make(chan FetchResult, 100)
	if err != nil {
		util.Logger().Error("failed to look up subscription", zap.String("Subscription", c.subId), zap.Error(err))
		return c.handleFetchError(err)
	}

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

	c.fetch(cancelCtx, stream)
	go func() {
		util.Logger().Debug("start ackChannel")
		for ids := range c.ackChannel {
			ackReq := &hstreampb.StreamingFetchRequest{
				SubscriptionId: c.subId,
				ConsumerName:   c.consumerName,
				AckIds:         ids,
			}
			if err = stream.Send(ackReq); err != nil {
				util.Logger().Error("streaming fetch client send error", zap.String("subId", c.subId), zap.Error(err))
				return
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
			recordSize := len(records.GetReceivedRecords())
			res := make([]*hstreampb.ReceivedRecord, recordSize)
			copy(res, records.GetReceivedRecords())
			util.Logger().Debug("receive records from stream", zap.String("subId", c.subId), zap.Int("count", len(records.GetReceivedRecords())))
			result := newRfcFetchRes(res, c.ackChannel)
			if err != nil && err == io.EOF {
				util.Logger().Info("streamingFetch serve stream EOF", zap.String("subId", c.subId))
				return
			} else if err != nil {
				result.SetError(err)
			} else {
				result.SetResult(records.GetReceivedRecords())
			}

			c.dataChannel <- result
		}
	}()
}

func (c *Consumer) handleFetchError(err error) chan FetchResult {
	errRes := &rfcFetchRes{}
	errRes.SetError(err)
	c.dataChannel <- errRes
	close(c.dataChannel)
	return c.dataChannel
}
