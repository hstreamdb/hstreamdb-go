package hstream

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/hstreamrpc"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamDB/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
	"io"
)

type Consumer struct {
	client       *HStreamClient
	subId        string
	consumerName string
	dataChannel  chan client.FetchResult

	streamingCancel context.CancelFunc
}

func NewConsumer(client *HStreamClient, subId string, consumerName string) *Consumer {
	return &Consumer{
		client:       client,
		subId:        subId,
		consumerName: consumerName,
	}
}

func (c *Consumer) Stop() {
	c.streamingCancel()
}

func (c *Consumer) StartFetch() (chan client.FetchResult, chan []*hstreampb.RecordId) {
	address, err := c.client.lookUpSubscription(c.subId)
	c.dataChannel = make(chan client.FetchResult, 100)
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

	ackChannel := c.fetch(cancelCtx, stream)
	go func() {
		for ids := range ackChannel {
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
	return c.dataChannel, ackChannel
}

func (c *Consumer) handleFetchError(err error) (chan client.FetchResult, chan []*hstreampb.RecordId) {
	errRes := &hstreamrpc.RPCFetchRes{}
	errRes.SetError(err)
	c.dataChannel <- errRes
	close(c.dataChannel)
	return c.dataChannel, nil
}

func (c *Consumer) fetch(cancelCtx context.Context, stream hstreampb.HStreamApi_StreamingFetchClient) chan []*hstreampb.RecordId {
	ackChannel := make(chan []*hstreampb.RecordId, 100)
	go func() {
		defer close(ackChannel)
		defer close(c.dataChannel)
		for {
			select {
			case <-cancelCtx.Done():
				util.Logger().Info("cancel fetching from ctx", zap.String("subId", c.subId))
				return
			default:
			}

			records, err := stream.Recv()
			res := make([]*hstreampb.ReceivedRecord, len(records.GetReceivedRecords()))
			copy(res, records.GetReceivedRecords())
			util.Logger().Debug("receive records from stream", zap.String("subId", c.subId), zap.Int("count", len(records.GetReceivedRecords())))
			for _, record := range res {
				util.Logger().Debug("fetched records", zap.String("subId", c.subId), zap.String("recordId", record.GetRecordId().String()))
			}
			result := &hstreamrpc.RPCFetchRes{}
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
	return ackChannel
}
