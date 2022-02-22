package hstream

import (
	"client/client"
	hstreampb "client/gen-proto/hstream/server"
	"client/hstreamrpc"
	"client/util"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"sync"
)

type Subscription struct {
	client   client.Client
	consumer map[string]*StreamConsumer
	sync.RWMutex
}

func NewSubscription(client client.Client) *Subscription {
	return &Subscription{
		client:   client,
		consumer: make(map[string]*StreamConsumer),
	}
}

func (s *Subscription) Create(ctx context.Context, subId string, streamName string, ackTimeout int32) error {
	sub := &hstreampb.Subscription{
		SubscriptionId:    subId,
		StreamName:        streamName,
		AckTimeoutSeconds: ackTimeout,
	}
	address, err := util.RandomServer(s.client)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateSubscription,
		Req:  sub,
	}

	if _, err = s.client.SendRequest(ctx, address, req); err != nil {
		return err
	}
	return nil
}

func (s *Subscription) Delete(ctx context.Context, subId string) error {
	address, err := util.RandomServer(s.client)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteSubscription,
		Req: &hstreampb.DeleteSubscriptionRequest{
			SubscriptionId: subId,
		},
	}

	if _, err = s.client.SendRequest(ctx, address, req); err != nil {
		return err
	}
	return nil
}

func (s *Subscription) List(ctx context.Context) (*client.SubIter, error) {
	address, err := util.RandomServer(s.client)
	if err != nil {
		return nil, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.ListSubscriptions,
		Req:  &hstreampb.ListSubscriptionsRequest{},
	}

	var resp *hstreamrpc.Response
	if resp, err = s.client.SendRequest(ctx, address, req); err != nil {
		return nil, err
	}
	subs := resp.Resp.(*hstreampb.ListSubscriptionsResponse).GetSubscription()
	return client.NewSubIter(subs), nil
}

func (s *Subscription) CheckExist(ctx context.Context, subId string) (bool, error) {
	address, err := util.RandomServer(s.client)
	if err != nil {
		return false, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CheckSubscriptionExist,
		Req: &hstreampb.CheckSubscriptionExistRequest{
			SubscriptionId: subId,
		},
	}

	var resp *hstreamrpc.Response
	if resp, err = s.client.SendRequest(ctx, address, req); err != nil {
		return false, err
	}
	isExist := resp.Resp.(*hstreampb.CheckSubscriptionExistResponse).Exists
	return isExist, nil
}

func (s *Subscription) lookUp(ctx context.Context, subId string) (string, error) {
	return lookup(ctx, s.client, subId, "")
}

func (s *Subscription) lookUpWithKey(ctx context.Context, subId string, key string) (string, error) {
	return lookup(ctx, s.client, subId, key)
}

type StreamConsumer struct {
	client       client.Client
	subId        string
	consumerName string

	watchChannel chan watchResult
	stop         map[string]context.CancelFunc

	watchCancel context.CancelFunc
}

func (s *Subscription) MakeConsumer(subId string, consumerName string) (client.StreamConsumer, error) {
	s.RLock()
	if _, ok := s.consumer[subId]; ok {
		s.RUnlock()
		return nil, fmt.Errorf("consumer already exists")
	}
	s.RUnlock()

	consumer := &StreamConsumer{
		client:       s.client,
		subId:        subId,
		consumerName: consumerName,
		watchChannel: make(chan watchResult),
		stop:         make(map[string]context.CancelFunc),
	}

	s.Lock()
	if _, ok := s.consumer[subId]; !ok {
		ctx, cancel := context.WithCancel(context.Background())
		if err := consumer.watch(ctx, subId); err != nil {
			util.Logger().Error("failed to watch subscription", zap.Error(err))
			cancel()
			return nil, err
		}
		consumer.watchCancel = cancel
		s.consumer[subId] = consumer
	}
	s.Unlock()
	util.Logger().Info("new consumer", zap.String("subId", subId), zap.String("consumerName", consumerName))
	return consumer, nil
}

func (c *StreamConsumer) Stop() {
	c.watchCancel()
}

func (c *StreamConsumer) Fetch(ctx context.Context, handler client.FetchResHandler) {
	go func() {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer func() {
			cancel()
			util.Logger().Info("consumer stopped", zap.String("subId", c.subId), zap.String("consumerName", c.consumerName))
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-c.watchChannel:
				if !ok {
					util.Logger().Info("stop fetch because watcher stopped", zap.String("subId", c.subId), zap.String("consumerName", c.consumerName))
					return
				}
				if result.err != nil {
					util.Logger().Error("watch error, stop Fetch", zap.String("subId", c.subId), zap.Error(result.err))
					res := hstreamrpc.RPCFetchRes{}
					res.SetError(result.err)
					handler.HandleRes(&res)
					return
				}
				switch result.value.GetChange().(type) {
				case *hstreampb.WatchSubscriptionResponse_ChangeAdd:
					addKey := result.value.GetChangeAdd().GetOrderingKey()
					util.Logger().Info("Receive add key", zap.String("subId", c.subId), zap.String("key", addKey))
					if _, ok := c.stop[addKey]; !ok {
						cancelFetchCtx, fetchCancel := context.WithCancel(cancelCtx)
						c.fetch(cancelFetchCtx, addKey, handler)
						c.stop[addKey] = fetchCancel
					}
				case *hstreampb.WatchSubscriptionResponse_ChangeRemove:
					removeKey := result.value.GetChangeRemove().GetOrderingKey()
					util.Logger().Info("Receive delete key", zap.String("subId", c.subId), zap.String("key", removeKey))
					if fetchCancel, ok := c.stop[removeKey]; ok {
						fetchCancel()
						delete(c.stop, removeKey)
					} else {
						util.Logger().Error("Delete key not found", zap.String("subId", c.subId), zap.String("key", removeKey))
					}
				}
			}
		}
	}()
}

func (c *StreamConsumer) fetch(ctx context.Context, key string, handler client.FetchResHandler) {
	address, err := lookup(ctx, c.client, c.subId, key)
	if err != nil {
		util.Logger().Error("lookup error in fetch", zap.Error(err))
		return
	}

	fetchReq := &hstreampb.StreamingFetchRequest{
		SubscriptionId: c.subId,
		ConsumerName:   c.consumerName,
		OrderingKey:    key,
		AckIds:         []*hstreampb.RecordId{},
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.StreamingFetch,
		Req:  fetchReq,
	}

	res, err := c.client.SendRequest(ctx, address, req)
	if err != nil {
		util.Logger().Error("send fetch request error", zap.Error(err))
		return
	}

	stream := res.Resp.(hstreampb.HStreamApi_StreamingFetchClient)
	// send an empty ack to trigger streaming fetch
	if err = stream.Send(fetchReq); err != nil {
		util.Logger().Error("send fetch request error", zap.Error(err))
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				util.Logger().Info("cancel fetching from ctx", zap.String("subId", c.subId), zap.String("key", key))
				return
			default:
				records, err := stream.Recv()
				util.Logger().Debug("receive records from stream", zap.String("subId", c.subId), zap.String("key", key), zap.Int("count", len(records.GetReceivedRecords())))
				result := &hstreamrpc.RPCFetchRes{}
				if err != nil && err == io.EOF {
					util.Logger().Info("streamingFetch serve stream EOF", zap.String("subId", c.subId), zap.String("key", key))
					return
				} else if err != nil {
					result.SetError(err)
				} else {
					result.SetResult(records.GetReceivedRecords())
				}

				handler.HandleRes(result)
				rids := make([]*hstreampb.RecordId, 0, len(records.GetReceivedRecords()))
				for _, record := range records.GetReceivedRecords() {
					rids = append(rids, record.GetRecordId())
				}

				ackReq := &hstreampb.StreamingFetchRequest{
					SubscriptionId: c.subId,
					OrderingKey:    key,
					ConsumerName:   c.consumerName,
					AckIds:         rids,
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
				// TODO: spawn a goroutine to send ack request ???
				if err := stream.Send(ackReq); err != nil {
					util.Logger().Error("streaming fetch client send error", zap.String("subId", c.subId), zap.String("key", key), zap.Error(err))
					return
				}
			}
		}
	}()
}

type watchResult struct {
	value *hstreampb.WatchSubscriptionResponse
	err   error
}

func (c *StreamConsumer) watch(ctx context.Context, subId string) error {
	util.Logger().Info("Send watch request", zap.String("subId", subId))
	timeoutCtx, cancel := context.WithTimeout(ctx, client.DIALTIMEOUT)
	server, err := lookup(timeoutCtx, c.client, subId, "")
	cancel()
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.WatchSubscription,
		Req: &hstreampb.WatchSubscriptionRequest{
			SubscriptionId: subId,
			ConsumerName:   c.consumerName,
		},
	}

	res, err := c.client.SendRequest(ctx, server, req)
	if err != nil {
		return err
	}

	watchStream := res.Resp.(hstreampb.HStreamApi_WatchSubscriptionClient)
	go func() {
		defer close(c.watchChannel)
		for {
			select {
			case <-ctx.Done():
				util.Logger().Info("cancel watch", zap.String("subId", subId), zap.Error(ctx.Err()))
				return
			default:
			}

			// FIXME: need to fork another goroutine and use channel to receive watch result?
			res, err := watchStream.Recv()
			result := watchResult{}
			if err != nil && err == io.EOF {
				util.Logger().Info("stop watch subscription because server send done.", zap.String("subId", subId))
				return
			} else if err != nil {
				if err != context.Canceled {
					return
				}
				util.Logger().Error("watch subscription error", zap.String("subId", subId), zap.Error(err))
				result.err = err
			} else {
				result.value = res
			}
			c.watchChannel <- result
		}
	}()
	return nil
}

func lookup(ctx context.Context, client client.Client, subId string, key string) (string, error) {
	address, err := util.RandomServer(client)
	if err != nil {
		return "", err
	}

	var req *hstreamrpc.Request
	if len(key) != 0 {
		req = &hstreamrpc.Request{
			Type: hstreamrpc.LookupSubscriptionWithOrderingKey,
			Req: &hstreampb.LookupSubscriptionWithOrderingKeyRequest{
				SubscriptionId: subId,
				OrderingKey:    key,
			},
		}
	} else {
		req = &hstreamrpc.Request{
			Type: hstreamrpc.LookupSubscription,
			Req: &hstreampb.LookupSubscriptionRequest{
				SubscriptionId: subId,
			},
		}
	}

	var resp *hstreamrpc.Response
	if resp, err = client.SendRequest(ctx, address, req); err != nil {
		util.Logger().Error("lookup subscription error", zap.String("subId", subId), zap.String("key", key), zap.Error(err))
		return "", errors.WithStack(err)
	}
	var node *hstreampb.ServerNode
	if len(key) != 0 {
		node = resp.Resp.(*hstreampb.LookupSubscriptionWithOrderingKeyResponse).GetServerNode()
		util.Logger().Debug("LookupSubscriptionWithOrderingKeyResponse", zap.String("subId", subId), zap.String("key", key), zap.String("node", node.String()))
	} else {
		node = resp.Resp.(*hstreampb.LookupSubscriptionResponse).GetServerNode()
		util.Logger().Debug("LookupSubscriptionResponse", zap.String("subId", subId), zap.String("node", node.String()))
	}
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}
