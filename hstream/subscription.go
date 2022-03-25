package hstream

import (
	"context"
	"fmt"
	"sync"

	"github.com/hstreamdb/hstreamdb-go/internal/client"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Subscription struct {
	client   client.Client
	consumer map[string]*Consumer
	sync.RWMutex
}

func NewSubscription(client client.Client) *Subscription {
	return &Subscription{
		client:   client,
		consumer: make(map[string]*Consumer),
	}
}

func (c *HStreamClient) CreateSubscription(subId string, streamName string, ackTimeout int32) error {
	sub := &hstreampb.Subscription{
		SubscriptionId:    subId,
		StreamName:        streamName,
		AckTimeoutSeconds: ackTimeout,
	}
	address, err := util.RandomServer(c)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateSubscription,
		Req:  sub,
	}

	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if _, err = c.SendRequest(ctx, address, req); err != nil {
		return err
	}
	return nil
}

func (c *HStreamClient) DeleteSubscription(subId string) error {
	address, err := util.RandomServer(c)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteSubscription,
		Req: &hstreampb.DeleteSubscriptionRequest{
			SubscriptionId: subId,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if _, err = c.SendRequest(ctx, address, req); err != nil {
		return err
	}
	return nil
}

func (c *HStreamClient) ListSubscriptions() (*client.SubIter, error) {
	address, err := util.RandomServer(c)
	if err != nil {
		return nil, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.ListSubscriptions,
		Req:  &hstreampb.ListSubscriptionsRequest{},
	}

	var resp *hstreamrpc.Response
	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if resp, err = c.SendRequest(ctx, address, req); err != nil {
		return nil, err
	}
	subs := resp.Resp.(*hstreampb.ListSubscriptionsResponse).GetSubscription()
	return client.NewSubIter(subs), nil
}

func (c *HStreamClient) CheckExist(subId string) (bool, error) {
	address, err := util.RandomServer(c)
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
	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if resp, err = c.SendRequest(ctx, address, req); err != nil {
		return false, err
	}
	isExist := resp.Resp.(*hstreampb.CheckSubscriptionExistResponse).Exists
	return isExist, nil
}

func (c *HStreamClient) NewConsumer(consumerName, subId string) *Consumer {
	return NewConsumer(c, subId, consumerName)
}

func (c *HStreamClient) lookUpSubscription(subId string) (string, error) {
	return c.lookup(subId, "")
}

func (c *HStreamClient) lookUpSubscriptionWithKey(subId string, key string) (string, error) {
	return c.lookup(subId, key)
}

func (c *HStreamClient) lookup(subId string, key string) (string, error) {
	address, err := util.RandomServer(c)
	if err != nil {
		return "", err
	}

	var req *hstreamrpc.Request
	if len(key) != 0 {
		req = &hstreamrpc.Request{
			Type: hstreamrpc.LookupSubscriptionWithOrderingKey,
			Req: &hstreampb.LookupSubscriptionWithOrderingKeyRequest{
				SubscriptionId: subId,
				// FIXME:
				//OrderingKey:    key,
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
	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if resp, err = c.SendRequest(ctx, address, req); err != nil {
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
