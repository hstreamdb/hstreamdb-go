package hstream

import (
	"fmt"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
)

type Subscription struct {
	SubscriptionId    string
	StreamName        string
	AckTimeoutSeconds int32
}

func (s *Subscription) SubscriptionToPb() *hstreampb.Subscription {
	return &hstreampb.Subscription{
		SubscriptionId:    s.SubscriptionId,
		StreamName:        s.StreamName,
		AckTimeoutSeconds: s.AckTimeoutSeconds,
	}
}

// CreateSubscription will send a CreateSubscriptionRPC to the server and wait for response.
func (c *HStreamClient) CreateSubscription(subId string, streamName string, ackTimeout int32) error {
	sub := &hstreampb.Subscription{
		SubscriptionId:    subId,
		StreamName:        streamName,
		AckTimeoutSeconds: ackTimeout,
	}

	address, err := c.lookUpSubscription(subId)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateSubscription,
		Req:  sub,
	}

	_, err = c.sendRequest(address, req)
	return err
}

// DeleteSubscription will send a DeleteSubscriptionRPC to the server and wait for response.
func (c *HStreamClient) DeleteSubscription(subId string) error {
	address, err := c.lookUpSubscription(subId)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteSubscription,
		Req: &hstreampb.DeleteSubscriptionRequest{
			SubscriptionId: subId,
		},
	}

	_, err = c.sendRequest(address, req)
	return err
}

// ListSubscriptions will send a ListSubscriptionsRPC to the server and wait for response.
func (c *HStreamClient) ListSubscriptions() (*client.SubIter, error) {
	address, err := c.randomServer()
	if err != nil {
		return nil, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.ListSubscriptions,
		Req:  &hstreampb.ListSubscriptionsRequest{},
	}

	var resp *hstreamrpc.Response
	if resp, err = c.sendRequest(address, req); err != nil {
		return nil, err
	}
	subs := resp.Resp.(*hstreampb.ListSubscriptionsResponse).GetSubscription()
	return client.NewSubIter(subs), nil
}

// CheckExist will send a CheckExistRPC to the server and wait for response.
func (c *HStreamClient) CheckExist(subId string) (bool, error) {
	address, err := c.randomServer()
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
	if resp, err = c.sendRequest(address, req); err != nil {
		return false, err
	}
	isExist := resp.Resp.(*hstreampb.CheckSubscriptionExistResponse).Exists
	return isExist, nil
}

// NewConsumer will create a new Consumer for the specific subscription.
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
	address, err := c.randomServer()
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
	if resp, err = c.sendRequest(address, req); err != nil {
		return "", err
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
