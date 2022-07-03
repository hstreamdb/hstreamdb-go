package hstream

import (
	"fmt"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Subscription struct {
	SubscriptionId    string
	StreamName        string
	AckTimeoutSeconds int32
	MaxUnackedRecords int32
}

func (s *Subscription) SubscriptionToPb() *hstreampb.Subscription {
	return &hstreampb.Subscription{
		SubscriptionId:    s.SubscriptionId,
		StreamName:        s.StreamName,
		AckTimeoutSeconds: s.AckTimeoutSeconds,
		MaxUnackedRecords: s.MaxUnackedRecords,
	}
}

func SubscriptionFromPb(pb *hstreampb.Subscription) Subscription {
	return Subscription{
		SubscriptionId:    pb.SubscriptionId,
		StreamName:        pb.StreamName,
		AckTimeoutSeconds: pb.AckTimeoutSeconds,
		MaxUnackedRecords: pb.MaxUnackedRecords,
	}
}

// CreateSubscription will create a subscription with MaxUnackedRecords set to 10000 by default
func (c *HStreamClient) CreateSubscription(subId string, streamName string, ackTimeout int32) error {
	sub := &hstreampb.Subscription{
		SubscriptionId:    subId,
		StreamName:        streamName,
		AckTimeoutSeconds: ackTimeout,
		MaxUnackedRecords: 10000,
	}
	return c.createSubscription(sub)
}

// CreateSubscriptionWithMaxUnack will create a subscription with the specified MaxUnackedRecords
func (c *HStreamClient) CreateSubscriptionWithMaxUnack(
	subId string, streamName string, ackTimeout int32, maxUnack int32) error {
	if maxUnack < 0 {
		return errors.New("maxUnack must be greater than or equal to 0")
	}

	sub := &hstreampb.Subscription{
		SubscriptionId:    subId,
		StreamName:        streamName,
		AckTimeoutSeconds: ackTimeout,
		MaxUnackedRecords: maxUnack,
	}
	return c.createSubscription(sub)
}

// createSubscription will send a CreateSubscriptionRPC to the server and wait for response.
func (c *HStreamClient) createSubscription(sub *hstreampb.Subscription) error {
	address, err := c.lookUpSubscription(sub.SubscriptionId)
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
func (c *HStreamClient) DeleteSubscription(subId string, force bool) error {
	address, err := c.lookUpSubscription(subId)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteSubscription,
		Req: &hstreampb.DeleteSubscriptionRequest{
			SubscriptionId: subId,
			Force:          force,
		},
	}

	_, err = c.sendRequest(address, req)
	return err
}

// ListSubscriptions will send a ListSubscriptionsRPC to the server and wait for response.
func (c *HStreamClient) ListSubscriptions() ([]Subscription, error) {
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
	res := make([]Subscription, 0, len(subs))
	for _, sub := range subs {
		res = append(res, SubscriptionFromPb(sub))
	}
	return res, nil
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
			Type: hstreamrpc.LookupSubscription,
			Req: &hstreampb.LookupSubscriptionRequest{
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
		node = resp.Resp.(*hstreampb.LookupSubscriptionResponse).GetServerNode()
		util.Logger().Debug("LookupSubscriptionWithOrderingKeyResponse", zap.String("subId", subId), zap.String("key", key), zap.String("node", node.String()))
	} else {
		node = resp.Resp.(*hstreampb.LookupSubscriptionResponse).GetServerNode()
		util.Logger().Debug("LookupSubscriptionResponse", zap.String("subId", subId), zap.String("node", node.String()))
	}
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}
