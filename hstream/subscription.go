package hstream

import (
	"client/client"
	hstreampb "client/gen-proto/hstream/server"
	"client/hstreamrpc"
	"context"
	"fmt"
	"math/rand"
)

type Subscription struct {
	client client.Client
}

func NewSubscription(client client.Client) *Subscription {
	return &Subscription{client: client}
}

func (s *Subscription) Create(ctx context.Context, subId string, streamName string, ackTimeout int32) error {
	sub := &hstreampb.Subscription{
		SubscriptionId:    subId,
		StreamName:        streamName,
		AckTimeoutSeconds: ackTimeout,
	}
	address, err := s.randomServer()
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
	address, err := s.randomServer()
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
	address, err := s.randomServer()
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
	address, err := s.randomServer()
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

func (s *Subscription) Fetch(ctx context.Context, subId string, key string, handler client.MsgHandler) error {
	//TODO implement me
	panic("implement me")
}

func (s *Subscription) lookUp(ctx context.Context, subId string) (string, error) {
	address, err := s.randomServer()
	if err != nil {
		return "", err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.LookupSubscription,
		Req: &hstreampb.LookupSubscriptionRequest{
			SubscriptionId: subId,
		},
	}

	var resp *hstreamrpc.Response
	if resp, err = s.client.SendRequest(ctx, address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupSubscriptionResponse).GetServerNode()
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}

func (s *Subscription) lookUpWithKey(ctx context.Context, subId string, key string) (string, error) {
	address, err := s.randomServer()
	if err != nil {
		return "", err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.LookupSubscriptionWithOrderingKey,
		Req: &hstreampb.LookupSubscriptionWithOrderingKeyRequest{
			SubscriptionId: subId,
			OrderingKey:    key,
		},
	}

	var resp *hstreamrpc.Response
	if resp, err = s.client.SendRequest(ctx, address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupSubscriptionWithOrderingKeyResponse).GetServerNode()
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}

func (s *Subscription) watch(ctx context.Context, subId string) (*hstreamrpc.Response, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Subscription) randomServer() (string, error) {
	infos, err := s.client.GetServerInfo()
	idx := rand.Intn(len(infos))
	if err != nil {
		return "", err
	}
	return infos[idx], nil
}
