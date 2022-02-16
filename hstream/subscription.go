package hstream

import (
	"client/client"
	"client/hstreamrpc"
	"context"
	"time"
)

type Subscription struct {
	client client.Client
}

func NewSubscription(client client.Client) *Subscription {
	return &Subscription{client: client}
}

func (s Subscription) Create(ctx context.Context, subId string, subName string, ackTimeout time.Duration) error {
	//TODO implement me
	panic("implement me")
}

func (s Subscription) Delete(ctx context.Context, subId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Subscription) List(ctx context.Context) (client.SubIterator, error) {
	//TODO implement me
	panic("implement me")
}

func (s Subscription) CheckExist(ctx context.Context, subId string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (s Subscription) Fetch(ctx context.Context, subId string, key string, handler client.MsgHandler) error {
	//TODO implement me
	panic("implement me")
}

func (s Subscription) lookUp(ctx context.Context, subId string) (*hstreamrpc.Response, error) {
	//TODO implement me
	panic("implement me")
}

func (s Subscription) lookUpWithKey(ctx context.Context, subId string, key string) (*hstreamrpc.Response, error) {
	//TODO implement me
	panic("implement me")
}

func (s Subscription) watch(ctx context.Context, subId string) (*hstreamrpc.Response, error) {
	//TODO implement me
	panic("implement me")
}
