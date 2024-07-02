package hstream

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
)

type SubscriptionOffset uint8

const (
	EARLIEST SubscriptionOffset = iota + 1
	LATEST
)

type Subscription struct {
	SubscriptionId    string
	StreamName        string
	AckTimeoutSeconds int32
	MaxUnackedRecords int32
	Offset            SubscriptionOffset
	CreationTime      time.Time
}

func (s *Subscription) SubscriptionToPb() *hstreampb.Subscription {
	return &hstreampb.Subscription{
		SubscriptionId:    s.SubscriptionId,
		StreamName:        s.StreamName,
		AckTimeoutSeconds: s.AckTimeoutSeconds,
		MaxUnackedRecords: s.MaxUnackedRecords,
		Offset:            SubscriptionOffsetToPb(s.Offset),
		CreationTime:      timestamppb.New(s.CreationTime),
	}
}

func SubscriptionFromPb(pb *hstreampb.Subscription) Subscription {
	return Subscription{
		SubscriptionId:    pb.SubscriptionId,
		StreamName:        pb.StreamName,
		AckTimeoutSeconds: pb.AckTimeoutSeconds,
		MaxUnackedRecords: pb.MaxUnackedRecords,
		Offset:            SubscriptionOffsetFromPb(pb.Offset),
		CreationTime:      pb.CreationTime.AsTime(),
	}
}

// SubscriptionOpts is the option for the Subscription.
type SubscriptionOpts func(sub *Subscription)

// WithAckTimeout sets the ack timeout in seconds.
func WithAckTimeout(timeout int32) SubscriptionOpts {
	return func(sub *Subscription) {
		sub.AckTimeoutSeconds = timeout
	}
}

// WithMaxUnackedRecords sets the max unacked records. If the number of records that have not
// been acked reaches the set value, the server will stop pushing more records to the client.
func WithMaxUnackedRecords(cnt int32) SubscriptionOpts {
	return func(sub *Subscription) {
		sub.MaxUnackedRecords = cnt
	}
}

// WithOffset sets the subscription offset.
func WithOffset(offset SubscriptionOffset) SubscriptionOpts {
	return func(sub *Subscription) {
		sub.Offset = offset
	}
}

func defaultSub(subId string, streamName string) Subscription {
	return Subscription{
		SubscriptionId:    subId,
		StreamName:        streamName,
		AckTimeoutSeconds: 600,
		MaxUnackedRecords: 10000,
		Offset:            LATEST,
	}
}

// CreateSubscription will send a CreateSubscriptionRPC to HStreamDB server and wait for response.
func (c *HStreamClient) CreateSubscription(subId string, streamName string, opts ...SubscriptionOpts) error {
	sub := defaultSub(subId, streamName)
	for _, opt := range opts {
		opt(&sub)
	}

	if sub.AckTimeoutSeconds <= 0 {
		return errors.New("ack timeout should greater than 0")
	}
	if sub.MaxUnackedRecords <= 0 {
		return errors.New("max unacked records should greater than 0.")
	}

	address, err := c.lookUpSubscription(sub.SubscriptionId)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateSubscription,
		Req:  sub.SubscriptionToPb(),
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
	address, err := c.randomServer()
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
	if resp, err = c.sendRequest(address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupSubscriptionResponse).GetServerNode()
	util.Logger().Debug("LookupSubscriptionResponse", zap.String("subId", subId), zap.String("node", node.String()))
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}
