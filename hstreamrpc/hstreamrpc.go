package hstreamrpc

import (
	hstreampb "client/gen-proto/hstream/server"
	"context"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ReqType uint16

const (
	CreateStream ReqType = 1 + iota
	DeleteStream
	ListStreams
	LookupStream
	Append

	CreateSubscription ReqType = 256 + iota
	ListSubscriptions
	CheckSubscriptionExist
	DeleteSubscription
	LookupSubscription
	LookupSubscriptionWithOrderingKey
	WatchSubscription
	StreamingFetch

	DescribeCluster ReqType = 512 + iota
)

func (t ReqType) String() string {
	switch t {
	case CreateStream:
		return "CreateStream"
	case DeleteStream:
		return "DeleteStream"
	case ListStreams:
		return "ListStreams"
	case LookupStream:
		return "LookupStream"
	case Append:
		return "Append"
	case CreateSubscription:
		return "CreateSubscription"
	case ListSubscriptions:
		return "ListSubscriptions"
	case CheckSubscriptionExist:
		return "CheckSubscriptionExist"
	case DeleteSubscription:
		return "DeleteSubscription"
	case LookupSubscription:
		return "LookupSubscription"
	case LookupSubscriptionWithOrderingKey:
		return "LookupSubscriptionWithOrderingKey"
	case WatchSubscription:
		return "WatchSubscription"
	case StreamingFetch:
		return "StreamingFetch"
	case DescribeCluster:
		return "DescribeCluster"
	}
	return "Unknown"
}

type Request struct {
	Type ReqType
	Req  interface{}
}

type Response struct {
	Resp interface{}
}

func Call(ctx context.Context, cli hstreampb.HStreamApiClient, req *Request) (*Response, error) {
	var err error
	resp := &Response{}
	switch req.Type {
	case CreateStream:
		resp.Resp, err = cli.CreateStream(ctx, req.Req.(*hstreampb.Stream))
	case DeleteStream:
		resp.Resp, err = cli.DeleteStream(ctx, req.Req.(*hstreampb.DeleteStreamRequest))
	case ListStreams:
		resp.Resp, err = cli.ListStreams(ctx, req.Req.(*hstreampb.ListStreamsRequest))
	case LookupStream:
		resp.Resp, err = cli.LookupStream(ctx, req.Req.(*hstreampb.LookupStreamRequest))
	case Append:
		resp.Resp, err = cli.Append(ctx, req.Req.(*hstreampb.AppendRequest))
	case CreateSubscription:
		resp.Resp, err = cli.CreateSubscription(ctx, req.Req.(*hstreampb.Subscription))
	case ListSubscriptions:
		resp.Resp, err = cli.ListSubscriptions(ctx, req.Req.(*hstreampb.ListSubscriptionsRequest))
	case CheckSubscriptionExist:
		resp.Resp, err = cli.CheckSubscriptionExist(ctx, req.Req.(*hstreampb.CheckSubscriptionExistRequest))
	case DeleteSubscription:
		resp.Resp, err = cli.DeleteSubscription(ctx, req.Req.(*hstreampb.DeleteSubscriptionRequest))
	case LookupSubscription:
		resp.Resp, err = cli.LookupSubscription(ctx, req.Req.(*hstreampb.LookupSubscriptionRequest))
	case LookupSubscriptionWithOrderingKey:
		resp.Resp, err = cli.LookupSubscriptionWithOrderingKey(ctx, req.Req.(*hstreampb.LookupSubscriptionWithOrderingKeyRequest))
	case WatchSubscription:
		resp.Resp, err = cli.WatchSubscription(ctx, req.Req.(*hstreampb.WatchSubscriptionRequest))
	case StreamingFetch:
		resp.Resp, err = cli.StreamingFetch(ctx)
	case DescribeCluster:
		resp.Resp, err = cli.DescribeCluster(ctx, req.Req.(*emptypb.Empty))
	}
	return resp, err
}

type RPCAppendRes struct {
	ready chan struct{}
	resp  *hstreampb.RecordId
	Err   error
}

func (r *RPCAppendRes) String() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	return r.resp.String()
}

func (r *RPCAppendRes) Ready() (*hstreampb.RecordId, error) {
	if r.Err != nil {
		return nil, r.Err
	}

	<-r.ready
	return r.resp, nil
}

func (r *RPCAppendRes) SetError(err error) {
	r.Err = err
	r.ready <- struct{}{}
}

func (r *RPCAppendRes) SetResponse(res interface{}) {
	r.resp = res.(*hstreampb.RecordId)
	r.ready <- struct{}{}
}

func NewRPCAppendRes() *RPCAppendRes {
	r := &RPCAppendRes{
		ready: make(chan struct{}, 1),
	}
	return r
}

type RPCFetchRes struct {
	result []*hstreampb.ReceivedRecord
	err    error
}

func (r *RPCFetchRes) SetError(err error) {
	r.err = err
}

func (r *RPCFetchRes) GetResult() ([]*hstreampb.ReceivedRecord, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.result, nil
}

func (r *RPCFetchRes) SetResult(res interface{}) {
	r.result = res.([]*hstreampb.ReceivedRecord)
}
