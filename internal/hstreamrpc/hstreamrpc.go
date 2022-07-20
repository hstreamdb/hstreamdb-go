package hstreamrpc

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/internal/retry"

	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ReqType represents the concrete rpc request type in Request.
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

	AdminRequest ReqType = 1024 + iota

	PerStreamStats ReqType = 2048 + iota
	PerStreamStatsAll
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
	case StreamingFetch:
		return "StreamingFetch"
	case DescribeCluster:
		return "DescribeCluster"
	case AdminRequest:
		return "AdminRequest"
	case PerStreamStats:
		return "PerStreamStats"
	case PerStreamStatsAll:
		return "PerStreamStatsAll"
	}
	return "Unknown"
}

// Request is a wrapper for all rpc requests.
type Request struct {
	Type ReqType
	Req  interface{}
}

// Response is a wrapper for all rpc responses.
type Response struct {
	Resp interface{}
}

// Call send rpc to server and wait for response.
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
		resp.Resp, err = cli.Append(ctx, req.Req.(*hstreampb.AppendRequest), retry.AppendRetry()...)
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
	case StreamingFetch:
		resp.Resp, err = cli.StreamingFetch(ctx, retry.FetchRetry()...)
	case DescribeCluster:
		resp.Resp, err = cli.DescribeCluster(ctx, req.Req.(*emptypb.Empty))
	case AdminRequest:
		resp.Resp, err = cli.SendAdminCommand(ctx, req.Req.(*hstreampb.AdminCommandRequest))
	case PerStreamStats:
		resp.Resp, err = cli.PerStreamTimeSeriesStats(ctx, req.Req.(*hstreampb.PerStreamTimeSeriesStatsRequest))
	case PerStreamStatsAll:
		resp.Resp, err = cli.PerStreamTimeSeriesStatsAll(ctx, req.Req.(*hstreampb.PerStreamTimeSeriesStatsAllRequest))
	}
	return resp, err
}
