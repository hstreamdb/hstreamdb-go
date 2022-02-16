package client

import (
	hstreampb "client/gen-proto/hstream/server"
	"client/hstreamrpc"
	"context"
	"time"
)

const DIALTIMEOUT = 5 * time.Second
const DEFAULTKEY = "__default__"

type Client interface {
	SendRequest(ctx context.Context, address string, req *hstreamrpc.Request) (*hstreamrpc.Response, error)
	Close() error
}

// FIXME
type StreamIterator = []*hstreampb.Stream
type SubIterator = []*hstreampb.Subscription

type Stream interface {
	Create(ctx context.Context, streamName string, replicationFactor uint32) error
	Delete(ctx context.Context, streamName string) error
	List(ctx context.Context) (StreamIterator, error)
	Append(ctx context.Context, streamName string, key string, tp RecordType, data []byte)
	//LookUp(ctx context.Context, streamName string, key string) (*hstreamrpc.Response, error)
}

type MsgHandler func(item interface{})

type Subscription interface {
	Create(ctx context.Context, subId string, subName string, ackTimeout time.Duration) error
	Delete(ctx context.Context, subId string) error
	List(ctx context.Context) (SubIterator, error)
	CheckExist(ctx context.Context, subId string) (bool, error)
	Fetch(ctx context.Context, subId string, key string, handler MsgHandler) error
	//LookUp(ctx context.Context, subId string) (*hstreamrpc.Response, error)
	//LookUpWithKey(ctx context.Context, subId string, key string) (*hstreamrpc.Response, error)
	//Watch(ctx context.Context, subId string) (*hstreamrpc.Response, error)
}
