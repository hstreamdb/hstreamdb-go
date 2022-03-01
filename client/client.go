package client

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstream/server"
	"time"
)

const DIALTIMEOUT = 5 * time.Second
const REQUESTTIMEOUT = 5 * time.Second
const DEFAULTKEY = "__default__"

type Client interface {
	GetServerInfo() ([]string, error)
	SendRequest(ctx context.Context, address string, req *hstreamrpc.Request) (*hstreamrpc.Response, error)
	Close()
}

type Iter interface {
	// Valid returns false when iteration is done.
	Valid() bool
	// Next would advance the iterator by one. It's user's responsibility
	// to check if the iterator is still valid after call Next()
	Next()
	// Close the iterator
	Close()
}

type AppendResult interface {
	Ready() (*hstreampb.RecordId, error)
	SetError(err error)
	SetResponse(res interface{})
}

type FetchResult interface {
	GetResult() ([]*hstreampb.ReceivedRecord, error)
	SetError(err error)
}

type StreamProducer interface {
	Append(tp RecordType, data []byte) AppendResult
	Stop()
}

type ProducerOpt func(producer StreamProducer)

type Stream interface {
	Create(ctx context.Context, streamName string, replicationFactor uint32) error
	Delete(ctx context.Context, streamName string) error
	List(ctx context.Context) (*StreamIter, error)
	MakeProducer(streamName string, key string, opts ...ProducerOpt) StreamProducer
}

type FetchResHandler interface {
	HandleRes(result FetchResult)
}

type StreamConsumer interface {
	Fetch(ctx context.Context, handler FetchResHandler)
	Stop()
}

type Subscription interface {
	Create(ctx context.Context, subId string, streamName string, ackTimeout int32) error
	Delete(ctx context.Context, subId string) error
	List(ctx context.Context) (*SubIter, error)
	CheckExist(ctx context.Context, subId string) (bool, error)
	MakeConsumer(subId string, consumerName string) StreamConsumer
}

type baseIter struct {
	length int
	cur    int
}

func (i *baseIter) Valid() bool {
	return i.cur >= 0 && i.cur < i.length
}

func (i *baseIter) Next() {
	i.cur++
}

func (i *baseIter) Close() {
	i.cur = -1
}

type StreamIter struct {
	streams []*hstreampb.Stream
	baseIter
}

func NewStreamIter(streams []*hstreampb.Stream) *StreamIter {
	iter := &StreamIter{streams: streams}
	iter.length = len(streams)
	iter.cur = 0
	return iter
}

func (i *StreamIter) GetStreams() []*hstreampb.Stream {
	return i.streams
}

func (i *StreamIter) Item() *hstreampb.Stream {
	return i.streams[i.cur]
}

type SubIter struct {
	subs []*hstreampb.Subscription
	baseIter
}

func NewSubIter(subs []*hstreampb.Subscription) *SubIter {
	iter := &SubIter{subs: subs}
	iter.length = len(subs)
	iter.cur = 0
	return iter
}

func (i *SubIter) GetSubs() []*hstreampb.Subscription {
	return i.subs
}

func (i *SubIter) Item() *hstreampb.Subscription {
	return i.subs[i.cur]
}
