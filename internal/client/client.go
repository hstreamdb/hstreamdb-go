package client

import (
	"context"
	"time"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
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
