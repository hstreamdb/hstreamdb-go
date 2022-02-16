package hstream

import (
	"client/client"
	"client/hstreamrpc"
	"context"
	"time"
)

const DEFAULTAPPENDTIMEOUT = time.Second * 5

type AppenderOpt func(appender *Appender)

type Appender struct {
	enableBatch bool
	batchSize   int32
	timeOut     time.Duration
}

func EnableBatch(batchSize int32) AppenderOpt {
	return func(appender *Appender) {
		appender.enableBatch = true
		appender.batchSize = batchSize
	}
}

func TimeOut(timeOut time.Duration) AppenderOpt {
	return func(appender *Appender) {
		appender.timeOut = timeOut
	}
}

func newDefaultAppender() *Appender {
	return &Appender{
		enableBatch: false,
		batchSize:   1,
		timeOut:     DEFAULTAPPENDTIMEOUT,
	}
}

func newAppender(opts ...AppenderOpt) *Appender {
	appender := newDefaultAppender()
	for _, opt := range opts {
		opt(appender)
	}
	return appender
}

type Stream struct {
	Appender
	client client.Client
}

func NewStream(client client.Client, opts ...AppenderOpt) *Stream {
	if len(opts) == 0 {
		return &Stream{Appender: *newDefaultAppender(), client: client}
	} else {
		return &Stream{Appender: *newAppender(opts...), client: client}
	}
}

func (s Stream) Create(ctx context.Context, streamName string, replicationFactor uint32) error {
	//TODO implement me
	panic("implement me")
}

func (s Stream) Delete(ctx context.Context, streamName string) error {
	//TODO implement me
	panic("implement me")
}

func (s Stream) List(ctx context.Context) (client.StreamIterator, error) {
	//TODO implement me
	panic("implement me")
}

func (s Stream) LookUp(ctx context.Context, streamName string, key string) (*hstreamrpc.Response, error) {
	//TODO implement me
	panic("implement me")
}

func (s Stream) Append(ctx context.Context, streamName string, key string, tp client.RecordType, data []byte) {
	//TODO implement me
	panic("implement me")
}
