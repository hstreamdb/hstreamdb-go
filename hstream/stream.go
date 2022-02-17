package hstream

import (
	"client/client"
	hstreampb "client/gen-proto/hstream/server"
	"client/hstreamrpc"
	"context"
	"fmt"
	"math/rand"
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

func (s *Stream) Create(ctx context.Context, streamName string, replicationFactor uint32) error {
	stream := &hstreampb.Stream{
		StreamName:        streamName,
		ReplicationFactor: replicationFactor,
	}
	address, err := s.randomServer()
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateStream,
		Req:  stream,
	}

	if _, err = s.client.SendRequest(ctx, address, req); err != nil {
		return err
	}
	return nil
}

func (s *Stream) Delete(ctx context.Context, streamName string) error {
	address, err := s.randomServer()
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteStream,
		Req: &hstreampb.DeleteStreamRequest{
			StreamName: streamName,
		},
	}

	if _, err = s.client.SendRequest(ctx, address, req); err != nil {
		return err
	}
	return nil
}

func (s *Stream) List(ctx context.Context) (*client.StreamIter, error) {
	address, err := s.randomServer()
	if err != nil {
		return nil, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.ListStreams,
		Req:  &hstreampb.ListStreamsRequest{},
	}

	var resp *hstreamrpc.Response
	if resp, err = s.client.SendRequest(ctx, address, req); err != nil {
		return nil, err
	}
	streams := resp.Resp.(*hstreampb.ListStreamsResponse).GetStreams()
	return client.NewStreamIter(streams), nil
}

func (s *Stream) LookUp(ctx context.Context, streamName string, key string) (string, error) {
	address, err := s.randomServer()
	if err != nil {
		return "", err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.LookupStream,
		Req: &hstreampb.LookupStreamRequest{
			StreamName:  streamName,
			OrderingKey: key,
		},
	}

	var resp *hstreamrpc.Response
	if resp, err = s.client.SendRequest(ctx, address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupStreamResponse).GetServerNode()
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}

func (s *Stream) Append(ctx context.Context, streamName string, key string, tp client.RecordType, data []byte) {
	//TODO implement me
	panic("implement me")
}

func (s *Stream) randomServer() (string, error) {
	infos, err := s.client.GetServerInfo()
	idx := rand.Intn(len(infos))
	if err != nil {
		return "", err
	}
	return infos[idx], nil
}
