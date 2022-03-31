package hstream

import (
	"context"
	"fmt"
	"time"

	"github.com/hstreamdb/hstreamdb-go/internal/client"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
)

const DEFAULTAPPENDTIMEOUT = time.Second * 5

type Stream struct {
	StreamName        string
	ReplicationFactor uint32
	// backlog duration == 0 means forbidden backlog
	BacklogDuration uint32
}

func (s *Stream) ToPbHStreamStream() *hstreampb.Stream {
	return &hstreampb.Stream{
		StreamName:        s.StreamName,
		ReplicationFactor: s.ReplicationFactor,
		BacklogDuration:   s.BacklogDuration,
	}
}

type StreamOpts func(stream *Stream)

func WithReplicationFactor(replicationFactor uint32) StreamOpts {
	return func(stream *Stream) {
		stream.ReplicationFactor = replicationFactor
	}
}

func EnableBacklog(backlogDuration uint32) StreamOpts {
	return func(stream *Stream) {
		stream.BacklogDuration = backlogDuration
	}
}

// defaultStream create a default stream with 3 replicas,
// the backlog duration is set to 0, which means forbidden backlog
func defaultStream(name string) *Stream {
	return &Stream{
		StreamName:        name,
		ReplicationFactor: 3,
		BacklogDuration:   0,
	}
}

func (c *HStreamClient) CreateStream(streamName string, opts ...StreamOpts) error {
	stream := defaultStream(streamName)
	for _, opt := range opts {
		opt(stream)
	}
	if stream.ReplicationFactor < 1 {
		return fmt.Errorf("replication factor must be greater than 0")
	}

	address, err := util.RandomServer(c)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateStream,
		Req:  stream.ToPbHStreamStream(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if _, err = c.SendRequest(ctx, address, req); err != nil {
		return err
	}
	return nil
}

func (c *HStreamClient) DeleteStream(streamName string) error {
	address, err := util.RandomServer(c)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteStream,
		Req: &hstreampb.DeleteStreamRequest{
			StreamName: streamName,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if _, err = c.SendRequest(ctx, address, req); err != nil {
		return err
	}
	return nil
}

func (c *HStreamClient) ListStreams() (*client.StreamIter, error) {
	address, err := util.RandomServer(c)
	if err != nil {
		return nil, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.ListStreams,
		Req:  &hstreampb.ListStreamsRequest{},
	}

	var resp *hstreamrpc.Response
	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if resp, err = c.SendRequest(ctx, address, req); err != nil {
		return nil, err
	}
	streams := resp.Resp.(*hstreampb.ListStreamsResponse).GetStreams()
	return client.NewStreamIter(streams), nil
}

func (c *HStreamClient) NewProducer(streamName string) *Producer {
	return newProducer(c, streamName)
}

func (c *HStreamClient) NewBatchProducer(streamName string, opts ...ProducerOpt) (*BatchProducer, error) {
	return newBatchProducer(c, streamName, opts...)
}

func (c *HStreamClient) LookUpStream(streamName string, key string) (string, error) {
	address, err := util.RandomServer(c)
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
	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	if resp, err = c.SendRequest(ctx, address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupStreamResponse).GetServerNode()
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}
