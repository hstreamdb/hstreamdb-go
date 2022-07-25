package hstream

import (
	"fmt"
	"github.com/pkg/errors"
	"time"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
)

const DEFAULTAPPENDTIMEOUT = time.Second * 5

type Stream struct {
	StreamName        string
	ReplicationFactor uint32
	// backlog duration == 0 means forbidden backlog
	BacklogDuration uint32
	ShardCount      uint32
}

func (s *Stream) StreamToPb() *hstreampb.Stream {
	return &hstreampb.Stream{
		StreamName:        s.StreamName,
		ReplicationFactor: s.ReplicationFactor,
		BacklogDuration:   s.BacklogDuration,
		ShardCount:        s.ShardCount,
	}
}

func StreamFromPb(pb *hstreampb.Stream) Stream {
	return Stream{
		StreamName:        pb.StreamName,
		ReplicationFactor: pb.ReplicationFactor,
		BacklogDuration:   pb.BacklogDuration,
		ShardCount:        pb.ShardCount,
	}
}

// StreamOpts is the option for the Stream.
type StreamOpts func(stream *Stream)

// WithReplicationFactor sets the replication factor of the stream.
func WithReplicationFactor(replicationFactor uint32) StreamOpts {
	return func(stream *Stream) {
		stream.ReplicationFactor = replicationFactor
	}
}

// EnableBacklog sets the backlog duration in seconds for the stream.
func EnableBacklog(backlogDuration uint32) StreamOpts {
	return func(stream *Stream) {
		stream.BacklogDuration = backlogDuration
	}
}

// WithShardCount sets the number of shards in the stream.
func WithShardCount(shardCnt uint32) StreamOpts {
	return func(stream *Stream) {
		stream.ShardCount = shardCnt
	}
}

// defaultStream create a default stream with 3 replicas,
// the backlog duration is set to 7 days
func defaultStream(name string) Stream {
	return Stream{
		StreamName:        name,
		ReplicationFactor: 3,
		BacklogDuration:   7 * 24 * 60 * 60,
		ShardCount:        1,
	}
}

// CreateStream will send a CreateStreamRPC to HStreamDB server and wait for response.
func (c *HStreamClient) CreateStream(streamName string, opts ...StreamOpts) error {
	stream := defaultStream(streamName)
	for _, opt := range opts {
		opt(&stream)
	}
	if stream.ReplicationFactor < 1 {
		return errors.New("replication factor must be greater than or equal to 0")
	}
	if stream.ShardCount <= 0 {
		return errors.New("shard count must be greater than 0")
	}

	address, err := c.randomServer()
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateStream,
		Req:  stream.StreamToPb(),
	}

	_, err = c.sendRequest(address, req)
	return err
}

// DeleteStream will send a DeleteStreamRPC to HStreamDB server and wait for response.
func (c *HStreamClient) DeleteStream(streamName string) error {
	address, err := c.randomServer()
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteStream,
		Req: &hstreampb.DeleteStreamRequest{
			StreamName: streamName,
		},
	}

	_, err = c.sendRequest(address, req)
	return err
}

// ListStreams will send a ListStreamsRPC to HStreamDB server and wait for response.
func (c *HStreamClient) ListStreams() ([]Stream, error) {
	address, err := c.randomServer()
	if err != nil {
		return nil, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.ListStreams,
		Req:  &hstreampb.ListStreamsRequest{},
	}

	var resp *hstreamrpc.Response
	if resp, err = c.sendRequest(address, req); err != nil {
		return nil, err
	}
	streams := resp.Resp.(*hstreampb.ListStreamsResponse).GetStreams()
	res := make([]Stream, 0, len(streams))
	for _, stream := range streams {
		res = append(res, StreamFromPb(stream))
	}
	return res, nil
}

// NewProducer will create a Producer for specific stream
func (c *HStreamClient) NewProducer(streamName string) *Producer {
	return newProducer(c, streamName)
}

// NewBatchProducer will create a BatchProducer for specific stream
func (c *HStreamClient) NewBatchProducer(streamName string, opts ...ProducerOpt) (*BatchProducer, error) {
	return newBatchProducer(c, streamName, opts...)
}

// LookUpStream will send a LookUpStreamRPC to HStreamDB server and wait for response.
func (c *HStreamClient) LookUpStream(streamName string, key string) (string, error) {
	address, err := c.randomServer()
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
	if resp, err = c.sendRequest(address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupStreamResponse).GetServerNode()
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}
