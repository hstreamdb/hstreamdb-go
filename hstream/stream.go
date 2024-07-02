package hstream

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
)

type Stream struct {
	StreamName        string
	ReplicationFactor uint32
	// backlog duration == 0 means forbidden backlog
	BacklogDuration uint32
	ShardCount      uint32
	CreationTime    time.Time
}

func (s *Stream) StreamToPb() *hstreampb.Stream {
	return &hstreampb.Stream{
		StreamName:        s.StreamName,
		ReplicationFactor: s.ReplicationFactor,
		BacklogDuration:   s.BacklogDuration,
		ShardCount:        s.ShardCount,
		CreationTime:      timestamppb.New(s.CreationTime),
	}
}

func StreamFromPb(pb *hstreampb.Stream) Stream {
	return Stream{
		StreamName:        pb.StreamName,
		ReplicationFactor: pb.ReplicationFactor,
		BacklogDuration:   pb.BacklogDuration,
		ShardCount:        pb.ShardCount,
		CreationTime:      pb.CreationTime.AsTime(),
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

// DeleteStreamOpts is the option for DeleteStream.
type DeleteStreamOpts func()

var (
	ForceDelete     = false
	IgnoreNoneExist = false
)

func EnableForceDelete() {
	ForceDelete = true
}

func EnableIgnoreNoneExist() {
	IgnoreNoneExist = true
}

// DeleteStream will send a DeleteStreamRPC to HStreamDB server and wait for response.
func (c *HStreamClient) DeleteStream(streamName string, opts ...DeleteStreamOpts) error {
	address, err := c.randomServer()
	if err != nil {
		return err
	}

	for _, opt := range opts {
		opt()
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.DeleteStream,
		Req: &hstreampb.DeleteStreamRequest{
			StreamName:     streamName,
			Force:          ForceDelete,
			IgnoreNonExist: IgnoreNoneExist,
		},
	}

	_, err = c.sendRequest(address, req)
	return err
}

// TrimStream will send a TrimStreamRPC to HStreamDB server and wait for response.
func (c *HStreamClient) TrimStream(streamName string, trimPoint StreamOffset) error {
	address, err := c.randomServer()
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.TrimStream,
		Req: &hstreampb.TrimStreamRequest{
			StreamName: streamName,
			TrimPoint:  trimPoint.toStreamOffset(),
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

// TrimShards will send a TrimShardsRPC to HStreamDB server. The server will find the oldest recordId
// in each shard, and trim all the records before that recordId(exclude)."
func (c *HStreamClient) TrimShards(streamName string, rids []string) (map[uint64]string, error) {
	address, err := c.randomServer()
	if err != nil {
		return nil, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.TrimShards,
		Req: &hstreampb.TrimShardsRequest{
			StreamName: streamName,
			RecordIds:  rids,
		},
	}

	res, err := c.sendRequest(address, req)
	if err != nil {
		return nil, err
	}
	return res.Resp.(*hstreampb.TrimShardsResponse).TrimPoints, nil
}

// NewProducer will create a Producer for specific stream
func (c *HStreamClient) NewProducer(streamName string) (*Producer, error) {
	return newProducer(c, streamName)
}

// NewBatchProducer will create a BatchProducer for specific stream
func (c *HStreamClient) NewBatchProducer(streamName string, opts ...ProducerOpt) (*BatchProducer, error) {
	return newBatchProducer(c, streamName, opts...)
}

// LookupShard will send a LookupShardRPC to HStreamDB server and wait for response.
func (c *HStreamClient) LookupShard(shardId uint64) (string, error) {
	address, err := c.randomServer()
	if err != nil {
		return "", err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.LookupShard,
		Req: &hstreampb.LookupShardRequest{
			ShardId: shardId,
		},
	}

	var resp *hstreamrpc.Response
	if resp, err = c.sendRequest(address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupShardResponse).GetServerNode()
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}
