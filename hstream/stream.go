package hstream

import (
	"context"
	"fmt"
	"github.com/hstreamdb/hstreamdb-go/hstreamrpc"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamDB/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"time"
)

const DEFAULTAPPENDTIMEOUT = time.Second * 5

type appendEntry struct {
	key        string
	value      client.HStreamRecord
	streamName string
	res        *hstreamrpc.RPCAppendRes
}

//type Stream struct {
//	writers map[string]*StreamProducer // FIXME: use Sync.Map to ensure thread safe ???
//	client  client2.Client
//}

type Stream struct {
	StreamName        string
	ReplicationFactor int16
	BacklogDuration   uint32
}

func NewStream(name string, replicationFactor int16, backlogDuration uint32) *Stream {
	return &Stream{
		StreamName:        name,
		ReplicationFactor: replicationFactor,
		BacklogDuration:   backlogDuration,
	}
}

func (c *HStreamClient) CreateStream(streamName string, replicationFactor uint32, backlogDuration uint32) error {
	stream := &hstreampb.Stream{
		StreamName:        streamName,
		ReplicationFactor: replicationFactor,
		BacklogDuration:   backlogDuration,
	}
	address, err := util.RandomServer(c)
	if err != nil {
		return err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.CreateStream,
		Req:  stream,
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
