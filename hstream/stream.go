package hstream

import (
	"client/client"
	hstreampb "client/gen-proto/hstream/server"
	"client/hstreamrpc"
	"client/util"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"strconv"
	"time"
)

const DEFAULTAPPENDTIMEOUT = time.Second * 5

type appendEntry struct {
	key        string
	value      client.HStreamRecord
	streamName string
	res        *hstreamrpc.RPCAppendRes
}

type Stream struct {
	writers map[string]*StreamProducer // FIXME: use Sync.Map to ensure thread safe ???
	client  client.Client
}

func NewStream(client client.Client) *Stream {
	return &Stream{
		client:  client,
		writers: make(map[string]*StreamProducer),
	}
}

func (s *Stream) Create(ctx context.Context, streamName string, replicationFactor uint32) error {
	stream := &hstreampb.Stream{
		StreamName:        streamName,
		ReplicationFactor: replicationFactor,
	}
	address, err := util.RandomServer(s.client)
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
	address, err := util.RandomServer(s.client)
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
	address, err := util.RandomServer(s.client)
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

func (s *Stream) MakeProducer(streamName string, key string, opts ...client.ProducerOpt) client.StreamProducer {
	pId := streamName + ":" + key
	if producer, ok := s.writers[pId]; ok {
		return producer
	}

	producer := newStreamProducer(s.client, streamName, key, opts...)
	if producer.enableBatch {
		producer.dataCh = make(chan *appendEntry, producer.batchSize)
		go producer.batchAppendLoop()
	}
	s.writers[pId] = producer
	util.Logger().Info("new producer", zap.String("streamName", streamName), zap.String("key", key))
	return producer
}

type StreamProducer struct {
	streamName  string
	key         string
	client      client.Client
	enableBatch bool
	batchSize   int
	buffer      []*appendEntry
	timeOut     time.Duration
	isClosed    bool

	dataCh chan *appendEntry
	stop   chan struct{}
}

func newStreamProducer(client client.Client, streamName string, key string, opts ...client.ProducerOpt) *StreamProducer {
	producer := &StreamProducer{
		streamName:  streamName,
		key:         key,
		client:      client,
		enableBatch: false,
		batchSize:   0,
		timeOut:     10 * time.Second,
		isClosed:    false,
		stop:        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(producer)
	}
	return producer
}

// EnableBatch enable batch append, if batchSize <= 0, convert to
// single append by default
func EnableBatch(batchSize int) client.ProducerOpt {
	return func(producer client.StreamProducer) {
		if batchSize <= 0 {
			util.Logger().Warn("batch size must be greater than 0")
			return
		}

		p := producer.(*StreamProducer)
		p.enableBatch = true
		p.batchSize = batchSize
		p.buffer = make([]*appendEntry, 0, batchSize)
	}
}

func TimeOut(timeOut time.Duration) client.ProducerOpt {
	return func(producer client.StreamProducer) {
		p := producer.(*StreamProducer)
		p.timeOut = timeOut
	}
}

func (p *StreamProducer) Stop() {
	defer func() {
		for i := 0; i < len(p.buffer); i++ {
			p.buffer[i] = nil
		}
		p.buffer = p.buffer[:0]
	}()
	util.Logger().Info("producer stopped", zap.String("streamName", p.streamName), zap.String("key", p.key))
	p.isClosed = true
	close(p.stop)
}

func (p *StreamProducer) Append(tp client.RecordType, data []byte) client.AppendResult {
	res := hstreamrpc.NewRPCAppendRes()
	var record client.HStreamRecord
	switch tp {
	case client.RAWRECORD:
		record = client.NewRawRecord(p.key, data)
	case client.HRECORD:
		record = client.NewHRecord(p.key, data)
	default:
		res.Err = errors.New("unsupported record type")
		return res
	}

	entry := &appendEntry{
		streamName: p.streamName,
		key:        p.key,
		value:      record,
		res:        res,
	}

	if p.enableBatch {
		p.dataCh <- entry
		return res
	}

	p.sendAppend([]*appendEntry{entry})
	return res
}

func (p *StreamProducer) fetchBatchData() []*appendEntry {
	timer := time.NewTimer(p.timeOut)
	defer timer.Stop()
	p.buffer = p.buffer[:0]

	for {
		select {
		case record := <-p.dataCh:
			p.buffer = append(p.buffer, record)
			if len(p.buffer) >= p.batchSize {
				if !timer.Stop() {
					<-timer.C
				}
				res := make([]*appendEntry, p.batchSize)
				copy(res, p.buffer)
				return res
			}
		case <-timer.C:
			util.Logger().Debug("Timeout!!!!!!!", zap.String("length of buffer", strconv.Itoa(len(p.buffer))))
			size := len(p.buffer)
			if size == 0 {
				return nil
			}
			res := make([]*appendEntry, size)
			copy(res, p.buffer)
			return res
		case <-p.stop:
			return nil
		}
	}
}

func (p *StreamProducer) batchAppendLoop() {
	for {
		if p.isClosed {
			return
		}

		records := p.fetchBatchData()
		if len(records) == 0 {
			continue
		}
		p.sendAppend(records)
	}
}

func (p *StreamProducer) sendAppend(records []*appendEntry) {
	reqRecords := make([]*hstreampb.HStreamRecord, 0, len(records))
	for _, record := range records {
		reqRecords = append(reqRecords, record.value.ToHStreamRecord())
	}
	req := &hstreamrpc.Request{
		Type: hstreamrpc.Append,
		Req: &hstreampb.AppendRequest{
			StreamName: p.streamName,
			Records:    reqRecords,
		},
	}

	// FIXME: add cache to avoid lookup zk every time
	server, err := p.lookUp(context.Background(), p.streamName, p.key)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}

	res, err := p.client.SendRequest(context.Background(), server, req)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}

	size := len(records)
	rids := res.Resp.(*hstreampb.AppendResponse).GetRecordIds()
	for i := 0; i < size; i++ {
		records[i].res.SetResponse(rids[i])
	}
}

func (p *StreamProducer) lookUp(ctx context.Context, streamName string, key string) (string, error) {
	address, err := util.RandomServer(p.client)
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
	if resp, err = p.client.SendRequest(ctx, address, req); err != nil {
		return "", err
	}
	node := resp.Resp.(*hstreampb.LookupStreamResponse).GetServerNode()
	return fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort()), nil
}

func handleBatchAppendError(err error, records []*appendEntry) {
	for _, record := range records {
		record.res.SetError(err)
	}
}
