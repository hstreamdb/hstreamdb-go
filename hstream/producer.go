package hstream

import (
	"context"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hstreamdb/hstreamdb-go/internal/client"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const DEFAULT_BATCHPRODUCER_FLUSH_TIMEOUT = 10 * time.Second

type appendEntry struct {
	key        string
	value      *hstreampb.HStreamRecord
	streamName string
	res        *rpcAppendRes
}

type AppendResult interface {
	Ready() (*RecordId, error)
	SetError(err error)
	SetResponse(res interface{})
}

// rpcAppendRes FIXME:
// - find another way to replace channel here.
//	  - if somebody create rpcAppendRes but never call SetResponse or SetError,
// 		the channel may casue memory leak.
//    - if somebody call SetResponse and SetError, the channel will panic.
type rpcAppendRes struct {
	ready chan struct{}
	resp  *RecordId
	Err   error
}

func newRPCAppendRes() *rpcAppendRes {
	return &rpcAppendRes{
		ready: make(chan struct{}, 1),
	}
}

func (r *rpcAppendRes) String() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	return r.resp.String()
}

func (r *rpcAppendRes) Ready() (*RecordId, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	<-r.ready
	return r.resp, nil
}

func (r *rpcAppendRes) SetError(err error) {
	defer close(r.ready)
	r.Err = err
	r.ready <- struct{}{}
}

func (r *rpcAppendRes) SetResponse(res interface{}) {
	defer close(r.ready)
	r.resp = FromPbRecordId(res.(*hstreampb.RecordId))
	r.ready <- struct{}{}
}

type Producer struct {
	client     *HStreamClient
	streamName string
}

func newProducer(client *HStreamClient, streamName string) *Producer {
	return &Producer{
		client:     client,
		streamName: streamName,
	}
}

func (p *Producer) Append(record HStreamRecord) AppendResult {
	key := record.GetRecordKey()
	entry := buildAppendEntry(p.streamName, key, record)
	if entry.res.Err != nil {
		return entry.res
	}

	sendAppend(p.client, p.streamName, record.GetRecordKey(), []*appendEntry{entry})
	return entry.res
}

func (p *Producer) Stop() {

}

type ProducerOpt func(producer *BatchProducer)

type BatchProducer struct {
	client      *HStreamClient
	streamName  string
	enableBatch bool
	batchSize   int
	timeOut     time.Duration
	isClosed    bool
	appends     map[string]*appender

	stop chan struct{}
}

func newBatchProducer(client *HStreamClient, streamName string, opts ...ProducerOpt) (*BatchProducer, error) {
	batchProducer := &BatchProducer{
		streamName:  streamName,
		client:      client,
		enableBatch: false,
		batchSize:   1,
		timeOut:     DEFAULT_BATCHPRODUCER_FLUSH_TIMEOUT,
		appends:     make(map[string]*appender),
		isClosed:    false,
		stop:        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(batchProducer)
	}
	if batchProducer.batchSize <= 0 {
		util.Logger().Error("batch size must be greater than 0")
		return nil, errors.New("batch size must be greater than 0")
	}
	return batchProducer, nil
}

// EnableBatch enable batch append, batchSize must greater than 0
func EnableBatch(batchSize int) ProducerOpt {
	return func(batchProducer *BatchProducer) {
		if batchSize <= 0 {
			util.Logger().Error("batch size must be greater than 0")
			return
		}
		p := batchProducer
		p.enableBatch = true
		p.batchSize = batchSize
	}
}

// TimeOut set millisecond time-trigger for batch producer to flush data to server.
// If timeOut <= 0, which means never trigger by time out.
func TimeOut(timeOut int) ProducerOpt {
	return func(batchProducer *BatchProducer) {
		var trigger time.Duration
		if timeOut < 0 {
			trigger = math.MaxUint32 * time.Second
		} else {
			trigger = time.Duration(timeOut) * time.Millisecond
		}
		batchProducer.timeOut = trigger
	}
}

func (p *BatchProducer) Stop() {
	util.Logger().Info("Stop BatchProducer", zap.String("streamName", p.streamName))
	p.isClosed = true
	close(p.stop)
	for _, appender := range p.appends {
		appender.Close()
	}
}

func (p *BatchProducer) Append(record HStreamRecord) AppendResult {
	key := record.GetRecordKey()
	entry := buildAppendEntry(p.streamName, key, record)
	if entry.res.Err != nil {
		return entry.res
	}

	appenderId := record.GetRecordType().String() + "-" + key
	if appender, ok := p.appends[appenderId]; ok {
		appender.dataCh <- entry
		return entry.res
	}

	appender := newAppender(p.client, p.streamName, key, p.batchSize, p.timeOut, p.stop)
	go appender.batchAppendLoop()
	p.appends[appenderId] = appender
	appender.dataCh <- entry
	return entry.res
}

type appender struct {
	client         *HStreamClient
	targetStream   string
	targetKey      string
	timeOut        time.Duration
	batchSize      int
	buffer         []*appendEntry
	lastSendServer string
	// isClosed == 1 means closed
	isClosed int32

	dataCh chan *appendEntry
	stop   chan struct{}
}

func newAppender(client *HStreamClient, stream, key string, size int, timeout time.Duration, stopCh chan struct{}) *appender {
	return &appender{
		client:       client,
		targetStream: stream,
		targetKey:    key,
		timeOut:      timeout,
		batchSize:    size,
		buffer:       make([]*appendEntry, 0, size),
		dataCh:       make(chan *appendEntry, size),
		stop:         stopCh,
		isClosed:     0,
	}
}

func (a *appender) Close() {
	if atomic.LoadInt32(&a.isClosed) == 1 {
		return
	}
	a.resetBuffer()
	atomic.StoreInt32(&a.isClosed, 1)
	close(a.dataCh)
}

func (a *appender) fetchBatchData() []*appendEntry {
	timer := time.NewTimer(a.timeOut)
	defer func() {
		timer.Stop()
	}()
	a.resetBuffer()

	for {
		select {
		case record := <-a.dataCh:
			a.buffer = append(a.buffer, record)
			if len(a.buffer) >= a.batchSize {
				timer.Stop()
				res := make([]*appendEntry, a.batchSize)
				copy(res, a.buffer)
				return res
			}
		case <-timer.C:
			util.Logger().Debug("Timeout!!!!!!!", zap.String("length of buffer", strconv.Itoa(len(a.buffer))))
			size := len(a.buffer)
			if size == 0 {
				return nil
			}
			res := make([]*appendEntry, size)
			copy(res, a.buffer)
			return res
		case <-a.stop:
			return nil
		}
	}
}

func (a *appender) batchAppendLoop() {
	for {
		if atomic.LoadInt32(&a.isClosed) == 1 {
			return
		}

		records := a.fetchBatchData()
		if len(records) == 0 {
			continue
		}
		sendAppend(a.client, a.targetStream, a.targetKey, records)
	}
}

func sendAppend(hsClient *HStreamClient, targetStream, targetKey string, records []*appendEntry) {
	reqRecords := make([]*hstreampb.HStreamRecord, 0, len(records))
	for _, record := range records {
		reqRecords = append(reqRecords, record.value)
	}
	req := &hstreamrpc.Request{
		Type: hstreamrpc.Append,
		Req: &hstreampb.AppendRequest{
			StreamName: targetStream,
			Records:    reqRecords,
		},
	}

	// FIXME: add cache to avoid lookup zk every time
	server, err := hsClient.LookUpStream(targetStream, targetKey)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	res, err := hsClient.SendRequest(ctx, server, req)
	defer cancel()

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

func (a *appender) resetBuffer() {
	for i := 0; i < len(a.buffer); i += 1 {
		a.buffer[i] = nil
	}
	a.buffer = a.buffer[:0]
}

func buildAppendEntry(streamName, key string, record HStreamRecord) *appendEntry {
	res := newRPCAppendRes()
	pbRecord, err := record.ToPbHStreamRecord()
	if err != nil {
		res.Err = err
	}
	entry := &appendEntry{
		streamName: streamName,
		key:        key,
		value:      pbRecord,
		res:        res,
	}
	return entry
}

func handleBatchAppendError(err error, records []*appendEntry) {
	for _, record := range records {
		record.res.SetError(err)
	}
}