package hstream

import (
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	DEFAULT_BATCHPRODUCER_FLUSH_TIMEOUT = 10 * time.Second
	DEFAULT_MAX_BATCHRECORDS_SIZE       = 1024 * 1024 // 1MB
)

type appendEntry struct {
	key        string
	value      *hstreampb.HStreamRecord
	streamName string
	res        *rpcAppendRes
}

// AppendResult is a handler to process the results of append operation.
type AppendResult interface {
	// Ready will return when the append request return,
	// or an error if append fails.
	Ready() (RecordId, error)
}

type rpcAppendRes struct {
	ready chan struct{}
	resp  RecordId
	err   error
}

func newRPCAppendRes() *rpcAppendRes {
	return &rpcAppendRes{
		ready: make(chan struct{}),
	}
}

func (r *rpcAppendRes) String() string {
	if r.err != nil {
		return r.err.Error()
	}
	return r.resp.String()
}

func (r *rpcAppendRes) Ready() (RecordId, error) {
	<-r.ready
	return r.resp, r.err
}

// setResponse will set the response of the append request. also it
// will close the ready channel so that user can get result by calling
// Ready().
func (r *rpcAppendRes) setResponse(res interface{}, err error) {
	defer close(r.ready)
	if res != nil {
		r.resp = RecordIdFromPb(res.(*hstreampb.RecordId))
	}
	r.err = err
}

// Producer produce a single piece of data to the specified stream.
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

// Append will write a single record to the specified stream. This is a synchronous method.
func (p *Producer) Append(record *HStreamRecord) AppendResult {
	key := record.Key
	entry := buildAppendEntry(p.streamName, key, record)

	p.sendAppend(p.streamName, record.Key, []*appendEntry{entry})
	return entry.res
}

func (p *Producer) sendAppend(targetStream, targetKey string, records []*appendEntry) {
	req := createAppendReq(records, targetStream)
	server, err := p.client.LookUpStream(targetStream, targetKey)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}

	res, err := p.client.sendRequest(server, req)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}
	setAppendResponse(res, records)
}

func (p *Producer) Stop() {

}

// ProducerOpt is the option for the BatchProducer.
type ProducerOpt func(producer *BatchProducer)

// BatchProducer is a producer that can batch write multiple records to the specified stream.
type BatchProducer struct {
	client        *HStreamClient
	streamName    string
	batchSize     int
	maxBatchBytes uint64
	timeOut       time.Duration
	isClosed      bool
	appends       map[string]*appender

	controller *flowController

	stop chan struct{}
	lock sync.Mutex
}

func newBatchProducer(client *HStreamClient, streamName string, opts ...ProducerOpt) (*BatchProducer, error) {
	batchProducer := &BatchProducer{
		streamName:    streamName,
		client:        client,
		batchSize:     1,
		maxBatchBytes: DEFAULT_MAX_BATCHRECORDS_SIZE,
		timeOut:       DEFAULT_BATCHPRODUCER_FLUSH_TIMEOUT,
		appends:       make(map[string]*appender),
		isClosed:      false,
		stop:          make(chan struct{}),
		controller:    nil,
	}

	for _, opt := range opts {
		opt(batchProducer)
	}

	if batchProducer.batchSize <= 0 {
		return nil, errors.New("batch size must be greater than 0")
	}

	if batchProducer.controller != nil && batchProducer.controller.outStandingBytes < batchProducer.maxBatchBytes {
		return nil, errors.New(fmt.Sprintf(
			"maxBatchBytes(%d) must less than and equal to controller's outStandingBytes(%d)",
			batchProducer.maxBatchBytes, batchProducer.controller.outStandingBytes))
	}

	return batchProducer, nil
}

// WithBatch set the batchSize-trigger for BatchProducer, batchSize must greater than 0
func WithBatch(batchSize int, maxBytes uint64) ProducerOpt {
	return func(batchProducer *BatchProducer) {
		p := batchProducer
		p.batchSize = batchSize
		p.maxBatchBytes = maxBytes
	}
}

// TimeOut set millisecond time-trigger for BatchProducer to flush data to server.
// If timeOut <= 0, which means never trigger by time out.
func TimeOut(timeOut int) ProducerOpt {
	return func(batchProducer *BatchProducer) {
		var trigger time.Duration
		if timeOut <= 0 {
			trigger = math.MaxUint32 * time.Second
		} else {
			trigger = time.Duration(timeOut) * time.Millisecond
		}
		batchProducer.timeOut = trigger
	}
}

// WithFlowControl set the flow control for BatchProducer. The maxBytes parameter
// indicates the maximum number of bytes of data that have not been appended successfully,
// including all data that has been sent to the server and all data that has not been sent to the server.
// maxBytes == 0 will disable the flow control.
func WithFlowControl(maxBytes uint64) ProducerOpt {
	return func(batchProducer *BatchProducer) {
		if maxBytes > 0 {
			batchProducer.controller = newFlowController(maxBytes)
		}
	}
}

// Stop will stop the BatchProducer.
func (p *BatchProducer) Stop() {
	p.isClosed = true
	close(p.stop)
	for _, appender := range p.appends {
		appender.Close()
	}
}

// Append will write batch records to the specified stream. This is an asynchronous method.
// The backend goroutines are responsible for collecting the batch records and sending the
// data to the server when the trigger conditions are met.
func (p *BatchProducer) Append(record *HStreamRecord) AppendResult {
	key := record.Key
	entry := buildAppendEntry(p.streamName, key, record)

	// records are collected by key.
	appenderId := record.GetType().String() + "-" + key
	p.lock.Lock()
	appender, ok := p.appends[appenderId]
	if !ok {
		appender = newAppender(p.client, p.streamName, key, p.batchSize, p.maxBatchBytes, p.timeOut, p.stop, p.controller)
		p.appends[appenderId] = appender
		go appender.batchAppendLoop()
	}
	p.lock.Unlock()

	appender.dataCh <- entry
	return entry.res
}

type appender struct {
	client         *HStreamClient
	targetStream   string
	targetKey      string
	timeOut        time.Duration
	batchSize      int
	maxRecordSize  uint64
	buffer         []*appendEntry
	lastSendServer string
	// isClosed == 1 means closed
	isClosed int32

	controller *flowController

	dataCh chan *appendEntry
	stop   chan struct{}
}

func newAppender(client *HStreamClient, stream, key string, batchSize int, maxRecordSize uint64,
	timeout time.Duration, stopCh chan struct{}, controller *flowController) *appender {
	return &appender{
		client:        client,
		targetStream:  stream,
		targetKey:     key,
		timeOut:       timeout,
		batchSize:     batchSize,
		maxRecordSize: maxRecordSize,
		buffer:        make([]*appendEntry, 0, batchSize),
		dataCh:        make(chan *appendEntry, batchSize),
		stop:          stopCh,
		isClosed:      0,
		controller:    controller,
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

func (a *appender) fetchBatchData() ([]*appendEntry, uint64) {
	// FIXME: consider reuse timer ???
	timer := time.NewTimer(a.timeOut)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()
	a.resetBuffer()
	totalPayloadBytes := uint64(0)

	for {
		select {
		case record := <-a.dataCh:
			a.buffer = append(a.buffer, record)
			totalPayloadBytes += uint64(len(record.value.Payload))
			if len(a.buffer) >= a.batchSize || totalPayloadBytes >= a.maxRecordSize {
				size := util.Min(a.batchSize, len(a.buffer))
				res := make([]*appendEntry, size)
				copy(res, a.buffer)
				return res, totalPayloadBytes
			}
		case <-timer.C:
			util.Logger().Debug("Timeout!!!!!!!", zap.String("length of buffer", strconv.Itoa(len(a.buffer))))
			size := len(a.buffer)
			if size == 0 {
				return nil, 0
			}
			res := make([]*appendEntry, size)
			copy(res, a.buffer)
			return res, totalPayloadBytes
		case <-a.stop:
			return nil, 0
		}
	}
}

func (a *appender) batchAppendLoop() {
	for {
		if atomic.LoadInt32(&a.isClosed) == 1 {
			return
		}

		records, payloadSize := a.fetchBatchData()
		if len(records) == 0 {
			continue
		}

		a.acquire(payloadSize)
		a.sendAppend(records, payloadSize, false)
		a.release(payloadSize)
	}
}

func (a *appender) acquire(need uint64) {
	if a.controller == nil {
		return
	}

	a.controller.Acquire(need)
}

func (a *appender) release(size uint64) {
	if a.controller == nil {
		return
	}

	a.controller.Release(size)
}

func (a *appender) sendAppend(records []*appendEntry, payloadSize uint64, forceLookUp bool) {
	req := createAppendReq(records, a.targetStream)

	var server string
	var err error
	if !forceLookUp && len(a.lastSendServer) != 0 {
		server = a.lastSendServer
	} else {
		util.Logger().Debug("cache miss", zap.String("stream", a.targetStream), zap.String("key", a.targetKey), zap.String("lastServer", a.lastSendServer))
		if server, err = a.client.LookUpStream(a.targetStream, a.targetKey); err != nil {
			handleBatchAppendError(err, records)
			return
		}
	}

	res, err := a.client.sendRequest(server, req)
	if err != nil {
		if !forceLookUp && status.Code(err) == codes.FailedPrecondition {
			util.Logger().Debug("cache miss because err", zap.String("stream", a.targetStream), zap.String("key", a.targetKey))
			a.sendAppend(records, payloadSize, true)
			return
		}
		handleBatchAppendError(err, records)
		return
	}
	a.lastSendServer = server

	setAppendResponse(res, records)
}

func createAppendReq(records []*appendEntry, targetStream string) *hstreamrpc.Request {
	reqRecords := make([]*hstreampb.HStreamRecord, 0, len(records))
	for _, record := range records {
		reqRecords = append(reqRecords, record.value)
	}
	return &hstreamrpc.Request{
		Type: hstreamrpc.Append,
		Req: &hstreampb.AppendRequest{
			StreamName: targetStream,
			Records:    reqRecords,
		},
	}
}

func setAppendResponse(res *hstreamrpc.Response, records []*appendEntry) {
	size := len(records)
	rids := res.Resp.(*hstreampb.AppendResponse).GetRecordIds()
	for i := 0; i < size; i++ {
		records[i].res.setResponse(rids[i], nil)
	}
}

func (a *appender) resetBuffer() {
	for i := 0; i < len(a.buffer); i++ {
		a.buffer[i] = nil
	}
	a.buffer = a.buffer[:0]
}

func buildAppendEntry(streamName, key string, record *HStreamRecord) *appendEntry {
	res := newRPCAppendRes()
	pbRecord := HStreamRecordToPb(record)
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
		record.res.setResponse(nil, err)
	}
}
