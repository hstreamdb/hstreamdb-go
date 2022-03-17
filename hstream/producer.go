package hstream

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/hstreamrpc"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamDB/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"strconv"
	"sync/atomic"
	"time"
)

const DEFAULT_BATCHPRODUCER_FLUSH_TIMEOUT = 100 * time.Millisecond

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

func (p *Producer) Append(tp client.RecordType, key string, data []byte) client.AppendResult {
	entry := buildAppendEntry(tp, p.streamName, key, data)
	if entry.res.Err != nil {
		return entry.res
	}

	sendAppend(p.client, p.streamName, key, []*appendEntry{entry})
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
	appends     map[string]*Appender

	stop chan struct{}
}

func newBatchProducer(client *HStreamClient, streamName string, opts ...ProducerOpt) (*BatchProducer, error) {
	batchProducer := &BatchProducer{
		streamName:  streamName,
		client:      client,
		enableBatch: false,
		batchSize:   1,
		timeOut:     DEFAULT_BATCHPRODUCER_FLUSH_TIMEOUT,
		appends:     make(map[string]*Appender),
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
			trigger = 1000000000 * time.Millisecond
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

func (p *BatchProducer) Append(tp client.RecordType, key string, data []byte) client.AppendResult {
	entry := buildAppendEntry(tp, p.streamName, key, data)
	if entry.res.Err != nil {
		return entry.res
	}

	appenderId := tp.String() + "-" + key
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

type Appender struct {
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

func newAppender(client *HStreamClient, stream, key string, size int, timeout time.Duration, stopCh chan struct{}) *Appender {
	return &Appender{
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

func (a *Appender) Close() {
	if atomic.LoadInt32(&a.isClosed) == 1 {
		return
	}
	a.resetBuffer()
	atomic.StoreInt32(&a.isClosed, 1)
	//close(a.stop)
}

func (a *Appender) fetchBatchData() []*appendEntry {
	timer := time.NewTimer(a.timeOut)
	defer timer.Stop()
	a.resetBuffer()

	for {
		select {
		case record := <-a.dataCh:
			a.buffer = append(a.buffer, record)
			if len(a.buffer) >= a.batchSize {
				if !timer.Stop() {
					<-timer.C
				}
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

func (a *Appender) batchAppendLoop() {
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
		reqRecords = append(reqRecords, record.value.ToPbRecord())
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

func (a *Appender) resetBuffer() {
	for i := 0; i < len(a.buffer); i += 1 {
		a.buffer[i] = nil
	}
	a.buffer = a.buffer[:0]
}

func buildAppendEntry(tp client.RecordType, streamName, key string, data []byte) *appendEntry {
	res := hstreamrpc.NewRPCAppendRes()
	var record client.HStreamRecord
	switch tp {
	case client.RAWRECORD:
		record = client.NewRawRecord(key, data)
	case client.HRECORD:
		record = client.NewHRecord(key, data)
	default:
		res.Err = errors.New("unsupported record type")
	}

	entry := &appendEntry{
		streamName: streamName,
		key:        key,
		value:      record,
		res:        res,
	}
	return entry
}

func handleBatchAppendError(err error, records []*appendEntry) {
	for _, record := range records {
		record.res.SetError(err)
	}
}
