package hstream

import (
	"crypto/md5"
	"fmt"
	"github.com/hstreamdb/hstreamdb-go/hstream/compression"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	DefaultBatchProducerFlushTimeout = 100 * time.Millisecond
	DefaultMaxBatchRecordsSize       = 1024 * 1024 // 1MB
	DefaultBatchRecordsCount         = 100
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
	Ready() (Record.RecordId, error)
}

type rpcAppendRes struct {
	ready chan struct{}
	resp  Record.RecordId
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

func (r *rpcAppendRes) Ready() (Record.RecordId, error) {
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

type shardInfoCache struct {
	sync.RWMutex
	shardMap   *ShardMap
	serverInfo map[uint64]string
}

func newShardInfoCache(shards []Shard) *shardInfoCache {
	mp := NewShardMap(DEFAULT_SHARDMAP_DEGREE)
	for i := 0; i < len(shards); i += 1 {
		mp.ReplaceOrInsert(&shards[i])
	}
	return &shardInfoCache{
		shardMap:   mp,
		serverInfo: make(map[uint64]string, len(shards)),
	}
}

func (c *shardInfoCache) getServerInfo(client *HStreamClient, partitionKey string) (string, uint64, error) {
	hashKey := calculateShardRangeKey(partitionKey)
	c.RLock()
	shard := c.shardMap.FindLessOrEqual(hashKey)
	if shard == nil {
		c.RUnlock()
		return "", 0, errors.New(fmt.Sprintf("Can't find shard for hashKey %s", hashKey))
	}
	info, ok := c.serverInfo[shard.ShardId]
	if ok {
		c.RUnlock()
		return info, shard.ShardId, nil
	}
	c.RUnlock()

	// cache miss, send LookupShard RPC to server
	newInfo, err := client.LookupShard(shard.ShardId)
	if err != nil {
		return "", 0, err
	}
	c.Lock()
	c.serverInfo[shard.ShardId] = newInfo
	c.Unlock()
	return newInfo, shard.ShardId, nil
}

func (c *shardInfoCache) clear() {
	c.Lock()
	c.shardMap.Clear()
	c.Unlock()
}

// Producer produce a single piece of data to the specified stream.
type Producer struct {
	client     *HStreamClient
	streamName string
	cache      *shardInfoCache
}

func newProducer(client *HStreamClient, streamName string) (*Producer, error) {
	shards, err := client.ListShards(streamName)
	if err != nil {
		return nil, err
	}
	util.Logger().Debug("list shards", zap.String("shards", fmt.Sprintf("%+v", shards)))
	cache := newShardInfoCache(shards)
	return &Producer{
		client:     client,
		streamName: streamName,
		cache:      cache,
	}, nil
}

// Append will write a single record to the specified stream. This is a synchronous method.
func (p *Producer) Append(record Record.HStreamRecord) AppendResult {
	key := record.GetKey()
	entry := buildAppendEntry(p.streamName, key, record)
	if entry.res.err != nil {
		return entry.res
	}

	p.sendAppend(p.streamName, record.GetKey(), []appendEntry{entry})
	return entry.res
}

func (p *Producer) sendAppend(targetStream, targetKey string, records []appendEntry) {
	server, shardId, err := p.cache.getServerInfo(p.client, targetKey)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}

	noneCompressor := compression.NewNoneCompressor()
	payloads, err := encodeBatchRecord(records, noneCompressor)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}
	req := createAppendReq(payloads, targetStream, shardId)

	res, err := p.client.sendRequest(server, req)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}
	setAppendResponse(res, records)
}

func (p *Producer) Stop() {
	p.cache.clear()
	p.cache = nil
}

// ProducerOpt is the option for the BatchProducer.
type ProducerOpt func(producer *BatchProducer)

// BatchProducer is a producer that can batch write multiple records to the specified stream.
type BatchProducer struct {
	client          *HStreamClient
	streamName      string
	batchSize       int
	maxBatchBytes   uint64
	timeOut         time.Duration
	isClosed        uint32
	appends         map[uint64]*appender
	shardMap        *ShardMap
	compressionType compression.CompressionType

	controller *flowController

	stop chan struct{}
	lock sync.RWMutex
}

func newBatchProducer(client *HStreamClient, streamName string, opts ...ProducerOpt) (*BatchProducer, error) {
	batchProducer := &BatchProducer{
		streamName:      streamName,
		client:          client,
		batchSize:       DefaultBatchRecordsCount,
		maxBatchBytes:   DefaultMaxBatchRecordsSize,
		timeOut:         DefaultBatchProducerFlushTimeout,
		appends:         make(map[uint64]*appender),
		isClosed:        0,
		compressionType: compression.None,
		stop:            make(chan struct{}),
		controller:      nil,
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

	shards, err := client.ListShards(streamName)
	if err != nil {
		return nil, err
	}
	mp := NewShardMap(DEFAULT_SHARDMAP_DEGREE)
	for i := 0; i < len(shards); i += 1 {
		mp.ReplaceOrInsert(&shards[i])
	}
	batchProducer.shardMap = mp
	util.Logger().Debug(fmt.Sprintf("shardMap: %+v", batchProducer.shardMap.Ascend()))

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

// WithCompression set the compression algorithm
func WithCompression(compressionType compression.CompressionType) ProducerOpt {
	return func(batchProducer *BatchProducer) {
		batchProducer.compressionType = compressionType
	}
}

// Stop will stop the BatchProducer.
func (p *BatchProducer) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if !atomic.CompareAndSwapUint32(&p.isClosed, 0, 1) {
		return
	}
	close(p.stop)
	// wait appender flush
	for _, appender := range p.appends {
		<-appender.waitFlushCh
	}
}

// Append will write batch records to the specified stream. This is an asynchronous method.
// The backend goroutines are responsible for collecting the batch records and sending the
// data to the server when the trigger conditions are met.
func (p *BatchProducer) Append(record Record.HStreamRecord) AppendResult {
	key := record.GetKey()
	entry := buildAppendEntry(p.streamName, key, record)
	if entry.res.err != nil {
		return entry.res
	}

	// records are collected by shard.
	hashKey := calculateShardRangeKey(key)
	p.lock.RLock()
	shard := p.shardMap.FindLessOrEqual(hashKey)
	if shard == nil {
		p.lock.RUnlock()
		entry.res.err = errors.New(fmt.Sprintf("Can't find shard for hashKey %s", hashKey))
		return entry.res
	}

	if appender, ok := p.appends[shard.ShardId]; ok {
		p.lock.RUnlock()
		appender.dataCh <- entry
		return entry.res
	}
	p.lock.RUnlock()

	p.lock.Lock()
	if appender, ok := p.appends[shard.ShardId]; ok {
		p.lock.Unlock()
		appender.dataCh <- entry
		return entry.res
	}

	appender := newAppender(p, shard)
	p.appends[shard.ShardId] = appender
	go appender.batchAppendLoop()
	p.lock.Unlock()

	appender.dataCh <- entry
	return entry.res
}

type appender struct {
	client            *HStreamClient
	targetStream      string
	targetShard       *Shard
	timeOut           time.Duration
	batchSize         int
	maxRecordSize     uint64
	buffer            []appendEntry
	totalPayloadBytes uint64
	lastSendServer    string
	// isClosed == 1 means closed
	isClosed int32

	compressor compression.Compressor

	controller *flowController

	dataCh      chan appendEntry
	stop        chan struct{}
	waitFlushCh chan struct{}
}

func newAppender(p *BatchProducer, shard *Shard) *appender {
	var compressor compression.Compressor
	switch p.compressionType {
	case compression.None:
		compressor = compression.NewNoneCompressor()
	case compression.Gzip:
		compressor = compression.NewGzipCompressor()
	case compression.Zstd:
		compressor = compression.NewZstdCompressor()
	}

	return &appender{
		client:        p.client,
		targetStream:  p.streamName,
		targetShard:   shard,
		timeOut:       p.timeOut,
		batchSize:     p.batchSize,
		maxRecordSize: p.maxBatchBytes,
		buffer:        make([]appendEntry, 0, p.batchSize),
		dataCh:        make(chan appendEntry, p.batchSize),
		stop:          p.stop,
		waitFlushCh:   make(chan struct{}),
		isClosed:      0,
		compressor:    compressor,
		controller:    p.controller,
	}
}

func (a *appender) Close() {
	if !atomic.CompareAndSwapInt32(&a.isClosed, 0, 1) {
		return
	}
	close(a.dataCh)
}

func (a *appender) fetchBatchData() uint64 {
	// FIXME: consider reuse timer ???
	timer := time.NewTimer(a.timeOut)
	defer func() {
		timer.Stop()
	}()

	for {
		select {
		case record, ok := <-a.dataCh:
			if !ok {
				return a.totalPayloadBytes
			}
			a.buffer = append(a.buffer, record)
			a.totalPayloadBytes += uint64(len(record.value.Payload))
			if len(a.buffer) >= a.batchSize || a.totalPayloadBytes >= a.maxRecordSize {
				return a.totalPayloadBytes
			}
		case <-timer.C:
			size := len(a.buffer)
			util.Logger().Debug("batch appender timeout trigger", zap.Int("length of buffer", size))
			if size == 0 {
				return 0
			}
			return a.totalPayloadBytes
		}
	}
}

func (a *appender) batchAppendLoop() {
	util.Logger().Info("batchAppendLoop", zap.Uint64("shardId", a.targetShard.ShardId))
	breakFlag := false
	for !breakFlag {
		select {
		case <-a.stop:
			util.Logger().Info("appender receive stop signal",
				zap.String("target stream", a.targetShard.StreamName),
				zap.Uint64("target shardId", a.targetShard.ShardId))
			breakFlag = true
			a.Close()
		default:
			payloadSize := a.fetchBatchData()
			if payloadSize == 0 {
				continue
			}

			a.acquire(payloadSize)
			a.sendAppend(a.buffer, false)
			a.resetBuffer()
			a.release(payloadSize)
		}
	}

	a.flush()
}

func (a *appender) flush() {
	defer close(a.waitFlushCh)
	for record := range a.dataCh {
		a.buffer = append(a.buffer, record)
		a.totalPayloadBytes += uint64(len(record.value.Payload))
		if len(a.buffer) >= a.batchSize || a.totalPayloadBytes >= a.maxRecordSize {
			a.acquire(a.totalPayloadBytes)
			a.sendAppend(a.buffer, false)
			a.resetBuffer()
			a.release(a.totalPayloadBytes)
		}
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

func (a *appender) sendAppend(records []appendEntry, forceLookUp bool) {
	batchedRecords, err := encodeBatchRecord(records, a.compressor)
	if err != nil {
		handleBatchAppendError(err, records)
		return
	}
	req := createAppendReq(batchedRecords, a.targetStream, a.targetShard.ShardId)

	var server string
	if !forceLookUp && len(a.lastSendServer) != 0 {
		server = a.lastSendServer
	} else {
		util.Logger().Debug("cache miss",
			zap.String("stream", a.targetStream),
			zap.Uint64("shardId", a.targetShard.ShardId),
			zap.String("lastServer", a.lastSendServer))
		if server, err = a.client.LookupShard(a.targetShard.ShardId); err != nil {
			handleBatchAppendError(err, records)
			return
		}
	}

	res, err := a.client.sendRequest(server, req)
	if err != nil {
		if !forceLookUp && status.Code(err) == codes.FailedPrecondition {
			util.Logger().Debug("cache miss because err", zap.String("stream", a.targetStream), zap.Uint64("shardId", a.targetShard.ShardId))
			a.sendAppend(records, true)
			return
		}
		handleBatchAppendError(err, records)
		return
	}
	a.lastSendServer = server

	setAppendResponse(res, records)
}

func createAppendReq(records *hstreampb.BatchedRecord, targetStream string, targetShard uint64) *hstreamrpc.Request {
	return &hstreamrpc.Request{
		Type: hstreamrpc.Append,
		Req: &hstreampb.AppendRequest{
			StreamName: targetStream,
			ShardId:    targetShard,
			Records:    records,
		},
	}
}

func setAppendResponse(res *hstreamrpc.Response, records []appendEntry) {
	size := len(records)
	rids := res.Resp.(*hstreampb.AppendResponse).GetRecordIds()
	for i := 0; i < size; i++ {
		records[i].res.setResponse(rids[i], nil)
	}
}

func (a *appender) resetBuffer() {
	a.buffer = a.buffer[:0]
	a.totalPayloadBytes = 0
}

func buildAppendEntry(streamName, key string, record Record.HStreamRecord) appendEntry {
	res := newRPCAppendRes()
	pbRecord, err := HStreamRecordToPb(record)
	if err != nil {
		res.err = err
	}
	entry := appendEntry{
		streamName: streamName,
		key:        key,
		value:      pbRecord,
		res:        res,
	}
	return entry
}

func handleBatchAppendError(err error, records []appendEntry) {
	for _, record := range records {
		record.res.setResponse(nil, err)
	}
}

// FIXME：mabey need to add object pool or cache
func calculateShardRangeKey(shardKey string) string {
	h := md5.Sum([]byte(shardKey))
	res := new(big.Int)
	res.SetBytes(h[:])
	return res.String()
}

func encodeBatchRecord(records []appendEntry, compressor compression.Compressor) (*hstreampb.BatchedRecord, error) {
	reqRecords := make([]*hstreampb.HStreamRecord, 0, len(records))
	for _, record := range records {
		reqRecords = append(reqRecords, record.value)
	}

	batchHStreamRecords := &hstreampb.BatchHStreamRecords{Records: reqRecords}
	bs, err := proto.Marshal(batchHStreamRecords)
	if err != nil {
		return nil, errors.WithMessage(err, "encode batchRecord error")
	}

	payload := make([]byte, 0, len(bs))
	payload = compressor.Compress(payload, bs)
	return &hstreampb.BatchedRecord{
		CompressionType: CompressionTypeToPb(compressor.GetAlgorithm()),
		PublishTime:     timestamppb.Now(),
		BatchSize:       uint32(len(records)),
		Payload:         payload,
	}, nil
}
