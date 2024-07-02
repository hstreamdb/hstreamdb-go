package integraion

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/hstreamdb/hstreamdb-go/hstream/compression"
	"github.com/hstreamdb/hstreamdb-go/util/test_util"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	readWriteTestPrifx = "test_read_write_"
)

func TestReadWrite(t *testing.T) {
	suite.Run(t, new(readWriteSuite))
}

type readWriteSuite struct {
	suite.Suite
	streamName string
}

func (r *readWriteSuite) SetupTest() {
	streamName := readWriteTestPrifx + "stream_" + uuid.New().String()
	err := client.CreateStream(streamName, hstream.WithShardCount(5))
	r.NoError(err)
	r.streamName = streamName
}

func (r *readWriteSuite) TearDownTest() {
	client.DeleteStream(r.streamName, hstream.EnableForceDelete)
}

func (r *readWriteSuite) TestAppendHRecord() {
	testProducer(r.T(), r.streamName, hRecordTp, 100)
}

func (r *readWriteSuite) TestAppendRawRecord() {
	testProducer(r.T(), r.streamName, rawRecordTp, 100)
}

func (r *readWriteSuite) TestBatchAppendHRecord() {
	testBatchProducer(r.T(), r.streamName, hRecordTp, 20, 10000, hstream.WithBatch(200, 4096))
}

func (r *readWriteSuite) TestBatchAppendRawRecord() {
	testBatchProducer(r.T(), r.streamName, rawRecordTp, 20, 10000, hstream.WithBatch(200, 4096))
}

func (r *readWriteSuite) TestBatchAppendWithTimeout() {
	testBatchProducer(r.T(), r.streamName, rawRecordTp, 20, 10000, hstream.TimeOut(100))
}

func (r *readWriteSuite) TestBatchAppendWithGzip() {
	testBatchProducer(r.T(), r.streamName, rawRecordTp, 20, 10000,
		hstream.WithBatch(200, 4096), hstream.WithCompression(compression.Gzip))
}

func (r *readWriteSuite) TestBatchAppendWithZstd() {
	testBatchProducer(r.T(), r.streamName, rawRecordTp, 20, 10000,
		hstream.WithBatch(200, 4096), hstream.WithCompression(compression.Zstd))
}

func (r *readWriteSuite) TestConsumeFromLatest() {
	producer, err := client.NewProducer(r.streamName)
	require.NoError(r.T(), err)
	n := 10
	payloads := generateRawRecord(n)
	produce(r.T(), producer, payloads)

	subId := readWriteTestPrifx + "sub_" + uuid.New().String()
	err = client.CreateSubscription(subId, r.streamName)
	require.NoError(r.T(), err)
	defer client.DeleteSubscription(subId, true)

	afterRids := produce(r.T(), producer, payloads)
	consumedRids := consumeRecords(r.T(), subId, n)
	require.Equal(r.T(), afterRids, consumedRids)
}

type payloadType int8

const (
	hRecordTp payloadType = iota
	rawRecordTp
)

func testProducer(t *testing.T, streamName string, tp payloadType, payloadSize int) {
	producer, err := client.NewProducer(streamName)
	require.NoError(t, err)

	var payloads []Record.HStreamRecord
	switch tp {
	case hRecordTp:
		payloads = generateHRecord(payloadSize)
	case rawRecordTp:
		payloads = generateRawRecord(payloadSize)
	}
	verifyProducer(t, producer, streamName, payloads)
}

func testBatchProducer(t *testing.T, streamName string, tp payloadType, keySize, payloadSize int, opts ...hstream.ProducerOpt) {
	producer, err := client.NewBatchProducer(streamName, opts...)
	require.NoError(t, err)
	defer producer.Stop()

	var payloads map[string][]Record.HStreamRecord
	switch tp {
	case hRecordTp:
		payloads = test_util.GenerateBatchHRecord(keySize, payloadSize)
	case rawRecordTp:
		payloads = test_util.GenerateBatchRawRecord(keySize, payloadSize)
	}
	verifyBatchProducer(t, producer, streamName, payloads)
}

func verifyProducer(t *testing.T, producer *hstream.Producer, streamName string, payloads []Record.HStreamRecord) {
	subId := readWriteTestPrifx + "sub_" + uuid.New().String()
	err := client.CreateSubscription(subId, streamName, hstream.WithOffset(hstream.EARLIEST))
	require.NoError(t, err)
	defer client.DeleteSubscription(subId, true)

	rids := produce(t, producer, payloads)
	t.Logf("produce done")
	fetchRes := consumeRecords(t, subId, len(payloads))
	require.Equal(t, rids, fetchRes)
}

func produce(t *testing.T, producer *hstream.Producer, payloads []Record.HStreamRecord) []string {
	recordSize := len(payloads)
	appRes := make([]hstream.AppendResult, 0, recordSize)

	for i := 0; i < recordSize; i++ {
		res := producer.Append(payloads[i])
		appRes = append(appRes, res)
	}

	rids := make([]string, 0, recordSize)
	for _, res := range appRes {
		resp, err := res.Ready()
		require.NoError(t, err)
		rids = append(rids, resp.String())
	}
	return rids
}

func verifyBatchProducer(t *testing.T, producer *hstream.BatchProducer, streamName string, payloads map[string][]Record.HStreamRecord) {
	subId := readWriteTestPrifx + "sub_" + uuid.New().String()
	err := client.CreateSubscription(subId, streamName, hstream.WithOffset(hstream.EARLIEST), hstream.WithAckTimeout(5))
	require.NoError(t, err)
	defer client.DeleteSubscription(subId, true)

	wg := sync.WaitGroup{}
	wg.Add(len(payloads))
	ridMp := sync.Map{}

	for k, v := range payloads {
		go func(key string, records []Record.HStreamRecord) {
			defer wg.Done()
			recordSize := len(records)
			result := make([]hstream.AppendResult, 0, recordSize)
			for i := 0; i < recordSize; i++ {
				r := producer.Append(records[i])
				result = append(result, r)
			}
			ridMp.Store(key, result)
		}(k, v)
	}

	wg.Wait()
	t.Logf("produce done")

	rids := []string{}
	ridMp.Range(func(key, value interface{}) bool {
		appRes := value.([]hstream.AppendResult)
		for _, res := range appRes {
			resp, err := res.Ready()
			require.NoError(t, err)
			rids = append(rids, resp.String())
		}
		return true
	})
	t.Logf("check done")
	t.Logf("length of rids %d", len(rids))

	fetchRes := consumeRecords(t, subId, len(rids))
	sort.Strings(rids)
	sort.Strings(fetchRes)
	require.Equal(t, rids, fetchRes)
}

func consumeRecords(t *testing.T, subId string, recordSize int) []string {
	consumerName := readWriteTestPrifx + "consumer_" + uuid.New().String()
	consumer := client.NewConsumer(consumerName, subId)
	defer consumer.Stop()

	dataCh := consumer.StartFetch()
	fetchRes := make([]string, 0, recordSize)
	timer := time.NewTimer(25 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return fetchRes
		case res, ok := <-dataCh:
			if !ok {
				t.Logf("dataCh closed")
				return fetchRes
			}
			require.NoError(t, res.Err)
			for _, record := range res.Result {
				rid := record.GetRecordId()
				fetchRes = append(fetchRes, rid.String())
				record.Ack()
			}
			// t.Logf("total fetched: %d", len(fetchRes))
			if len(fetchRes) == recordSize {
				return fetchRes
			}
		}
	}
}

func generateHRecord(recordSize int) []Record.HStreamRecord {
	return test_util.GenerateHRecordWithKey("key-1", recordSize)
}

func generateRawRecord(recordSize int) []Record.HStreamRecord {
	return test_util.GenerateRawRecordWithKey("key-1", recordSize)
}
