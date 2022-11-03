package hstream

import (
	"sync/atomic"
	"testing"
	"time"

	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util/test_util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	testStreamName = "test"
)

func TestAppendRetry(t *testing.T) {
	listShards := func(req proto.Message) (resp proto.Message, err error) {
		request := req.(*hstreampb.ListShardsRequest)
		return &hstreampb.ListShardsResponse{
			Shards: []*hstreampb.Shard{
				{StreamName: request.StreamName, ShardId: 1},
			},
		}, nil
	}

	counter := uint64(0)
	app := func(req proto.Message) (resp proto.Message, err error) {
		atomic.AddUint64(&counter, 1)
		return nil, status.Error(codes.Unavailable, "unavailable")
	}
	server.responseSetter = []hacker{listShards, app, app, app, app}
	producer, err := testClient.NewProducer(testStreamName)
	require.NoError(t, err)

	rawRecord := test_util.GenerateRawRecordWithKey("a", 1)
	producer.Append(rawRecord[0])
	require.Equal(t, counter, uint64(3))
}

func TestBatchAppendWithBatchSize(t *testing.T) {
	listShards := func(req proto.Message) (resp proto.Message, err error) {
		request := req.(*hstreampb.ListShardsRequest)
		return &hstreampb.ListShardsResponse{
			Shards: []*hstreampb.Shard{
				{StreamName: request.StreamName, ShardId: 1},
			},
		}, nil
	}

	batchStatic := []uint32{}
	app := func(req proto.Message) (resp proto.Message, err error) {
		request := req.(*hstreampb.AppendRequest)
		size := request.Records.BatchSize
		batchStatic = append(batchStatic, size)
		t.Logf("received append record, size: %d\n", size)
		return generateAppendResponse(size, request.StreamName, request.ShardId)
	}

	server.responseSetter = []hacker{listShards, app, app, app, app}
	producer, err := testClient.NewBatchProducer(testStreamName, WithBatch(25, 100000))
	require.NoError(t, err)
	defer producer.Stop()

	keySize := 1
	recordSize := 100
	appendRecords(t, producer, keySize, recordSize)

	require.Equal(t, []uint32{25, 25, 25, 25}, batchStatic)
}

func TestBatchAppendWithTimeout(t *testing.T) {
	listShards := func(req proto.Message) (resp proto.Message, err error) {
		request := req.(*hstreampb.ListShardsRequest)
		return &hstreampb.ListShardsResponse{
			Shards: []*hstreampb.Shard{
				{StreamName: request.StreamName, ShardId: 1},
			},
		}, nil
	}

	var lastAppend time.Time
	timeout := int64(2)
	latencies := []int64{}
	app := func(req proto.Message) (resp proto.Message, err error) {
		request := req.(*hstreampb.AppendRequest)
		size := request.Records.BatchSize
		now := time.Now()
		latencies = append(latencies, now.Sub(lastAppend).Milliseconds())
		lastAppend = now
		t.Logf("received append record, size: %d\n", size)
		return generateAppendResponse(size, request.StreamName, request.ShardId)
	}

	server.responseSetter = []hacker{listShards, app, app, app, app, app, app, app, app}
	producer, err := testClient.NewBatchProducer(testStreamName,
		WithBatch(100000, 100000000),
		TimeOut(int(timeout)))
	require.NoError(t, err)
	defer producer.Stop()

	keySize := 1
	recordSize := 10000
	lastAppend = time.Now()
	appendRecords(t, producer, keySize, recordSize)

	for _, latency := range latencies {
		require.True(t, latency > timeout)
	}
}

func generateAppendResponse(size uint32, streamName string, shardId uint64) (*hstreampb.AppendResponse, error) {
	recordIds := make([]*hstreampb.RecordId, 0, size)
	for i := uint32(0); i < size; i++ {
		recordIds = append(recordIds, &hstreampb.RecordId{
			ShardId:    shardId,
			BatchId:    1,
			BatchIndex: i,
		})
	}
	return &hstreampb.AppendResponse{
		StreamName: streamName,
		ShardId:    shardId,
		RecordIds:  recordIds,
	}, nil
}

func appendRecords(t *testing.T, producer *BatchProducer, keySize int, recordSize int) {
	rawRecords := test_util.GenerateBatchRawRecord(keySize, recordSize)
	res := []AppendResult{}
	for _, records := range rawRecords {
		t.Logf("total records %d", len(records))
		for _, record := range records {
			response := producer.Append(record)
			res = append(res, response)
		}
	}

	for _, r := range res {
		_, err := r.Ready()
		require.NoError(t, err)
	}
}
