package integraion

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
)

const (
	testStreamPrefix = "stream_test_"
	testSubPrefix    = "sub_test_"
	testReaderPrefix = "reader_test_"
)

func TestCreateAndDeleteStream(t *testing.T) {
	tests := map[string]struct {
		options       []hstream.StreamOpts
		shouldSuccess bool
	}{
		"default": {options: []hstream.StreamOpts{}, shouldSuccess: true},
		"full opts": {
			options: []hstream.StreamOpts{
				hstream.WithReplicationFactor(2),
				hstream.EnableBacklog(1000),
				hstream.WithShardCount(3),
			},
			shouldSuccess: true,
		},
		"invaild replication factor": {
			options: []hstream.StreamOpts{
				hstream.WithReplicationFactor(0),
			},
			shouldSuccess: false,
		},
		"invaild shard count": {
			options: []hstream.StreamOpts{
				hstream.WithShardCount(0),
			},
			shouldSuccess: false,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			// t.Parallel()
			streamName := testStreamPrefix + uuid.New().String()
			err := client.CreateStream(streamName, tc.options...)
			defer func() {
				if tc.shouldSuccess {
					err = client.DeleteStream(streamName, hstream.EnableForceDelete)
					require.NoError(t, err)
				}
			}()

			if tc.shouldSuccess {
				require.NoError(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}

func TestTrimStreams(t *testing.T) {
	streamName := testStreamPrefix + uuid.New().String()
	err := client.CreateStream(streamName, hstream.WithShardCount(5))
	require.NoError(t, err)
	defer func() {
		_ = client.DeleteStream(streamName, hstream.EnableForceDelete)
	}()

	stamp := time.Now().UnixMilli()
	err = client.TrimStream(streamName, hstream.EarliestOffset)
	require.NoError(t, err)
	err = client.TrimStream(streamName, hstream.NewTimestampOffset(stamp))
	require.NoError(t, err)
	err = client.TrimStream(streamName, hstream.LatestOffset)
	require.NoError(t, err)
}

func TestTrimShards(t *testing.T) {
	streamName := testStreamPrefix + uuid.New().String()
	shardCnt := 500
	err := client.CreateStream(streamName, hstream.WithShardCount(uint32(shardCnt)))
	require.NoError(t, err)
	defer func() {
		_ = client.DeleteStream(streamName, hstream.EnableForceDelete)
	}()

	producer, err := client.NewBatchProducer(streamName, hstream.WithBatch(10, 4096), hstream.TimeOut(-1))
	require.NoError(t, err)

	keys := 1500
	records := 30000
	rand.Seed(time.Now().UnixNano())
	appRes := make([]hstream.AppendResult, 0, records)
	trimPointsIndex := make(map[string]int, keys)

	for i := 0; i < records; i++ {
		key := fmt.Sprintf("key_%d", rand.Intn(keys))
		if _, ok := trimPointsIndex[key]; !ok && i%90 == 0 {
			trimPointsIndex[key] = i
		}
		payload := map[string]interface{}{
			"key":   key,
			"value": i,
		}
		hRecord, _ := Record.NewHStreamHRecord(key, payload)
		appRes = append(appRes, producer.Append(hRecord))
	}
	producer.Stop()

	shardRecords := make(map[uint64][]Record.RecordId, shardCnt)
	rids := make([]Record.RecordId, 0, records)
	for _, res := range appRes {
		resp, err := res.Ready()
		require.NoError(t, err)
		shardRecords[resp.ShardId] = append(shardRecords[resp.ShardId], resp)
		rids = append(rids, resp)
	}

	trimPoints := make([]string, 0, keys)
	expectedTrimRecord := make(map[uint64]Record.RecordId, shardCnt)
	for _, value := range trimPointsIndex {
		rid := rids[value]
		trimPoints = append(trimPoints, rid.String())
		if _, ok := expectedTrimRecord[rid.ShardId]; !ok {
			expectedTrimRecord[rid.ShardId] = rid
		} else if Record.CompareRecordId(rid, expectedTrimRecord[rid.ShardId]) < 0 {
			expectedTrimRecord[rid.ShardId] = rid
		}
	}

	expectedTrimPoints := make(map[uint64]string, shardCnt)
	for shardId, rid := range expectedTrimRecord {
		expectedTrimPoints[shardId] = rid.String()
	}

	t.Log("===== trim points =====")
	for _, p := range trimPoints {
		t.Log(p)
	}

	t.Log("===== expected trim points =====")
	for _, p := range expectedTrimPoints {
		t.Log(p)
	}

	start := time.Now()

	for shardId := range expectedTrimPoints {
		earliestPosition := Record.RecordId{ShardId: shardId}
		trimPoints = append(trimPoints, earliestPosition.String())
	}

	res, err := client.TrimShards(streamName, trimPoints)
	t.Logf("trim cost %d ms", time.Since(start).Milliseconds())
	t.Log("===== trim results =====")
	for _, p := range res {
		t.Log(p)
	}
	require.NoError(t, err)
	require.Equal(t, len(expectedTrimPoints), len(res))
	require.EqualValues(t, expectedTrimPoints, res)
}

func TestListStreams(t *testing.T) {
	streams := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		streamName := testStreamPrefix + uuid.New().String()
		err := client.CreateStream(streamName)
		require.NoError(t, err)
		streams = append(streams, streamName)
	}
	defer func() {
		for _, streamName := range streams {
			_ = client.DeleteStream(streamName, hstream.EnableForceDelete)
		}
	}()

	res, err := client.ListStreams()
	require.Equal(t, len(streams), len(res), "the count of listed streams doesn't match the count of created")
	require.NoError(t, err)

	for _, stream := range res {
		require.Contains(t, streams, stream.StreamName)
	}
}

func TestCreateAndDeleteSub(t *testing.T) {
	tests := map[string]struct {
		options       []hstream.SubscriptionOpts
		shouldSuccess bool
	}{
		"default": {options: []hstream.SubscriptionOpts{}, shouldSuccess: true},
		"full opts": {
			options: []hstream.SubscriptionOpts{
				hstream.WithAckTimeout(100),
				hstream.WithMaxUnackedRecords(1000),
				hstream.WithOffset(hstream.EARLIEST),
			},
			shouldSuccess: true,
		},
		"invaild ack timeout": {
			options: []hstream.SubscriptionOpts{
				hstream.WithAckTimeout(0),
			},
			shouldSuccess: false,
		},
		"invaild unacked records": {
			options: []hstream.SubscriptionOpts{
				hstream.WithMaxUnackedRecords(0),
			},
			shouldSuccess: false,
		},
	}

	streamName := testStreamPrefix + "sub_test_stream"
	err := client.CreateStream(streamName)
	require.NoError(t, err)
	defer client.DeleteStream(streamName, hstream.EnableForceDelete)

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			subId := testSubPrefix + uuid.New().String()
			err := client.CreateSubscription(subId, streamName, tc.options...)
			defer func() {
				if tc.shouldSuccess {
					err = client.DeleteSubscription(subId, true)
					require.NoError(t, err)
				}
			}()

			if tc.shouldSuccess {
				require.NoError(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}

func TestListSubscriptions(t *testing.T) {
	streamName := testStreamPrefix + uuid.New().String()
	err := client.CreateStream(streamName)
	require.NoError(t, err)
	defer client.DeleteStream(streamName, hstream.EnableForceDelete)

	subCnt := 5
	subs := make([]string, 0, subCnt)
	for i := 0; i < subCnt; i++ {
		subId := testSubPrefix + uuid.New().String()
		err = client.CreateSubscription(subId, streamName)
		require.NoError(t, err)
		subs = append(subs, subId)
	}
	defer func() {
		for _, subId := range subs {
			_ = client.DeleteSubscription(subId, true)
		}
	}()

	res, err := client.ListSubscriptions()
	require.NoError(t, err)

	for _, sub := range res {
		require.Contains(t, subs, sub.SubscriptionId)
	}
}

func TestCreateAndDeleteReader(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	streamName := testStreamPrefix + uuid.New().String()
	err := client.CreateStream(streamName)
	require.NoError(t, err)
	defer client.DeleteStream(streamName, hstream.EnableForceDelete)

	producer, err := client.NewProducer(streamName)
	require.NoError(t, err)
	defer producer.Stop()
	writeRecords := make([]Record.RecordId, 0, 3)
	rawRecord, _ := Record.NewHStreamRawRecord("key-1", []byte("value-1"))
	for i := 0; i < 3; i++ {
		r := producer.Append(rawRecord)
		rid, err := r.Ready()
		require.NoError(t, err)
		writeRecords = append(writeRecords, rid)
	}

	shards, err := client.ListShards(streamName)
	require.NoError(t, err)
	require.Equal(t, len(shards), 1)

	shardId := shards[0].ShardId
	rid := writeRecords[2]
	invailedRecordId := Record.RecordId{
		BatchId:    rid.BatchId,
		BatchIndex: rid.BatchIndex + 1,
		ShardId:    rid.ShardId,
	}

	tests := map[string]struct {
		options       []hstream.ShardReaderOpts
		shouldSuccess bool
	}{
		"default": {options: []hstream.ShardReaderOpts{}, shouldSuccess: true},
		"full opts": {
			options: []hstream.ShardReaderOpts{
				hstream.WithReaderTimeout(100),
				hstream.WithMaxRecords(30),
			},
			shouldSuccess: true,
		},
		"reader from Latest": {
			options: []hstream.ShardReaderOpts{
				hstream.WithShardOffset(hstream.LatestOffset),
			},
			shouldSuccess: true,
		},
		"reader from rid": {
			options: []hstream.ShardReaderOpts{
				hstream.WithShardOffset(hstream.NewRecordOffset(writeRecords[1])),
			},
			shouldSuccess: true,
		},
		"reader from invailedRid": {
			options: []hstream.ShardReaderOpts{
				hstream.WithShardOffset(hstream.NewRecordOffset(invailedRecordId)),
			},
			shouldSuccess: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			readerId := testReaderPrefix + uuid.New().String()
			reader, err := client.NewShardReader(streamName, readerId, shardId, tc.options...)
			defer func() {
				if tc.shouldSuccess {
					err = client.DeleteShardReader(shardId, readerId)
					require.NoError(t, err)
				}
			}()

			if tc.shouldSuccess {
				require.NoError(t, err)
				reader.Close()
			} else {
				require.NotNil(t, err)
			}
		})
	}
}
