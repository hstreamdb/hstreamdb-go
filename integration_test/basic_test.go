package integraion

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/stretchr/testify/require"
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
			shouldSuccess: false},
		"invaild shard count": {
			options: []hstream.StreamOpts{
				hstream.WithShardCount(0),
			},
			shouldSuccess: false},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			//t.Parallel()
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
			shouldSuccess: false},
		"invaild unacked records": {
			options: []hstream.SubscriptionOpts{
				hstream.WithMaxUnackedRecords(0),
			},
			shouldSuccess: false},
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
				hstream.WithShardOffset(hstream.LatestShardOffset),
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
