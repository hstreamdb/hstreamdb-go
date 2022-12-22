package integraion

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/stretchr/testify/suite"
)

func TestShardReader(t *testing.T) {
	suite.Run(t, new(testShardReaderSuite))
}

type testShardReaderSuite struct {
	suite.Suite
	serverUrl  string
	client     *hstream.HStreamClient
	streamName string
}

func (s *testShardReaderSuite) SetupTest() {
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := client.CreateStream(streamName, hstream.WithShardCount(1))
	require.NoError(s.T(), err)
	s.streamName = streamName
}

func (s *testShardReaderSuite) TearDownTest() {
	client.DeleteStream(s.streamName, hstream.EnableForceDelete)
}

func (s *testShardReaderSuite) TestReadFromEarliest() {
	s.readFromSpecialOffset(hstream.EarliestShardOffset)
}

func (s *testShardReaderSuite) TestReadFromLatest() {
	s.readFromSpecialOffset(hstream.LatestShardOffset)
}

func (s *testShardReaderSuite) TestReadFromRecordId() {
	shards, err := client.ListShards(s.streamName)
	require.NoError(s.T(), err)

	totalRecords := 100
	producer, err := client.NewProducer(s.streamName)
	require.NoError(s.T(), err)
	writeRecords := make([]Record.RecordId, 0, totalRecords)
	rawRecord, _ := Record.NewHStreamRawRecord("key-1", []byte("value-1"))
	for i := 0; i < totalRecords; i++ {
		r := producer.Append(rawRecord)
		rid, err := r.Ready()
		require.NoError(s.T(), err)
		writeRecords = append(writeRecords, rid)
	}

	idx := rand.Intn(totalRecords)

	readerId := "reader_" + strconv.Itoa(rand.Int())
	reader, err := client.NewShardReader(s.streamName, readerId, shards[0].ShardId,
		hstream.WithShardOffset(hstream.NewRecordOffset(writeRecords[idx])),
		hstream.WithReaderTimeout(100),
		hstream.WithMaxRecords(10))
	require.NoError(s.T(), err)
	defer client.DeleteShardReader(shards[0].ShardId, readerId)
	defer reader.Close()

	readRecords := make([]Record.ReceivedRecord, 0, totalRecords)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for {
		res, err := reader.Read(ctx)
		for _, r := range res {
			s.T().Logf("res: %+v\n", r.GetRecordId())
		}
		require.NoError(s.T(), err)
		readRecords = append(readRecords, res...)
		if len(readRecords) >= totalRecords-idx {
			break
		}
	}

	s.Equal(totalRecords-idx, len(readRecords))
}

func (s *testShardReaderSuite) readFromSpecialOffset(offset hstream.ShardOffset) {
	shards, err := client.ListShards(s.streamName)
	s.T().Logf("shards: %+v", shards)
	require.NoError(s.T(), err)

	totalRecords := int32(100 * len(shards))
	count := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(len(shards))
	for idx, shard := range shards {
		go func(idx int, shard hstream.Shard) {
			defer wg.Done()
			readerId := "reader_" + strconv.Itoa(idx)
			reader, err := client.NewShardReader(s.streamName, readerId, shard.ShardId,
				hstream.WithShardOffset(offset), hstream.WithReaderTimeout(100), hstream.WithMaxRecords(10))
			require.NoError(s.T(), err)
			defer reader.Close()
			total := int32(0)
			defer atomic.AddInt32(&count, total)
			defer client.DeleteShardReader(shards[0].ShardId, readerId)
			defer reader.Close()

			producer, err := client.NewProducer(s.streamName)
			require.NoError(s.T(), err)
			writeRecords := make([]Record.RecordId, 0, totalRecords)
			for i := int32(0); i < totalRecords; i++ {
				rawRecord, _ := Record.NewHStreamRawRecord(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
				r := producer.Append(rawRecord)
				rid, err := r.Ready()
				require.NoError(s.T(), err)
				writeRecords = append(writeRecords, rid)
			}

			readRecords := make([]Record.ReceivedRecord, 0, totalRecords)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for {
				res, err := reader.Read(ctx)
				if err == context.DeadlineExceeded {
					break
				}
				require.NoError(s.T(), err)

				readRecords = append(readRecords, res...)
				s.T().Logf("read %d records from shard: %d", len(res), shard.ShardId)
				atomic.AddInt32(&count, int32(len(res)))
				total += int32(len(res))
			}
		}(idx, shard)
	}
	wg.Wait()

	s.T().Logf("total reads %d", atomic.LoadInt32(&count))
	s.Equal(totalRecords, atomic.LoadInt32(&count))
}
