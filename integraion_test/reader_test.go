package integraion

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

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
	err := client.CreateStream(streamName)
	s.NoError(err)
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
	s.NoError(err)

	totalRecords := 100
	producer, err := client.NewProducer(s.streamName)
	s.NoError(err)
	writeRecords := make([]Record.RecordId, 0, totalRecords)
	rawRecord, _ := Record.NewHStreamRawRecord("key-1", []byte("value-1"))
	for i := 0; i < totalRecords; i++ {
		r := producer.Append(rawRecord)
		rid, err := r.Ready()
		s.NoError(err)
		writeRecords = append(writeRecords, rid)
	}

	idx := rand.Intn(totalRecords)

	readerId := "reader_" + strconv.Itoa(rand.Int())
	reader, err := client.NewShardReader(s.streamName, readerId, shards[0].ShardId,
		hstream.WithShardOffset(hstream.NewRecordOffset(writeRecords[idx])),
		hstream.WithReaderTimeout(100),
		hstream.WithMaxRecords(10))
	s.NoError(err)
	defer client.DeleteShardReader(shards[0].ShardId, readerId)
	defer reader.Close()

	readRecords := make([]Record.ReceivedRecord, 0, totalRecords)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	for {
		res, err := reader.Read(ctx)
		s.NoError(err)
		readRecords = append(readRecords, res...)
		if len(readRecords) >= totalRecords-idx {
			break
		}
	}

	s.Equal(totalRecords-idx, len(readRecords))
}

func (s *testShardReaderSuite) readFromSpecialOffset(offset hstream.ShardOffset) {
	shards, err := client.ListShards(s.streamName)
	s.NoError(err)

	readerId := "reader_" + strconv.Itoa(rand.Int())
	reader, err := client.NewShardReader(s.streamName, readerId, shards[0].ShardId,
		hstream.WithShardOffset(offset), hstream.WithReaderTimeout(100), hstream.WithMaxRecords(10))
	s.NoError(err)
	defer client.DeleteShardReader(shards[0].ShardId, readerId)
	defer reader.Close()

	totalRecords := 100
	producer, err := client.NewProducer(s.streamName)
	s.NoError(err)
	writeRecords := make([]Record.RecordId, 0, totalRecords)
	rawRecord, _ := Record.NewHStreamRawRecord("key-1", []byte("value-1"))
	for i := 0; i < 100; i++ {
		r := producer.Append(rawRecord)
		rid, err := r.Ready()
		s.NoError(err)
		writeRecords = append(writeRecords, rid)
	}

	readRecords := make([]Record.ReceivedRecord, 0, totalRecords)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		res, err := reader.Read(ctx)
		s.NoError(err)
		readRecords = append(readRecords, res...)
		if len(readRecords) >= totalRecords {
			break
		}
	}
	s.Equal(totalRecords, len(readRecords))
}
