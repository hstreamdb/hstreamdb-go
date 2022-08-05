package integraion

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/hstreamdb/hstreamdb-go/util/test_util"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestShardReader(t *testing.T) {
	suite.Run(t, new(testShardReaderSuite))
}

type testShardReaderSuite struct {
	suite.Suite
	serverUrl string
	client    *hstream.HStreamClient
}

func (s *testShardReaderSuite) SetupTest() {
	var err error
	s.serverUrl = test_util.ServerUrl
	s.client, err = hstream.NewHStreamClient(s.serverUrl)
	util.SetLogLevel(util.DEBUG)
	s.NoError(err)
}

func (s *testShardReaderSuite) TearDownTest() {
	s.client.Close()
}

func (s *testShardReaderSuite) TestReadFromEarliest() {
	s.readFromSpecialOffset(hstream.EarliestShardOffset)
}

func (s *testShardReaderSuite) TestReadFromLatest() {
	s.readFromSpecialOffset(hstream.LatestShardOffset)
}

func (s *testShardReaderSuite) TestReadFromRecordId() {
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)
	shards, err := s.client.ListShards(streamName)
	s.NoError(err)

	totalRecords := 100
	producer, err := s.client.NewProducer(streamName)
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
	reader, err := s.client.NewShardReader(streamName, readerId, shards[0].ShardId,
		hstream.WithShardOffset(hstream.NewRecordOffset(writeRecords[idx])), hstream.WithReaderTimeout(100),
		hstream.WithMaxRecords(10))
	s.NoError(err)
	defer reader.DeleteShardReader()
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
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)
	shards, err := s.client.ListShards(streamName)
	s.NoError(err)

	readerId := "reader_" + strconv.Itoa(rand.Int())
	reader, err := s.client.NewShardReader(streamName, readerId, shards[0].ShardId,
		hstream.WithShardOffset(offset), hstream.WithReaderTimeout(100), hstream.WithMaxRecords(10))
	s.NoError(err)
	defer reader.DeleteShardReader()
	defer reader.Close()

	totalRecords := 100
	producer, err := s.client.NewProducer(streamName)
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
