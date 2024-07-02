package integraion

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
)

func TestReadStream(t *testing.T) {
	suite.Run(t, new(testReadStreamSuite))
}

type testReadStreamSuite struct {
	suite.Suite
	serverUrl  string
	client     *hstream.HStreamClient
	streamName string
}

func (s *testReadStreamSuite) SetupTest() {
	rand.Seed(time.Now().UnixMilli())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := client.CreateStream(streamName, hstream.WithShardCount(3))
	require.NoError(s.T(), err)
	s.streamName = streamName
}

func (s *testReadStreamSuite) TearDownTest() {
	client.DeleteStream(s.streamName, hstream.EnableForceDelete)
}

func (s *testReadStreamSuite) TestReadFromEarliest() {
	s.readFromSpecialOffset(hstream.EarliestOffset, hstream.EmptyOffset, 1000)
}

func (s *testReadStreamSuite) TestReadToLatest() {
	s.readFromSpecialOffset(hstream.EmptyOffset, hstream.LatestOffset, 0)
}

func (s *testReadStreamSuite) readFromSpecialOffset(from, to hstream.StreamOffset, total uint64) {
	totalRecords := 1000

	producer, err := client.NewProducer(s.streamName)
	require.NoError(s.T(), err)
	writeRecords := make([]Record.RecordId, 0, totalRecords)
	for i := 0; i < totalRecords; i++ {
		rawRecord, _ := Record.NewHStreamRawRecord(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
		r := producer.Append(rawRecord)
		rid, err := r.Ready()
		require.NoError(s.T(), err)
		writeRecords = append(writeRecords, rid)
	}

	stream, err := client.ReadStream(s.streamName, from, to, total)
	require.NoError(s.T(), err)

	readRecords := make([]Record.RecordId, 0, totalRecords)
	for res := range stream {
		require.NoError(s.T(), res.Err)
		for _, r := range res.Records {
			s.T().Logf("[%s]: %s\n", r.GetRecordId(), r.GetPayload())
			readRecords = append(readRecords, r.GetRecordId())
		}
	}

	s.ElementsMatch(writeRecords, readRecords)
}
