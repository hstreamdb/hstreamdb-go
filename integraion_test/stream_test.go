package integraion_test

import (
	"client/client"
	"client/hstream"
	"client/util/test_util"
	"context"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"strconv"
	"testing"
)

func TestStream(t *testing.T) {
	suite.Run(t, new(testStreamSuite))
}

type testStreamSuite struct {
	suite.Suite
	serverUrl string
	client    client.Client
	stream    *hstream.Stream
}

func (s *testStreamSuite) SetupTest() {
	s.serverUrl = test_util.ServerUrl
	s.client = hstream.NewHStreamClient(s.serverUrl)
	s.stream = hstream.NewStream(s.client)
}

func (s *testStreamSuite) TearDownTest() {
	//if err := s.client.Close(); err != nil {
	//	s.T().Error(err)
	//}
}

func (s *testStreamSuite) TestCreateStream() {
	ctx := context.Background()
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	defer func() {
		_ = s.stream.Delete(ctx, streamName)
	}()
	s.NoError(err)
	err = s.stream.Create(ctx, streamName, 1)
	s.Error(err)
}

func (s *testStreamSuite) TestDeleteStream() {
	ctx := context.Background()
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	s.NoError(err)
	err = s.stream.Delete(ctx, streamName)
	s.NoError(err)
}

func (s *testStreamSuite) TestListStreams() {
	ctx := context.Background()
	streams := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		streamName := "test_stream_" + strconv.Itoa(rand.Int())
		err := s.stream.Create(ctx, streamName, 1)
		s.NoError(err)
		streams = append(streams, streamName)
	}
	defer func() {
		for _, streamName := range streams {
			_ = s.stream.Delete(ctx, streamName)
		}
	}()

	iter, err := s.stream.List(ctx)
	s.NoError(err)
	for ; iter.Valid(); iter.Next() {
		streamName := iter.Item().GetStreamName()
		s.Contains(streams, streamName)
	}
}
