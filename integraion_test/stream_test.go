package integraion_test

import (
	"fmt"
	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	"github.com/hstreamdb/hstreamdb-go/util/test_util"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestStream(t *testing.T) {
	suite.Run(t, new(testStreamSuite))
}

type testStreamSuite struct {
	suite.Suite
	serverUrl string
	client    *hstream.HStreamClient
	stream    *hstream.Stream
}

func (s *testStreamSuite) SetupTest() {
	var err error
	s.serverUrl = test_util.ServerUrl
	s.client, err = hstream.NewHStreamClient(s.serverUrl)
	s.NoError(err)
}

func (s *testStreamSuite) TearDownTest() {
	//if err := s.client.Close(); err != nil {
	//	s.T().Error(err)
	//}
}

func (s *testStreamSuite) TestCreateStream() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName, 1, 1)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)
	err = s.client.CreateStream(streamName, 1, 1)
	s.Error(err)
}

func (s *testStreamSuite) TestDeleteStream() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName, 1, 1)
	s.NoError(err)
	err = s.client.DeleteStream(streamName)
	s.NoError(err)
}

func (s *testStreamSuite) TestListStreams() {
	rand.Seed(time.Now().UnixNano())
	streams := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		streamName := "test_stream_" + strconv.Itoa(rand.Int())
		err := s.client.CreateStream(streamName, 1, 1)
		s.NoError(err)
		streams = append(streams, streamName)
	}
	defer func() {
		for _, streamName := range streams {
			_ = s.client.DeleteStream(streamName)
		}
	}()

	iter, err := s.client.ListStreams()
	s.NoError(err)
	for ; iter.Valid(); iter.Next() {
		streamName := iter.Item().GetStreamName()
		s.Contains(streams, streamName)
	}
}

func (s *testStreamSuite) TestAppend() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName, 1, 1)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer := s.client.NewProducer(streamName)
	res := make([]client.AppendResult, 0, 100)
	for i := 0; i < 100; i++ {
		r := producer.Append(client.RAWRECORD, "key-1", []byte("test-value"))
		res = append(res, r)
	}

	for _, r := range res {
		resp, err := r.Ready()
		s.NoError(err)
		s.T().Log(resp.String())
	}
}

func (s *testStreamSuite) TestBatchAppend() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName, 1, 1)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer, err := s.client.NewBatchProducer(streamName, hstream.EnableBatch(10))
	s.NoError(err)
	defer producer.Stop()

	res := make([]client.AppendResult, 0, 100)
	for i := 0; i < 100; i++ {
		r := producer.Append(client.RAWRECORD, "test-key", []byte("test-value"+strconv.Itoa(i)))
		res = append(res, r)
	}

	for idx, r := range res {
		resp, err := r.Ready()
		s.NoError(err)
		s.T().Log(fmt.Sprintf("record[%d]=%s", idx, resp.String()))
	}
}

func (s *testStreamSuite) TestBatchAppendMultiKey() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName, 1, 1)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer, err := s.client.NewBatchProducer(streamName, hstream.EnableBatch(10))
	s.NoError(err)

	keys := []string{"test-key1", "test-key2", "test-key3"}
	rids := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(3)
	for _, key := range keys {
		go func(key string) {
			result := make([]client.AppendResult, 0, 100)
			for i := 0; i < 100; i++ {
				r := producer.Append(client.RAWRECORD, key, []byte(fmt.Sprintf("test-value-%s-%d", key, i)))
				result = append(result, r)
			}
			rids.Store(key, result)
			wg.Done()
		}(key)
	}

	wg.Wait()
	rids.Range(func(key, value interface{}) bool {
		k := key.(string)
		res := value.([]client.AppendResult)
		for idx, r := range res {
			resp, err := r.Ready()
			s.NoError(err)
			s.T().Log(fmt.Sprintf("[key: %s]: record[%d]=%s", k, idx, resp.String()))
		}
		return true
	})
}
