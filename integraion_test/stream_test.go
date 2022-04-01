package integraion_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/util/test_util"
	"github.com/stretchr/testify/suite"
)

func TestStream(t *testing.T) {
	suite.Run(t, new(testStreamSuite))
}

type testStreamSuite struct {
	suite.Suite
	serverUrl string
	client    *hstream.HStreamClient
}

func (s *testStreamSuite) SetupTest() {
	var err error
	s.serverUrl = test_util.ServerUrl
	s.client, err = hstream.NewHStreamClient(s.serverUrl)
	s.NoError(err)
}

func (s *testStreamSuite) TearDownTest() {
	s.client.Close()
}

func (s *testStreamSuite) TestCreateStream() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)
	err = s.client.CreateStream(streamName)
	s.Error(err)
}

func (s *testStreamSuite) TestDeleteStream() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	s.NoError(err)
	err = s.client.DeleteStream(streamName)
	s.NoError(err)
}

func (s *testStreamSuite) TestListStreams() {
	rand.Seed(time.Now().UnixNano())
	streams := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		streamName := "test_stream_" + strconv.Itoa(rand.Int())
		err := s.client.CreateStream(streamName)
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
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer := s.client.NewProducer(streamName)
	res := make([]hstream.AppendResult, 0, 100)
	rawRecord, _ := hstream.NewHStreamRawRecord("key-1", []byte("value-1"))
	for i := 0; i < 100; i++ {
		r := producer.Append(rawRecord)
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
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer, err := s.client.NewBatchProducer(streamName, hstream.EnableBatch(10))
	s.NoError(err)
	defer producer.Stop()

	res := make([]hstream.AppendResult, 0, 100)
	for i := 0; i < 100; i++ {
		rawRecord, _ := hstream.NewHStreamRawRecord("key-1", []byte("test-value"+strconv.Itoa(i)))
		r := producer.Append(rawRecord)
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
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer, err := s.client.NewBatchProducer(streamName, hstream.EnableBatch(10))
	defer producer.Stop()
	s.NoError(err)

	keys := []string{"test-key1", "test-key2", "test-key3"}
	rids := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(3)
	for _, key := range keys {
		go func(key string) {
			result := make([]hstream.AppendResult, 0, 100)
			for i := 0; i < 100; i++ {
				rawRecord, _ := hstream.NewHStreamRawRecord("key-1", []byte(fmt.Sprintf("test-value-%s-%d", key, i)))
				r := producer.Append(rawRecord)
				result = append(result, r)
			}
			rids.Store(key, result)
			wg.Done()
		}(key)
	}

	wg.Wait()
	rids.Range(func(key, value interface{}) bool {
		k := key.(string)
		res := value.([]hstream.AppendResult)
		for idx, r := range res {
			resp, err := r.Ready()
			s.NoError(err)
			s.T().Log(fmt.Sprintf("[key: %s]: record[%d]=%s", k, idx, resp.String()))
		}
		return true
	})
}
