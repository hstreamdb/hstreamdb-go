package integraion_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/util"
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
	util.SetLogLevel(util.DEBUG)
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

	res, err := s.client.ListStreams()
	s.NoError(err)

	for _, stream := range res {
		s.Contains(streams, stream.StreamName)
	}
}

func (s *testStreamSuite) TestAppendHRecord() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer := s.client.NewProducer(streamName)
	res := make([]hstream.AppendResult, 0, 100)
	payload := map[string]interface{}{
		"key":       "key-1",
		"value":     []byte(fmt.Sprintf("test-value-%s-%d", "key-1", 1)),
		"timestamp": time.Now().UnixNano(),
	}
	hRecord, _ := hstream.NewHStreamHRecord("key-1", payload)
	for i := 0; i < 100; i++ {
		r := producer.Append(hRecord)
		res = append(res, r)
	}

	for _, r := range res {
		resp, err := r.Ready()
		s.NoError(err)
		s.T().Log(resp.String())
	}
}

func (s *testStreamSuite) TestAppendRawRecord() {
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

	producer, err := s.client.NewBatchProducer(streamName, hstream.WithBatch(10, 150), hstream.WithFlowControl(400))
	s.NoError(err)
	defer producer.Stop()

	res := make([]hstream.AppendResult, 0, 1000)
	for i := 0; i < 1000; i++ {
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

func (s *testStreamSuite) TestBatchAppendHRecordWithMultiKey() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer, err := s.client.NewBatchProducer(streamName, hstream.WithBatch(10, 1000))
	defer producer.Stop()
	s.NoError(err)

	keys := []string{"test-key1", "test-key2", "test-key3", "test-key4", "test-key5", "test-key6", "test-key7", "test-key8", "test-key9"}
	rids := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(9)
	for _, key := range keys {
		go func(key string) {
			result := make([]hstream.AppendResult, 0, 1000)
			for i := 0; i < 1000; i++ {
				payload := map[string]interface{}{
					"key":       key,
					"value":     []byte(fmt.Sprintf("test-value-%s-%d", key, i)),
					"timestamp": time.Now().UnixNano(),
				}
				hRecord, _ := hstream.NewHStreamHRecord(key, payload)
				r := producer.Append(hRecord)
				result = append(result, r)
			}
			rids.Store(key, result)
			wg.Done()
		}(key)
	}

	wg.Wait()
	rids.Range(func(key, value interface{}) bool {
		//k := key.(string)
		res := value.([]hstream.AppendResult)
		for _, r := range res {
			_, err := r.Ready()
			s.NoError(err)
			//s.T().Log(fmt.Sprintf("[key: %s]: record[%d]=%s", k, idx, resp.String()))
		}
		return true
	})
}

func (s *testStreamSuite) TestBatchAppendRawRecordWithMultiKey() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	producer, err := s.client.NewBatchProducer(streamName, hstream.WithBatch(10, 250))
	defer producer.Stop()
	s.NoError(err)

	keys := []string{"test-key1", "test-key2", "test-key3", "test-key4", "test-key5", "test-key6", "test-key7", "test-key8", "test-key9"}
	rids := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(9)
	for _, key := range keys {
		go func(key string) {
			result := make([]hstream.AppendResult, 0, 1000)
			for i := 0; i < 1000; i++ {
				rawRecord, _ := hstream.NewHStreamRawRecord(key, []byte(fmt.Sprintf("test-value-%s-%d", key, i)))
				r := producer.Append(rawRecord)
				result = append(result, r)
			}
			rids.Store(key, result)
			wg.Done()
		}(key)
	}

	wg.Wait()
	rids.Range(func(key, value interface{}) bool {
		//k := key.(string)
		res := value.([]hstream.AppendResult)
		for _, r := range res {
			_, err := r.Ready()
			s.NoError(err)
			//s.T().Log(fmt.Sprintf("[key: %s]: record[%d]=%s", k, idx, resp.String()))
		}
		return true
	})
}
