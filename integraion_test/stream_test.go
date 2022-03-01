package integraion_test

import (
	"context"
	"fmt"
	"github.com/hstreamdb/hstreamdb-go/client"
	"github.com/hstreamdb/hstreamdb-go/hstream"
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
	client    client.Client
	stream    *hstream.Stream
}

func (s *testStreamSuite) SetupTest() {
	var err error
	s.serverUrl = test_util.ServerUrl
	s.client, err = hstream.NewHStreamClient(s.serverUrl)
	s.NoError(err)
	s.stream = hstream.NewStream(s.client)
}

func (s *testStreamSuite) TearDownTest() {
	//if err := s.client.Close(); err != nil {
	//	s.T().Error(err)
	//}
}

func (s *testStreamSuite) TestCreateStream() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
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
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	s.NoError(err)
	err = s.stream.Delete(ctx, streamName)
	s.NoError(err)
}

func (s *testStreamSuite) TestListStreams() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
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

func (s *testStreamSuite) TestAppend() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	defer func() {
		_ = s.stream.Delete(ctx, streamName)
	}()
	s.NoError(err)

	producer := s.stream.MakeProducer(streamName, "test-key")
	res := make([]client.AppendResult, 0, 100)
	for i := 0; i < 100; i++ {
		r := producer.Append(client.RAWRECORD, []byte("test-value"))
		res = append(res, r)
	}

	for _, r := range res {
		resp, err := r.Ready()
		s.NoError(err)
		s.T().Log(resp.String())
	}
}

func (s *testStreamSuite) TestBatchAppend() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	defer func() {
		_ = s.stream.Delete(ctx, streamName)
	}()
	s.NoError(err)

	producer := s.stream.MakeProducer(streamName, "test-key", hstream.EnableBatch(10))
	defer producer.Stop()

	res := make([]client.AppendResult, 0, 100)
	for i := 0; i < 100; i++ {
		r := producer.Append(client.RAWRECORD, []byte("test-value"+strconv.Itoa(i)))
		res = append(res, r)
	}

	for idx, r := range res {
		resp, err := r.Ready()
		s.NoError(err)
		s.T().Log(fmt.Sprintf("record[%d]=%s", idx, resp.String()))
	}
}

func (s *testStreamSuite) TestBatchAppendMultiKey() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	defer func() {
		_ = s.stream.Delete(ctx, streamName)
	}()
	s.NoError(err)

	producer1 := s.stream.MakeProducer(streamName, "test-key1", hstream.EnableBatch(10))
	defer producer1.Stop()
	producer2 := s.stream.MakeProducer(streamName, "test-key2", hstream.EnableBatch(10))
	defer producer2.Stop()
	producer3 := s.stream.MakeProducer(streamName, "test-key3", hstream.EnableBatch(10))
	defer producer3.Stop()

	producers := []client.StreamProducer{producer1, producer2, producer3}
	rids := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(3)
	for pid, p := range producers {
		go func(pid int, p client.StreamProducer) {
			result := make([]client.AppendResult, 0, 100)
			for i := 0; i < 100; i++ {
				r := p.Append(client.RAWRECORD, []byte(fmt.Sprintf("test-value-%d-%d", pid, i)))
				result = append(result, r)
			}
			rids.Store(pid, result)
			wg.Done()
		}(pid, p)
	}

	wg.Wait()
	rids.Range(func(key, value interface{}) bool {
		pid := key.(int)
		res := value.([]client.AppendResult)
		for idx, r := range res {
			resp, err := r.Ready()
			s.NoError(err)
			s.T().Log(fmt.Sprintf("[producer%d]: record[%d]=%s", pid, idx, resp.String()))
		}
		return true
	})
}
