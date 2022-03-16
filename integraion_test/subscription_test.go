package integraion_test

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/client"
	"github.com/hstreamdb/hstreamdb-go/hstream"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamDB/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/hstreamdb/hstreamdb-go/util/test_util"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestSubscription(t *testing.T) {
	suite.Run(t, new(testSubscriptionSuite))
}

type testSubscriptionSuite struct {
	suite.Suite
	serverUrl string
	client    client.Client
	stream    *hstream.Stream
	sub       *hstream.Subscription
}

func (s *testSubscriptionSuite) SetupTest() {
	var err error
	s.serverUrl = test_util.ServerUrl
	s.client, err = hstream.NewHStreamClient(s.serverUrl)
	s.NoError(err)
	s.stream = hstream.NewStream(s.client)
	s.sub = hstream.NewSubscription(s.client)
}

func (s *testSubscriptionSuite) TearDownTest() {
	//if err := s.client.Close(); err != nil {
	//	s.T().Error(err)
	//}
}

func (s *testSubscriptionSuite) TestCreateSubscription() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	defer func() {
		_ = s.stream.Delete(ctx, streamName)
	}()
	s.NoError(err)

	subId := "test_subscription_" + strconv.Itoa(rand.Int())
	err = s.sub.Create(ctx, subId, streamName, 5)
	defer func() {
		_ = s.sub.Delete(ctx, subId)
	}()
	s.NoError(err)
}

func (s *testSubscriptionSuite) TestDeleteSubscription() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	defer func() {
		_ = s.stream.Delete(ctx, streamName)
	}()
	s.NoError(err)

	subId := "test_subscription_" + strconv.Itoa(rand.Int())
	err = s.sub.Create(ctx, subId, streamName, 5)
	s.NoError(err)
	err = s.sub.Delete(ctx, subId)
	s.NoError(err)
}

func (s *testSubscriptionSuite) TestListSubscription() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	defer func() {
		_ = s.stream.Delete(ctx, streamName)
	}()
	s.NoError(err)

	subs := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		subId := "test_subscription_" + strconv.Itoa(rand.Int())
		err := s.sub.Create(ctx, subId, streamName, 5)
		s.NoError(err)
		subs = append(subs, subId)
	}
	defer func() {
		for _, subId := range subs {
			_ = s.stream.Delete(ctx, subId)
		}
	}()

	iter, err := s.sub.List(ctx)
	s.NoError(err)
	for ; iter.Valid(); iter.Next() {
		subId := iter.Item().GetSubscriptionId()
		s.Contains(subs, subId)
	}
}

func (s *testSubscriptionSuite) TestFetch() {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.stream.Create(ctx, streamName, 1)
	defer func() {
		_ = s.stream.Delete(ctx, streamName)
	}()
	s.NoError(err)
	subId := "test_subscription_" + strconv.Itoa(rand.Int())
	err = s.sub.Create(ctx, subId, streamName, 5)
	defer func() {
		_ = s.sub.Delete(ctx, subId)
	}()
	s.NoError(err)

	producer := s.stream.MakeProducer(streamName, "key-10", hstream.EnableBatch(2))

	res := make([]client.AppendResult, 0, 10)
	for i := 0; i < 10; i++ {
		r := producer.Append(client.RAWRECORD, []byte("test-value"+strconv.Itoa(i)))
		res = append(res, r)
	}

	rids := make([]*hstreampb.RecordId, 0, 10)
	for _, r := range res {
		resp, err := r.Ready()
		s.NoError(err)
		rids = append(rids, resp)
	}
	producer.Stop()

	consumer, err := s.sub.MakeConsumer(subId, "consumer-1")
	s.NoError(err)

	handler := test_util.MakeGatherRidsHandler(len(rids))
	consumer.Fetch(context.Background(), handler)
	<-handler.Done
	consumer.Stop()

	c1 := util.RecordIdComparator{RecordIdList: rids}
	c2 := util.RecordIdComparator{RecordIdList: handler.GetRes()}
	s.True(util.RecordIdComparatorCompare(c1, c2))
}
