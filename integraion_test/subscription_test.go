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
	s.serverUrl = test_util.ServerUrl
	s.client = hstream.NewHStreamClient(s.serverUrl)
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
