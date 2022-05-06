package integraion_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/util/test_util"
	"github.com/stretchr/testify/suite"
)

func TestSubscription(t *testing.T) {
	suite.Run(t, new(testSubscriptionSuite))
}

type testSubscriptionSuite struct {
	suite.Suite
	serverUrl string
	client    *hstream.HStreamClient
}

func (s *testSubscriptionSuite) SetupTest() {
	var err error
	s.serverUrl = test_util.ServerUrl
	s.client, err = hstream.NewHStreamClient(s.serverUrl)
	s.NoError(err)
}

func (s *testSubscriptionSuite) TearDownTest() {
	s.client.Close()
}

func (s *testSubscriptionSuite) TestCreateSubscription() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	subId := "test_subscription_" + strconv.Itoa(rand.Int())
	err = s.client.CreateSubscription(subId, streamName, 5)
	defer func() {
		_ = s.client.DeleteSubscription(subId)
	}()
	s.NoError(err)
}

func (s *testSubscriptionSuite) TestDeleteSubscription() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	subId := "test_subscription_" + strconv.Itoa(rand.Int())
	err = s.client.CreateSubscription(subId, streamName, 5)
	s.NoError(err)
	err = s.client.DeleteSubscription(subId)
	s.NoError(err)
}

func (s *testSubscriptionSuite) TestListSubscription() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)

	subs := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		subId := "test_subscription_" + strconv.Itoa(rand.Int())
		err := s.client.CreateSubscription(subId, streamName, 5)
		s.NoError(err)
		subs = append(subs, subId)
	}
	defer func() {
		for _, subId := range subs {
			_ = s.client.DeleteSubscription(subId)
		}
	}()

	res, err := s.client.ListSubscriptions()
	s.NoError(err)
	for _, sub := range res {
		s.Contains(subs, sub.SubscriptionId)
	}
}

func (s *testSubscriptionSuite) TestFetch() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName)
	defer func() {
		_ = s.client.DeleteStream(streamName)
	}()
	s.NoError(err)
	subId := "test_subscription_" + strconv.Itoa(rand.Int())
	err = s.client.CreateSubscription(subId, streamName, 5)
	defer func() {
		_ = s.client.DeleteSubscription(subId)
	}()
	s.NoError(err)

	producer, err := s.client.NewBatchProducer(streamName, hstream.EnableBatch(5))
	s.NoError(err)

	res := make([]hstream.AppendResult, 0, 100)
	for i := 0; i < 100; i++ {
		rawRecord, _ := hstream.NewHStreamRawRecord("key-1", []byte("test-value"+strconv.Itoa(i)))
		r := producer.Append(rawRecord)
		res = append(res, r)
	}

	rids := make([]hstream.RecordId, 0, 100)
	for _, r := range res {
		resp, err := r.Ready()
		s.NoError(err)
		rids = append(rids, resp)
	}
	producer.Stop()

	consumer := s.client.NewConsumer("consumer-1", subId)
	defer consumer.Stop()

	dataCh := consumer.StartFetch()
	fetchRes := make([]hstream.RecordId, 0, 100)
	for res := range dataCh {
		s.NoError(res.Err)
		for _, record := range res.Result {
			rid := record.GetRecordId()
			fetchRes = append(fetchRes, rid)
			record.Ack()
		}
		if len(fetchRes) == 100 {
			break
		}
	}

	c1 := test_util.RecordIdComparator{RecordIdList: rids}
	c2 := test_util.RecordIdComparator{RecordIdList: fetchRes}
	s.True(test_util.RecordIdComparatorCompare(c1, c2))
}
