package integraion_test

import (
	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
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
	client    *hstream.HStreamClient
}

func (s *testSubscriptionSuite) SetupTest() {
	var err error
	s.serverUrl = test_util.ServerUrl
	s.client, err = hstream.NewHStreamClient(s.serverUrl)
	s.NoError(err)
}

func (s *testSubscriptionSuite) TearDownTest() {
	//if err := s.client.Close(); err != nil {
	//	s.T().Error(err)
	//}
}

func (s *testSubscriptionSuite) TestCreateSubscription() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName, 1, 100)
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
	err := s.client.CreateStream(streamName, 1, 100)
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
	err := s.client.CreateStream(streamName, 1, 100)
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

	iter, err := s.client.ListSubscriptions()
	s.NoError(err)
	for ; iter.Valid(); iter.Next() {
		subId := iter.Item().GetSubscriptionId()
		s.Contains(subs, subId)
	}
}

func (s *testSubscriptionSuite) TestFetch() {
	rand.Seed(time.Now().UnixNano())
	streamName := "test_stream_" + strconv.Itoa(rand.Int())
	err := s.client.CreateStream(streamName, 1, 1000)
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

	producer, err := s.client.NewBatchProducer(streamName, hstream.EnableBatch(2))
	s.NoError(err)

	res := make([]client.AppendResult, 0, 10)
	for i := 0; i < 10; i++ {
		r := producer.Append(client.RAWRECORD, "key-10", []byte("test-value"+strconv.Itoa(i)))
		res = append(res, r)
	}

	rids := make([]*hstreampb.RecordId, 0, 10)
	for _, r := range res {
		resp, err := r.Ready()
		s.NoError(err)
		rids = append(rids, resp)
	}
	producer.Stop()

	consumer := s.client.NewConsumer("consumer-1", subId)

	dataCh, ackCh := consumer.StartFetch()
	fetchRes := make([]*hstreampb.RecordId, 0, 10)
	for res := range dataCh {
		receivedRecords, err := res.GetResult()
		s.NoError(err)
		ackIds := make([]*hstreampb.RecordId, 0, len(receivedRecords))
		for _, record := range receivedRecords {
			rid := record.GetRecordId()
			fetchRes = append(fetchRes, rid)
			ackIds = append(ackIds, rid)
		}
		ackCh <- ackIds
		if len(fetchRes) == 10 {
			consumer.Stop()
			break
		}
	}

	c1 := util.RecordIdComparator{RecordIdList: rids}
	c2 := util.RecordIdComparator{RecordIdList: fetchRes}
	s.True(util.RecordIdComparatorCompare(c1, c2))
}
