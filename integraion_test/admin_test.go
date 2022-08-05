package integraion

import (
	"testing"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/util/test_util"
	"github.com/stretchr/testify/suite"
)

func TestAdmin(t *testing.T) {
	suite.Run(t, new(testAdminSuite))
}

type testAdminSuite struct {
	suite.Suite
	serverUrl string
	client    *hstream.HStreamClient
}

func (s *testAdminSuite) SetupTest() {
	var err error
	s.serverUrl = test_util.ServerUrl
	s.client, err = hstream.NewHStreamClient(s.serverUrl)
	s.NoError(err)
}

func (s *testAdminSuite) TearDownTest() {
	s.client.Close()
}

func (s *testAdminSuite) TestGetStatus() {
	cmd := "server status"
	res, err := s.client.AdminRequest(cmd)
	s.NoError(err)
	s.T().Log(res)
}

func (s *testAdminSuite) TestStatsAppends() {
	cmd := "server stats appends -i 1s -i 10s"
	res, err := s.client.AdminRequest(cmd)
	s.NoError(err)
	s.T().Log(res)
}
