package hstream

import (
	"os"
	"testing"

	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

type mainWrapper struct {
	m *testing.M
}

var (
	server     *mockServer
	testClient *HStreamClient
)

func (mw *mainWrapper) Run() int {
	var err error
	server, err = startMockHStreamService(1, "127.0.0.1", 7580)
	if err != nil {
		util.Logger().Error("create mock server err", zap.Error(err))
		os.Exit(1)
	}
	defer server.stop()

	testClient, err = NewHStreamClient("hstream://127.0.0.1:7580")
	if err != nil {
		util.Logger().Error("create test client err", zap.Error(err))
		os.Exit(1)
	}
	defer testClient.Close()
	// util.SetLogLevel(util.DEBUG)

	return mw.m.Run()
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(&mainWrapper{m})
}
