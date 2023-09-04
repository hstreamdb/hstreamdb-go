package integraion

import (
	"os"
	"testing"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

var serverUrl = "hstream://172.16.156.11:6580"

var client *hstream.HStreamClient

type mainWrapper struct {
	m *testing.M
}

func (mw *mainWrapper) Run() int {
	var err error
	client, err = hstream.NewHStreamClient(serverUrl)
	defer client.Close()

	if err != nil {
		util.Logger().Error("create client err", zap.Error(err))
		os.Exit(1)
	}
	util.SetLogLevel(util.DEBUG)
	return mw.m.Run()
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(&mainWrapper{m: m})
}
