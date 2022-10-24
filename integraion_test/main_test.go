package integraion

import (
	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
	"os"
	"testing"

	"go.uber.org/goleak"
)

var serverUrl = "localhost:6580"

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
	return mw.m.Run()
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(&mainWrapper{m: m})
}
