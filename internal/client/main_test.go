package client

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var server *mockServer

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(&mainWrapper{m})
}

type mainWrapper struct {
	m *testing.M
}

func (mw *mainWrapper) Run() int {
	var err error
	server, err = startMockHStreamService(1, "127.0.0.1", 7580)
	if err != nil {
		util.Logger().Error("create mock server err", zap.Error(err))
		os.Exit(1)
	}
	defer server.stop()

	return mw.m.Run()
}

type mockServer struct {
	hstreampb.HStreamApiServer
	grpcServer *grpc.Server
	Id         uint32
	Host       string
	Port       uint32
}

func (s *mockServer) DescribeCluster(ctx context.Context, req *emptypb.Empty) (*hstreampb.DescribeClusterResponse, error) {
	return &hstreampb.DescribeClusterResponse{
		ServerNodes: []*hstreampb.ServerNode{
			{Id: 1, Host: "127.0.0.1", Port: 7580},
			{Id: 1, Host: "127.0.0.2", Port: 7581},
			{Id: 1, Host: "127.0.0.3", Port: 7582},
		},
	}, nil
}

func (s *mockServer) stop() {
	s.grpcServer.Stop()
}

func startMockHStreamService(id uint32, host string, port int) (*mockServer, error) {
	conn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, errors.WithMessage(err, "can't start mock service")
	}
	port = conn.Addr().(*net.TCPAddr).Port
	util.Logger().Info("start hstream mock service", zap.String("address", fmt.Sprintf("%s:%d", host, port)))

	server := &mockServer{
		Id:   id,
		Host: host,
		Port: uint32(port),
	}
	s := grpc.NewServer(grpc.ConnectionTimeout(time.Minute))
	hstreampb.RegisterHStreamApiServer(s, server)
	server.grpcServer = s

	go func() {
		if err = s.Serve(conn); err != nil {
			util.Logger().Error("serve grpc service error", zap.Error(err))
			return
		}
	}()
	return server, nil
}
