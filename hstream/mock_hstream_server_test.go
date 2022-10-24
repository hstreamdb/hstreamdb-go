package hstream

import (
	"context"
	"fmt"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"time"
)

type hacker func(req proto.Message) (resp proto.Message, err error)

type mockServer struct {
	hstreampb.HStreamApiServer
	grpcServer *grpc.Server
	Id         uint32
	Host       string
	Port       uint32

	responseSetter []hacker
}

func (s *mockServer) LookupShard(ctx context.Context, req *hstreampb.LookupShardRequest) (*hstreampb.LookupShardResponse, error) {
	return &hstreampb.LookupShardResponse{
		ShardId: req.ShardId,
		ServerNode: &hstreampb.ServerNode{
			Id:   s.Id,
			Host: s.Host,
			Port: s.Port,
		},
	}, nil
}

func (s *mockServer) DescribeCluster(ctx context.Context, req *emptypb.Empty) (*hstreampb.DescribeClusterResponse, error) {
	return &hstreampb.DescribeClusterResponse{
		ServerNodes: []*hstreampb.ServerNode{
			{Id: s.Id, Host: s.Host, Port: s.Port},
		},
	}, nil
}

func (s *mockServer) ListShards(ctx context.Context, req *hstreampb.ListShardsRequest) (*hstreampb.ListShardsResponse, error) {
	resp, err := s.getResponse(req)
	if err != nil {
		return nil, err
	}

	res := resp.(*hstreampb.ListShardsResponse)
	return res, nil
}

func (s *mockServer) getResponse(req proto.Message) (proto.Message, error) {
	setter := s.responseSetter[0]
	s.responseSetter = s.responseSetter[1:]

	resp, err := setter(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *mockServer) Append(ctx context.Context, req *hstreampb.AppendRequest) (*hstreampb.AppendResponse, error) {
	resp, err := s.getResponse(req)
	if err != nil {
		return nil, err
	}

	res := resp.(*hstreampb.AppendResponse)
	return res, nil
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
