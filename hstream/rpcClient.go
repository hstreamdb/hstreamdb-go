package hstream

import (
	"client/client"
	hstreampb "client/gen-proto/hstream/server"
	"client/hstreamrpc"
	"client/util"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
	"sync"
)

type ServerSet map[string]struct{}

type HStreamClient struct {
	sync.RWMutex
	connections map[string]*grpc.ClientConn
	serverInfo  ServerSet
	closed      bool
}

func (c *HStreamClient) GetServerInfo() ([]string, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return nil, errors.New("client closed")
	}
	if len(c.serverInfo) == 0 {
		return nil, errors.New("no server info")
	}

	info := make([]string, 0, len(c.serverInfo))
	for k := range c.serverInfo {
		info = append(info, k)
	}
	return info, nil
}

func (c *HStreamClient) SendRequest(ctx context.Context, address string, req *hstreamrpc.Request) (*hstreamrpc.Response, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	cli := hstreampb.NewHStreamApiClient(conn)
	return hstreamrpc.Call(ctx, cli, req)
}

func (c *HStreamClient) Close() error {
	//TODO implement me
	panic("implement me")
}

// NewHStreamClient TODO：use connection pool for each address
func NewHStreamClient(address string) *HStreamClient {
	addr := strings.Split(address, ",")
	set := make(ServerSet, len(addr))
	for _, v := range addr {
		set[v] = struct{}{}
	}
	cli := &HStreamClient{
		connections: make(map[string]*grpc.ClientConn),
		closed:      false,
		serverInfo:  set,
	}
	return cli
}

func (c *HStreamClient) getConnection(address string) (*grpc.ClientConn, error) {
	c.RLock()
	if c.closed {
		c.RUnlock()
		return nil, errors.Errorf("Client closed.")
	}

	if len(c.connections) == 0 {
		c.RUnlock()
		if err := c.initConnection(); err != nil {
			return nil, err
		}
		c.RLock()
	}

	if conn, ok := c.connections[address]; ok {
		c.RUnlock()
		return conn, nil
	}
	c.RUnlock()

	return c.createConnection(address)
}

func (c *HStreamClient) createConnection(address string) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()
	if conn, ok := c.connections[address]; ok {
		return conn, nil
	}

	conn, err := c.connect(address)
	c.connections[address] = conn
	if err != nil {
		util.Logger().Warn("Failed to connect to hstreamdb server", zap.String("address", address), zap.Error(err))
		return nil, err
	}
	util.Logger().Info("Connected to hstreamdb server", zap.String("address", address))
	return conn, nil
}

func (c *HStreamClient) connect(address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn, err
}

func (c *HStreamClient) initConnection() error {
	var address string
	c.Lock()
	if len(c.connections) > 0 {
		c.Unlock()
		return nil
	}
	if len(c.serverInfo) == 0 {
		c.Unlock()
		return errors.Errorf("No hstreamdb server address")
	}

	for addr := range c.serverInfo {
		conn, err := c.connect(addr)
		if err != nil {
			util.Logger().Warn("Failed to connect to hstreamdb server", zap.String("address", addr), zap.Error(err))
			continue
		}
		c.connections[addr] = conn
		address = addr
		util.Logger().Info("Connected to hstreamdb server", zap.String("address", addr))
		break
	}
	if len(c.connections) == 0 {
		c.Unlock()
		return errors.Errorf("Failed to connect to hstreamdb server")
	}
	c.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), client.REQUESTTIMEOUT)
	defer cancel()
	res, err := c.SendRequest(ctx, address, &hstreamrpc.Request{Type: hstreamrpc.DescribeCluster, Req: &emptypb.Empty{}})
	if err != nil {
		cancel()
		return errors.Errorf("Send DescribeClusterReq to hstreamdb server %s err: %s", address, err.Error())
	}
	serverNodes := res.Resp.(*hstreampb.DescribeClusterResponse).GetServerNodes()
	set := make(ServerSet, len(serverNodes))
	for _, node := range serverNodes {
		// FIXME: use a more efficient way to concat address info
		info := fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort())
		set[info] = struct{}{}
	}
	c.Lock()
	c.serverInfo = set
	c.Unlock()
	return nil
}
