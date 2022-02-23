package hstream

import (
	"client/client"
	hstreampb "client/gen-proto/hstream/server"
	"client/hstreamrpc"
	"client/util"
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type ServerSet map[string]struct{}

func (s ServerSet) String() string {
	builder := strings.Builder{}
	builder.WriteString("[")
	for server := range s {
		builder.WriteString(server)
		builder.WriteString(",")
	}
	builder.WriteString("]")
	return builder.String()
}

type HStreamClient struct {
	sync.RWMutex
	connections map[string]*grpc.ClientConn
	serverInfo  ServerSet
	// closed == 0 means client is closed
	closed int32
}

func (c *HStreamClient) GetServerInfo() ([]string, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed == 0 {
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

func (c *HStreamClient) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 0
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
		closed:      1,
		serverInfo:  set,
	}
	return cli
}

func (c *HStreamClient) getConnection(address string) (*grpc.ClientConn, error) {
	if c.isClosed() {
		return nil, errors.New("client closed")
	}

	if len(c.connections) == 0 {
		c.Lock()
		if err := c.initConnection(); err != nil {
			c.Unlock()
			return nil, err
		}
		c.Unlock()
	}

	c.RLock()
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
	if err != nil {
		util.Logger().Warn("Failed to connect to hstreamdb server", zap.String("address", address), zap.Error(err))
		return nil, errors.WithStack(err)
	}
	c.connections[address] = conn
	util.Logger().Info("Connected to hstreamdb server", zap.String("address", address))
	return conn, nil
}

func (c *HStreamClient) connect(address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptor))

	// wait connection state convert to ready
	conn.WaitForStateChange(ctx, connectivity.Idle)
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		if !conn.WaitForStateChange(ctx, state) {
			return nil, ctx.Err()
		}
	}
	return conn, err
}

// initConnection iterate all server address and try to create connection.
// return when first connection established. it's the caller's responsibility
// to hold a lock before call this method.
func (c *HStreamClient) initConnection() error {
	if c.closed == 0 {
		return errors.New("client closed")
	}
	if len(c.connections) != 0 {
		return nil
	}
	if len(c.serverInfo) == 0 {
		return errors.Errorf("No hstreamdb server address")
	}

	for addr := range c.serverInfo {
		conn, err := c.connect(addr)
		if err != nil {
			util.Logger().Warn("Failed to connect to hstreamdb server", zap.String("address", addr), zap.Error(err))
			continue
		}

		c.connections[addr] = conn
		util.Logger().Info("InitConnection success, connect to server", zap.String("address", addr))
		go c.serverDiscovery()
		return nil
	}

	return errors.Errorf("Can't init connection with serverInfo: %s", c.serverInfo)
}

// FIXME: need to call this method periodically ???
// serverDiscovery try to send a DescribeCluster RPC to each server address, update serverInfo
// with the result.
func (c *HStreamClient) serverDiscovery() error {
	if c.isClosed() {
		util.Logger().Info("Client closed, stop serverDiscovery")
		return nil
	}
	if len(c.serverInfo) == 0 {
		return errors.Errorf("No hstreamdb server address")
	}

	ctx, cancel := context.WithTimeout(context.Background(), client.REQUESTTIMEOUT)
	defer cancel()

	oldInfo := c.serverInfo
	for addr := range oldInfo {
		res, err := c.SendRequest(ctx, addr, &hstreamrpc.Request{Type: hstreamrpc.DescribeCluster, Req: &emptypb.Empty{}})
		if err != nil {
			continue
		}

		serverNodes := res.Resp.(*hstreampb.DescribeClusterResponse).GetServerNodes()
		set := make(ServerSet, len(serverNodes))
		for _, node := range serverNodes {
			// FIXME: use a more efficient way to concat address info
			info := strings.Join([]string{node.GetHost(), strconv.Itoa(int(node.GetPort()))}, ":")
			set[info] = struct{}{}
		}
		c.Lock()
		c.serverInfo = set
		c.Unlock()
		return nil
	}

	return errors.New("Failed to update server info")
}
