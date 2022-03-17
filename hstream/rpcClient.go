package hstream

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/hstreamrpc"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamDB/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
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

type ServerList []string

func (s ServerList) String() string {
	listStr := strings.Join(s, ",")
	return "[" + listStr + "]"
}

type HStreamClient struct {
	sync.RWMutex
	connections map[string]*grpc.ClientConn
	serverInfo  ServerList
	// closed == 0 means client is closed
	closed int32
}

// GetServerInfo returns cached server info
func (c *HStreamClient) GetServerInfo() ([]string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.serverInfo) == 0 {
		return nil, errors.New("no server info")
	}
	return c.serverInfo, nil
}

func (c *HStreamClient) SendRequest(ctx context.Context, address string, req *hstreamrpc.Request) (*hstreamrpc.Response, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	cli := hstreampb.NewHStreamApiClient(conn)
	return hstreamrpc.Call(ctx, cli, req)
}

func (c *HStreamClient) Close() {
	for address, conn := range c.connections {
		if err := conn.Close(); err != nil {
			util.Logger().Error("close connection failed", zap.String("address", address), zap.Error(err))
		}
	}
}

// isClosed check if the client is closed
func (c *HStreamClient) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 0
}

// NewHStreamClient TODO：use connection pool for each address
func NewHStreamClient(address string) (*HStreamClient, error) {
	cli := &HStreamClient{
		connections: make(map[string]*grpc.ClientConn),
		closed:      1,
		serverInfo:  strings.Split(address, ","),
	}

	for _, addr := range cli.serverInfo {
		conn, err := cli.connect(addr)
		if err != nil {
			util.Logger().Warn("Failed to connect to hstreamdb server", zap.String("address", addr), zap.Error(err))
			continue
		}

		info, err := cli.requestServerInfo(addr)
		if err != nil {
			continue
		}
		cli.serverInfo = info
		cli.connections[addr] = conn
		util.Logger().Info("InitConnection success, connect to server", zap.String("address", addr))
		return cli, nil
	}

	return nil, errors.New("Failed to connect to hstreamdb server")
}

// getConnection returns a connection to the server.
func (c *HStreamClient) getConnection(address string) (*grpc.ClientConn, error) {
	if c.isClosed() {
		return nil, errors.New("client closed")
	}

	c.RLock()
	if conn, ok := c.connections[address]; ok {
		c.RUnlock()
		return conn, nil
	}
	c.RUnlock()

	// FIXME: check if the address is in the server list before create a new connection ???
	return c.createConnection(address)
}

// createConnection will try to establish connection with specified server.
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
	if err != nil {
		return nil, err
	}

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
	return conn, nil
}

// requestServerInfo sends a describeCluster RPC to the specific server and returns information about all servers in current cluster.
func (c *HStreamClient) requestServerInfo(address string) (ServerList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.REQUESTTIMEOUT)
	defer cancel()
	res, err := c.SendRequest(ctx, address, &hstreamrpc.Request{Type: hstreamrpc.DescribeCluster, Req: &emptypb.Empty{}})
	if err != nil {
		return nil, err
	}

	serverNodes := res.Resp.(*hstreampb.DescribeClusterResponse).GetServerNodes()
	newInfo := make(ServerList, 0, len(serverNodes))
	for _, node := range serverNodes {
		info := strings.Join([]string{node.GetHost(), strconv.Itoa(int(node.GetPort()))}, ":")
		newInfo = append(newInfo, info)
	}
	return newInfo, nil
}

// FIXME: need to call this method periodically ???
// serverDiscovery try to send a DescribeCluster RPC to each server address, update serverInfo
// with the result.
func (c *HStreamClient) serverDiscovery() error {
	if c.isClosed() {
		util.Logger().Info("Client closed, stop serverDiscovery")
		return nil
	}
	c.RLock()
	if len(c.serverInfo) == 0 {
		c.RUnlock()
		return errors.Errorf("No hstreamdb server address")
	}
	oldInfo := c.serverInfo
	c.RUnlock()

	for _, addr := range oldInfo {
		newInfo, err := c.requestServerInfo(addr)
		if err != nil {
			continue
		}

		c.Lock()
		c.serverInfo = newInfo
		c.Unlock()
		return nil
	}

	return errors.New("Failed to update server info")
}
