package client

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const DIALTIMEOUT = 5 * time.Second
const REQUESTTIMEOUT = 5 * time.Second
const DEFAULTKEY = "__default__"

// Client is a client that sends RPC to HStreamDB server.
// It should not be used after calling Close().
type Client interface {
	// GetServerInfo returns the basic server infos of the cluster.
	GetServerInfo() ([]string, error)
	// SendRequest sends a rpc request to the server.
	SendRequest(ctx context.Context, address string, req *hstreamrpc.Request) (*hstreamrpc.Response, error)
	// Close closes the client.
	Close()
}

type serverList []string

func (s serverList) String() string {
	listStr := strings.Join(s, ",")
	return "[" + listStr + "]"
}

// RPCClient will send rpc requests to HStreamDB server.
type RPCClient struct {
	sync.RWMutex
	connections map[string]*grpc.ClientConn
	serverInfo  serverList
	// closed == 0 means client is closed
	closed int32
}

// GetServerInfo returns cached server info
func (c *RPCClient) GetServerInfo() ([]string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.serverInfo) == 0 {
		return nil, errors.New("no server info")
	}
	return c.serverInfo, nil
}

// SendRequest sends a hstreamrpc.Request to the specified server.
func (c *RPCClient) SendRequest(ctx context.Context, address string, req *hstreamrpc.Request) (*hstreamrpc.Response, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	cli := hstreampb.NewHStreamApiClient(conn)
	resp, err := hstreamrpc.Call(ctx, cli, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to send request %#+v to %s", req, address)
	}
	return resp, nil
}

func (c *RPCClient) Close() {
	util.Logger().Info("closing hstream client")
	for address, conn := range c.connections {
		util.Logger().Info("closing connection to server", zap.String("address", address))
		if err := conn.Close(); err != nil {
			util.Logger().Error("close connection failed", zap.String("address", address), zap.Error(err))
		}
	}
}

// isClosed check if the client is closed
func (c *RPCClient) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 0
}

// NewRPCClient TODO：use connection pool for each address
func NewRPCClient(address string) (*RPCClient, error) {
	cli := &RPCClient{
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
func (c *RPCClient) getConnection(address string) (*grpc.ClientConn, error) {
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
func (c *RPCClient) createConnection(address string) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()
	if conn, ok := c.connections[address]; ok {
		return conn, nil
	}

	conn, err := c.connect(address)
	if err != nil {
		return nil, err
	}
	c.connections[address] = conn
	util.Logger().Info("Connected to hstreamdb server", zap.String("address", address))
	return conn, nil
}

// connect will call grpc.DialContext with specified server address.
// when the function return success, the connection is ready to use.
func (c *RPCClient) connect(address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DIALTIMEOUT)
	defer cancel()
	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(hstreamrpc.UnaryClientInterceptor))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial %s", address)
	}

	// wait connection state convert to ready
	conn.WaitForStateChange(ctx, connectivity.Idle)
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		if !conn.WaitForStateChange(ctx, state) {
			return nil, errors.WithStack(err)
		}
	}
	return conn, nil
}

// requestServerInfo sends a describeCluster RPC to the specific server and returns information about all servers in current cluster.
func (c *RPCClient) requestServerInfo(address string) (serverList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), REQUESTTIMEOUT)
	defer cancel()
	res, err := c.SendRequest(ctx, address, &hstreamrpc.Request{Type: hstreamrpc.DescribeCluster, Req: &emptypb.Empty{}})
	if err != nil {
		return nil, err
	}

	serverNodes := res.Resp.(*hstreampb.DescribeClusterResponse).GetServerNodes()
	newInfo := make(serverList, 0, len(serverNodes))
	for _, node := range serverNodes {
		info := strings.Join([]string{node.GetHost(), strconv.Itoa(int(node.GetPort()))}, ":")
		newInfo = append(newInfo, info)
	}
	return newInfo, nil
}

// FIXME: need to call this method periodically ???
// serverDiscovery try to send a DescribeCluster RPC to each server address, update serverInfo
// with the result.
func (c *RPCClient) serverDiscovery() error {
	if c.isClosed() {
		util.Logger().Info("Client closed, stop serverDiscovery")
		return nil
	}
	c.RLock()
	if len(c.serverInfo) == 0 {
		c.RUnlock()
		return errors.New("No hstreamdb server address")
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

type Iter interface {
	// Valid returns false when iteration is done.
	Valid() bool
	// Next would advance the iterator by one. It's user's responsibility
	// to check if the iterator is still valid after call Next()
	Next()
	// Close the iterator
	Close()
}

type baseIter struct {
	length int
	cur    int
}

func (i *baseIter) Valid() bool {
	return i.cur >= 0 && i.cur < i.length
}

func (i *baseIter) Next() {
	i.cur++
}

func (i *baseIter) Close() {
	i.cur = -1
}

type StreamIter struct {
	streams []*hstreampb.Stream
	baseIter
}

func NewStreamIter(streams []*hstreampb.Stream) *StreamIter {
	iter := &StreamIter{streams: streams}
	iter.length = len(streams)
	iter.cur = 0
	return iter
}

func (i *StreamIter) GetStreams() []*hstreampb.Stream {
	return i.streams
}

func (i *StreamIter) Item() *hstreampb.Stream {
	return i.streams[i.cur]
}

type SubIter struct {
	subs []*hstreampb.Subscription
	baseIter
}

func NewSubIter(subs []*hstreampb.Subscription) *SubIter {
	iter := &SubIter{subs: subs}
	iter.length = len(subs)
	iter.cur = 0
	return iter
}

func (i *SubIter) GetSubs() []*hstreampb.Subscription {
	return i.subs
}

func (i *SubIter) Item() *hstreampb.Subscription {
	return i.subs[i.cur]
}
