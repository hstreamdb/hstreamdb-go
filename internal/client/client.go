package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/hstreamdb/hstreamdb-go/hstream/security"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	DialTimeout    = 5 * time.Second
	RequestTimeout = 5 * time.Second
	// UpdateServerInfoDuration TODO: support config by user
	// Period of updating server node information in milliseconds
	UpdateServerInfoDuration = 60000 // 60s
)

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
	tlsCfg      *tls.Config
	// closed == 0 means client is closed
	closed int32
	// Millisecond timestamp of the last update of the server node information
	lastServerInfoTs atomic.Int64
}

// GetServerInfo If the current time does not exceed the update period since the last update,
// the cached server node information is returned, otherwise try to update the node information and return
func (c *RPCClient) GetServerInfo() ([]string, error) {
	if time.Now().UnixMilli()-c.lastServerInfoTs.Load() >= UpdateServerInfoDuration {
		if err := c.updateServerInfo(); err != nil {
			return nil, err
		}
	}

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
	c.Lock()
	defer c.Unlock()
	util.Logger().Info("closing hstream client")
	for address, conn := range c.connections {
		util.Logger().Info("closing connection to server", zap.String("address", address))
		if err := conn.Close(); err != nil {
			util.Logger().Error("close connection failed", zap.String("address", address), zap.Error(err))
		}
	}
	c.closed = 0
}

// isClosed check if the client is closed
func (c *RPCClient) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 0
}

// NewRPCClient TODO：use connection pool for each address
func NewRPCClient(address string, tlsCfg security.TLSAuth) (*RPCClient, error) {
	address = strings.TrimSpace(address)
	schema := strings.Split(address, "://")
	if len(schema) != 2 {
		return nil, errors.New(fmt.Sprintf("unexpected address %s, correct example: hstream://127.0.0.1:6570", address))
	}

	if err := checkUrlSchema(schema[0], tlsCfg); err != nil {
		return nil, err
	}

	hosts := strings.Split(schema[1], ",")
	for i := 0; i < len(hosts); i++ {
		host := hosts[i]
		if strings.Contains(host, ":") {
			continue
		}
		hosts[i] = hosts[i] + ":6570"
	}

	cli := &RPCClient{
		connections: make(map[string]*grpc.ClientConn),
		closed:      1,
		serverInfo:  hosts,
	}

	if len(tlsCfg.ClusterSSLCA) != 0 {
		cfg, err := tlsCfg.ToTLSConfig()
		if err != nil {
			return nil, err
		}
		cli.tlsCfg = cfg
	}

	if err := cli.updateServerInfo(); err != nil {
		return nil, err
	}

	return cli, nil
}

// updateServerInfo will update serverList with current cluster node infos
func (c *RPCClient) updateServerInfo() error {
	c.RLock()
	addrs := c.serverInfo
	c.RUnlock()

	for _, addr := range addrs {
		info, err := c.requestServerInfo(addr)
		if err != nil {
			util.Logger().Warn("Failed to request serverInfo", zap.String("address", addr), zap.Error(err))
			continue
		}

		c.Lock()
		c.serverInfo = info
		c.lastServerInfoTs.Store(time.Now().UnixMilli())
		c.Unlock()
		util.Logger().Info("Update server info success", zap.String("new server", c.serverInfo.String()))
		return nil
	}
	return errors.New("Failed to update hstreamdb server info.")
}

// checkUrlSchema check if the url is legal
func checkUrlSchema(schema string, tlsCfg security.TLSAuth) error {
	urlSchema := strings.ToUpper(schema)
	if urlSchema != "HSTREAM" && urlSchema != "HSTREAMS" {
		return errors.New(fmt.Sprintf("invalid url schema: %s", schema))
	}

	if urlSchema == "HSTREAMS" && !tlsCfg.CheckEnable() {
		return errors.New("hstreams url schema should set ca")
	}
	return nil
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
	util.Logger().Info("Connected to hstreamdb server", zap.String("address", address), zap.String("state", conn.GetState().String()))
	return conn, nil
}

// connect will call grpc.DialContext with specified server address.
// when the function return success, the connection is ready to use.
func (c *RPCClient) connect(address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DialTimeout)
	tlsOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if c.tlsCfg != nil {
		tlsOpt = grpc.WithTransportCredentials(credentials.NewTLS(c.tlsCfg))
	}
	conn, err := grpc.DialContext(ctx, address, tlsOpt,
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(hstreamrpc.UnaryClientInterceptor, grpc_retry.UnaryClientInterceptor())),
	)
	if err != nil {
		cancel()
		return nil, errors.Wrapf(err, "failed to dial %s", address)
	}
	cancel()

	//// wait connection state convert to ready
	// FIXME: use grpc.WithBlock() to wait connection ready. This part of the code is currently reserved for debugging purposes
	waitCtx, waitCancel := context.WithTimeout(context.Background(), DialTimeout)
	conn.WaitForStateChange(waitCtx, connectivity.Idle)
	defer waitCancel()
	for {
		state := conn.GetState()
		util.Logger().Debug("hstreamdb server connection state", zap.String("address", address), zap.String("state", state.String()))
		if state == connectivity.Ready {
			break
		}
		if !conn.WaitForStateChange(waitCtx, state) {
			util.Logger().Error("WaitForStateChange failed", zap.String("address", address), zap.String("state", state.String()))
			return nil, errors.Wrapf(waitCtx.Err(), "WaitForStateChange failed, state: %s", state.String())
		}
	}
	return conn, nil
}

// requestServerInfo sends a describeCluster RPC to the specific server and returns information about all servers in current cluster.
func (c *RPCClient) requestServerInfo(address string) (serverList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
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
