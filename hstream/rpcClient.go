package hstream

import (
	"client/client"
	"client/hstreamrpc"
	"client/util"
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"sync"
)

type HStreamClient struct {
	sync.RWMutex
	connections   map[string]*grpc.ClientConn
	serverAddress []string
	closed        bool
}

func (c *HStreamClient) SendRequest(ctx context.Context, address string, req *hstreamrpc.Request) (*hstreamrpc.Response, error) {
	//TODO implement me
	panic("implement me")
}

func (c *HStreamClient) Close() error {
	//TODO implement me
	panic("implement me")
}

// NewHStreamClient TODO：use connection pool for each address
func NewHStreamClient(address []string) *HStreamClient {
	cli := &HStreamClient{
		connections:   make(map[string]*grpc.ClientConn),
		closed:        false,
		serverAddress: address,
	}
	return cli
}

func (c *HStreamClient) getConnection(address string) (*grpc.ClientConn, error) {
	c.RLock()
	if c.closed {
		c.RUnlock()
		return nil, errors.Errorf("Client closed.")
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

	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	conn, err := grpc.DialContext(ctx, address)
	cancel()
	if err != nil {
		util.Logger().Warn("Failed to connect to hstreamdb server", zap.String("address", address), zap.Error(err))
		return nil, err
	}
	util.Logger().Info("Connected to hstreamdb server", zap.String("address", address))
	c.connections[address] = conn
	return conn, nil
}
