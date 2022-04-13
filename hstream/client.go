package hstream

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	"math/rand"
)

// HStreamClient is the client for the HStreamDB service.
type HStreamClient struct {
	client.Client
}

func NewHStreamClient(address string) (*HStreamClient, error) {
	cli, err := client.NewRPCClient(address)
	if err != nil {
		return nil, err
	}
	return &HStreamClient{
		cli,
	}, nil
}

// reqToRandomServer is a helper function to send request to a random server.
func (c *HStreamClient) reqToRandomServer(req *hstreamrpc.Request) (*hstreamrpc.Response, error) {
	address, err := c.randomServer()
	if err != nil {
		return nil, err
	}
	return c.sendRequest(address, req)
}

// sendRequest is a helper function to wrap SendRequest function with timeout context.
func (c *HStreamClient) sendRequest(address string, req *hstreamrpc.Request) (*hstreamrpc.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.DIALTIMEOUT)
	defer cancel()
	return c.SendRequest(ctx, address, req)
}

func (c *HStreamClient) randomServer() (string, error) {
	infos, err := c.GetServerInfo()
	if err != nil {
		return "", err
	}
	idx := rand.Intn(len(infos))
	return infos[idx], nil
}
