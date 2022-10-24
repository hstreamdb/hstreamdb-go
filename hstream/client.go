package hstream

import (
	"context"
	"github.com/hstreamdb/hstreamdb-go/hstream/security"
	"math/rand"

	"github.com/hstreamdb/hstreamdb-go/internal/client"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
)

// HStreamClient is the client for the HStreamDB service.
type HStreamClient struct {
	client.Client
}

// TLSOps set tls configurations
type TLSOps func(cfg *security.TLSAuth)

// WithCaCert set ca path
func WithCaCert(ca string) TLSOps {
	return func(cfg *security.TLSAuth) {
		cfg.ClusterSSLCA = ca
	}
}

// WithClientCert set client cert path
func WithClientCert(cert string) TLSOps {
	return func(cfg *security.TLSAuth) {
		cfg.ClusterSSLCert = cert
	}
}

// WithClientKey set client key path
func WithClientKey(key string) TLSOps {
	return func(cfg *security.TLSAuth) {
		cfg.ClusterSSLKey = key
	}
}

func NewHStreamClient(address string, tlsOps ...TLSOps) (*HStreamClient, error) {
	tlsCfg := security.TLSAuth{}
	for _, cfg := range tlsOps {
		cfg(&tlsCfg)
	}

	cli, err := client.NewRPCClient(address, tlsCfg)
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
