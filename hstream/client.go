package hstream

import (
	"context"
	"math/rand"

	"github.com/hstreamdb/hstreamdb-go/hstream/security"
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	"github.com/hstreamdb/hstreamdb-go/util"
)

// HStreamClient is the client for the HStreamDB service.
type HStreamClient struct {
	client.Client
}

type authConfig struct {
	tlsCfg security.TLSAuth
	token  string
}

// AuthOpts set tls configurations
type AuthOpts func(cfg *authConfig)

// WithCaCert set ca path
func WithCaCert(ca string) AuthOpts {
	return func(cfg *authConfig) {
		cfg.tlsCfg.ClusterSSLCA = ca
	}
}

// WithClientCert set client cert path
func WithClientCert(cert string) AuthOpts {
	return func(cfg *authConfig) {
		cfg.tlsCfg.ClusterSSLCert = cert
	}
}

// WithClientKey set client key path
func WithClientKey(key string) AuthOpts {
	return func(cfg *authConfig) {
		cfg.tlsCfg.ClusterSSLKey = key
	}
}

func WithAuthToken(token string) AuthOpts {
	return func(cfg *authConfig) {
		cfg.token = token
	}
}

func NewHStreamClient(address string, authOps ...AuthOpts) (*HStreamClient, error) {
	authCfg := authConfig{}
	for _, cfg := range authOps {
		cfg(&authCfg)
	}

	cli, err := client.NewRPCClient(address, authCfg.tlsCfg, authCfg.token)
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
	ctx, cancel := context.WithTimeout(context.Background(), client.DialTimeout)
	defer cancel()
	return c.SendRequest(ctx, address, req)
}

func (c *HStreamClient) randomServer() (string, error) {
	infos, err := c.GetServerInfo(false)
	if err != nil {
		return "", err
	}
	idx := rand.Intn(len(infos))
	return infos[idx], nil
}

func (c *HStreamClient) SetLogLevel(level util.LogLevel) {
	util.SetLogLevel(level)
}
