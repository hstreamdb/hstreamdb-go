package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type TLSAuth struct {
	ClusterSSLCA   string
	ClusterSSLCert string
	ClusterSSLKey  string
}

func (t *TLSAuth) CheckEnable() bool {
	return len(t.ClusterSSLCA) != 0 || (len(t.ClusterSSLCert) != 0 && len(t.ClusterSSLKey) != 0)
}

func (t *TLSAuth) ToTLSConfig() (*tls.Config, error) {
	if len(t.ClusterSSLCA) == 0 {
		return &tls.Config{
			InsecureSkipVerify: true,
		}, nil
	}

	caCert, err := os.ReadFile(t.ClusterSSLCA)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("can't read ca file %s", t.ClusterSSLCA))
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, errors.New("fail to append ca certs.")
	}
	util.Logger().Info("load ca", zap.String("ca", t.ClusterSSLCA))

	cfg := &tls.Config{
		RootCAs:   caCertPool,
		ClientCAs: caCertPool,
	}

	if len(t.ClusterSSLCert) == 0 && len(t.ClusterSSLKey) == 0 {
		return cfg, nil
	}

	cert, err := tls.LoadX509KeyPair(t.ClusterSSLCert, t.ClusterSSLKey)
	if err != nil {
		return nil, errors.WithMessage(err, "can't load clint cert")
	}
	util.Logger().Info("load tls key pair", zap.String("cert", t.ClusterSSLKey), zap.String("key", t.ClusterSSLKey))
	cfg.Certificates = []tls.Certificate{cert}
	return cfg, nil
}
