package client

import (
	"github.com/hstreamdb/hstreamdb-go/hstream/security"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseUrl(t *testing.T) {
	tests := []struct {
		testName string
		input    string
		want     serverList
	}{
		{
			testName: "parse without prefix",
			input:    "127.0.0.1:7580,127.0.0.2:7581,127.0.0.3:7582",
			want:     serverList{"127.0.0.1:7580", "127.0.0.2:7581", "127.0.0.3:7582"},
		},
		{
			testName: "parse with prefix",
			input:    "hstream://127.0.0.1:7580,127.0.0.2:7581,127.0.0.3:7582",
			want:     serverList{"127.0.0.1:7580", "127.0.0.2:7581", "127.0.0.3:7582"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.testName, func(t *testing.T) {
			t.Parallel()
			client, err := NewRPCClient(tc.input, security.TLSAuth{})
			defer client.Close()
			require.NoError(t, err)
			require.Equal(t, tc.want, client.serverInfo)
		})
	}
}
