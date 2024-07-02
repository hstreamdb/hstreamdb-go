package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hstreamdb/hstreamdb-go/hstream/security"
)

func TestParseUrl(t *testing.T) {
	tests := []struct {
		testName string
		input    string
		want     serverList
	}{
		{
			testName: "parse without port",
			input:    "hstream://127.0.0.1,127.0.0.2,127.0.0.3",
			want:     serverList{"127.0.0.1:6570", "127.0.0.2:6570", "127.0.0.3:6570"},
		},
		{
			testName: "parse with hstream prefix",
			input:    "hstream://127.0.0.1:6570,127.0.0.2:6570,127.0.0.3:6570",
			want:     serverList{"127.0.0.1:6570", "127.0.0.2:6570", "127.0.0.3:6570"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.testName, func(t *testing.T) {
			t.Parallel()
			client, err := NewRPCClient(tc.input, security.TLSAuth{}, "")
			defer client.Close()
			require.NoError(t, err)
			require.Equal(t, tc.want, client.serverInfo)
		})
	}
}
