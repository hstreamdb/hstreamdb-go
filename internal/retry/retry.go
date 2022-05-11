package retry

import (
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"time"
)

const (
	BACKOFF_BASE_TIME = 1 * time.Second
	MAX_RETRIES       = 3
)

func AppendRetry() []grpc.CallOption {
	return []grpc.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BACKOFF_BASE_TIME)),
		grpc_retry.WithCodes(codes.Unavailable),
		grpc_retry.WithMax(MAX_RETRIES),
	}
}

func FetchRetry() []grpc.CallOption {
	return []grpc.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BACKOFF_BASE_TIME)),
		grpc_retry.WithCodes(codes.Unavailable),
		grpc_retry.WithMax(MAX_RETRIES),
	}
}
