package hstream

import (
	"context"
	hstreampb "github.com/hstreamdb/hstreamdb-go/gen-proto/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func UnaryClientInterceptor(ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	commFields := []zap.Field{
		zap.String("method", method),
		zap.String("target", cc.Target()),
	}
	switch req.(type) {
	case *hstreampb.LookupStreamRequest:
		commFields = append(commFields, zap.String("req", req.(*hstreampb.LookupStreamRequest).String()))
	case *hstreampb.LookupSubscriptionRequest:
		commFields = append(commFields, zap.String("req", req.(*hstreampb.LookupSubscriptionRequest).String()))
	case *hstreampb.LookupSubscriptionWithOrderingKeyRequest:
		commFields = append(commFields, zap.String("req", req.(*hstreampb.LookupSubscriptionWithOrderingKeyRequest).String()))
	case *hstreampb.Stream:
		commFields = append(commFields, zap.String("req", req.(*hstreampb.Stream).String()))
	case *hstreampb.Subscription:
		commFields = append(commFields, zap.String("req", req.(*hstreampb.Subscription).String()))
	default:
	}
	util.Logger().Debug("unaryRPC", commFields...)

	if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
		util.Logger().Debug("unaryRPC error", zap.String("method", method), zap.String("target", cc.Target()),
			zap.Error(err))
		return err
	}
	return nil
}
