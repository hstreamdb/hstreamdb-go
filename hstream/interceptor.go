package hstream

import (
	hstreampb "client/gen-proto/hstream/server"
	"client/util"
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func UnaryClientInterceptor(ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	switch req.(type) {
	case *hstreampb.LookupStreamRequest:
		util.Logger().Debug("unaryRPC", zap.String("method", method), zap.String("target", cc.Target()),
			zap.String("req", req.(*hstreampb.LookupStreamRequest).String()))
	case *hstreampb.LookupSubscriptionRequest:
		util.Logger().Debug("unaryRPC", zap.String("method", method), zap.String("target", cc.Target()),
			zap.String("req", req.(*hstreampb.LookupSubscriptionRequest).String()))
	case *hstreampb.LookupSubscriptionWithOrderingKeyRequest:
		util.Logger().Debug("unaryRPC", zap.String("method", method), zap.String("target", cc.Target()),
			zap.String("req", req.(*hstreampb.LookupSubscriptionWithOrderingKeyRequest).String()))
	case *hstreampb.Stream:
		util.Logger().Debug("unaryRPC", zap.String("method", method), zap.String("target", cc.Target()),
			zap.String("req", req.(*hstreampb.Stream).String()))
	case *hstreampb.Subscription:
		util.Logger().Debug("unaryRPC", zap.String("method", method), zap.String("target", cc.Target()),
			zap.String("req", req.(*hstreampb.Subscription).String()))
	default:
		util.Logger().Debug("unaryRPC", zap.String("method", method), zap.String("target", cc.Target()))
	}

	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		util.Logger().Debug("unaryRPC error", zap.String("method", method), zap.String("target", cc.Target()),
			zap.Error(err))
	}
	return err
}
