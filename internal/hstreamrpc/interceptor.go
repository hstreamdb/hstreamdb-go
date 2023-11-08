package hstreamrpc

import (
	"context"
	"reflect"

	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func UnaryClientInterceptor(ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	commFields := []zap.Field{
		zap.String("method", method),
		zap.String("target", cc.Target()),
	}
	switch req := req.(type) {
	case *hstreampb.LookupShardRequest:
		commFields = append(commFields, zap.String("req", req.String()))
	case *hstreampb.LookupSubscriptionRequest:
		commFields = append(commFields, zap.String("req", req.String()))
	case *hstreampb.Stream:
		commFields = append(commFields, zap.String("req", req.String()))
	case *hstreampb.Subscription:
		commFields = append(commFields, zap.String("req", req.String()))
	case *hstreampb.AppendRequest:
		//commFields = append(commFields, zap.String("req", req.String()))
	default:
	}
	util.Logger().Debug("unaryRPC", commFields...)

	ctx1 := context.Background()
	if err := invoker(ctx1, method, req, reply, cc, opts...); err != nil {
		strReq := reflect.ValueOf(req).MethodByName("String").Call([]reflect.Value{})[0].String()
		util.Logger().Debug("unaryRPC error", zap.String("method", method), zap.String("req", strReq), zap.String("target", cc.Target()),
			zap.Error(err))
		return err
	}
	return nil
}

func RegistUnaryAuthInterceptor(token string) func(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+token)
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}

func RegistStreamAuthInterceptor(token string) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+token)
		return streamer(newCtx, desc, cc, method, opts...)
	}
}
