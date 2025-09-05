package middleware

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

func RequestIdMiddleware() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		requestIds := md.Get(shared.RequestIdKey.String())
		requestId := uuid.NewString()
		if requestIds != nil {
			requestId = requestIds[0]
		}
		return handler(context.WithValue(ctx, shared.RequestIdKey, requestId), req)
	}
}
