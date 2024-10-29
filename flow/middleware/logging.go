package middleware

import (
	"context"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"google.golang.org/grpc"
	"log/slog"
)

func RequestLoggingMiddleWare() grpc.UnaryServerInterceptor {
	if !peerdbenv.PeerDBRAPIRequestLoggingEnabled() {
		slog.Info("Request Logging Interceptor is disabled")
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	slog.Info("Setting up request logging middleware")
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		slog.Info("Received gRPC request", slog.String("method", info.FullMethod))

		resp, err := handler(ctx, req)
		if err != nil {
			slog.Error("gRPC request failed", slog.String("method", info.FullMethod), slog.Any("error", err))
		} else {
			slog.Info("gRPC request completed successfully", slog.String("method", info.FullMethod))
		}
		return resp, err
	}
}
