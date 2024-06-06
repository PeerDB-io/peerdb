package middleware

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/PeerDB-io/peer-flow/logger"
)

func CreateAuthServerInterceptor(ctx context.Context, plaintext string, unauthenticatedMethods []string) grpc.UnaryServerInterceptor {
	if plaintext == "" {
		logger.LoggerFromCtx(ctx).Warn("Authentication is disabled")
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	unauthenticatedMethodsSet := make(map[string]struct{})
	for _, method := range unauthenticatedMethods {
		unauthenticatedMethodsSet[method] = struct{}{}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := unauthenticatedMethodsSet[info.FullMethod]; ok {
			return handler(ctx, req)
		}
		ctx, err := Authorize(ctx, plaintext)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// CreateRequestLoggingInterceptor logs all requests
// this is important for monitoring, debugging and auditing
func CreateRequestLoggingInterceptor(ignoredMethods []string) grpc.UnaryServerInterceptor {
	ignoredMethodsSet := make(map[string]struct{})
	for _, method := range ignoredMethods {
		ignoredMethodsSet[method] = struct{}{}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := ignoredMethodsSet[info.FullMethod]; ok {
			return handler(ctx, req)
		}
		start := time.Now()
		logger.LoggerFromCtx(ctx).Info(
			"Received request",
			slog.String("grpc.method", info.FullMethod),
		)
		resp, err := handler(ctx, req)
		var errorCode string
		if err != nil {
			// if error is a grpc error, extract the error code
			if grpcErr, ok := status.FromError(err); ok {
				errorCode = grpcErr.Code().String()
			}
		}
		// TODO maybe also look at x-forwarded-for ?
		var clientIp string
		if p, ok := peer.FromContext(ctx); ok {
			clientIp = p.Addr.String()
		}

		logger.LoggerFromCtx(ctx).Info(
			"Request completed",
			slog.String("grpc.method", info.FullMethod),
			slog.Duration("duration", time.Since(start)),
			slog.Float64("duration_seconds", time.Since(start).Seconds()),
			slog.Any("error", err),
			slog.String("grpc.code", errorCode),
			slog.String("client_ip", clientIp),
		)
		return resp, err
	}
}
