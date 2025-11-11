package middleware

import (
	"context"
	"log/slog"
	"runtime/debug"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryMiddleware returns a gRPC unary server interceptor that recovers from panics
func RecoveryMiddleware() grpc.UnaryServerInterceptor {
	return recovery.UnaryServerInterceptor(
		recovery.WithRecoveryHandlerContext(handlePanic),
	)
}

// handlePanic is called when a panic occurs in a gRPC handler
func handlePanic(ctx context.Context, p any) error {
	stack := debug.Stack()

	method := "unknown"
	if md, ok := grpc.Method(ctx); ok {
		method = md
	}

	slog.ErrorContext(ctx, "panic recovered in gRPC handler",
		slog.String("method", method),
		slog.Any("panic", p),
		slog.String("stack", string(stack)),
	)

	return status.Errorf(codes.Internal, "internal server error")
}
