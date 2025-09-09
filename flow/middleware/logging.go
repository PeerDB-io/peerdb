package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

const grpcFullServiceName = "peerdb_route.FlowService"

func RequestLoggingMiddleware() grpc.UnaryServerInterceptor {
	if !internal.PeerDBRAPIRequestLoggingEnabled() {
		slog.Info("Request Logging Interceptor is disabled")
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	httpMethodMapping := buildHttpMethodMapping()
	slog.Info("Setting up request logging middleware")
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var httpMethod string
		if method, exists := httpMethodMapping[info.FullMethod]; exists {
			httpMethod = method
		}
		startTime := time.Now()
		slog.InfoContext(ctx, "Received gRPC request",
			slog.String("method", info.FullMethod),
			slog.String("httpMethod", httpMethod))

		resp, err := handler(ctx, req)
		if err != nil {
			slog.ErrorContext(ctx, "gRPC request failed",
				slog.String("method", info.FullMethod),
				slog.String("httpMethod", httpMethod),
				slog.Any("error", err),
				slog.Duration("duration", time.Since(startTime)))
		} else {
			slog.InfoContext(ctx, "gRPC request completed successfully",
				slog.String("method", info.FullMethod),
				slog.String("httpMethod", httpMethod),
				slog.Duration("duration", time.Since(startTime)))
		}
		return resp, err
	}
}

func buildHttpMethodMapping() map[string]string {
	mapping := make(map[string]string)

	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(grpcFullServiceName)
	if err != nil {
		slog.Warn("failed to find descriptor for "+grpcFullServiceName, slog.Any("error", err))
		return nil
	}
	serviceDesc, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		slog.Warn(string(desc.FullName()) + " is not a service descriptor")
		return nil
	}
	for i := range serviceDesc.Methods().Len() {
		method := serviceDesc.Methods().Get(i)
		grpcMethod := fmt.Sprintf("/%s/%s", serviceDesc.FullName(), method.Name())
		if rule, ok := proto.GetExtension(method.Options(), annotations.E_Http).(*annotations.HttpRule); ok {
			var httpMethod string
			switch rule.Pattern.(type) {
			case *annotations.HttpRule_Get:
				httpMethod = "GET"
			case *annotations.HttpRule_Post:
				httpMethod = "POST"
			case *annotations.HttpRule_Put:
				httpMethod = "PUT"
			case *annotations.HttpRule_Delete:
				httpMethod = "DELETE"
			default:
				httpMethod = "OTHER"
			}
			mapping[grpcMethod] = httpMethod
		}
	}
	return mapping
}
