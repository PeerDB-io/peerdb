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

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

var GrpcFullServiceName = protos.FlowService_ServiceDesc.ServiceName

// RequestLoggingMiddleware always logs failed requests, so handlers should return errors without logging them.
// Received/completed logs are gated behind PEERDB_API_REQUEST_LOGGING_ENABLED.
func RequestLoggingMiddleware(ctx context.Context) grpc.UnaryServerInterceptor {
	logRequests := internal.PeerDBRAPIRequestLoggingEnabled(ctx)
	httpMethodMapping := BuildHttpMethodMapping(ctx)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		attrs := []any{
			slog.String("method", info.FullMethod),
			slog.String("httpMethod", httpMethodMapping[info.FullMethod]),
		}
		if r, ok := req.(interface{ GetFlowJobName() string }); ok && r.GetFlowJobName() != "" {
			attrs = append(attrs, slog.String(string(shared.FlowNameKey), r.GetFlowJobName()))
		}
		startTime := time.Now()
		if logRequests {
			slog.InfoContext(ctx, "Received gRPC request", attrs...)
		}

		resp, err := handler(ctx, req)
		if err != nil {
			slog.ErrorContext(ctx, "gRPC request failed",
				append(attrs, slog.Any("error", err), slog.Duration("duration", time.Since(startTime)))...)
		} else if logRequests {
			slog.InfoContext(ctx, "gRPC request completed successfully",
				append(attrs, slog.Duration("duration", time.Since(startTime)))...)
		}
		return resp, err
	}
}

func BuildHttpMethodMapping(ctx context.Context) map[string]string {
	mapping := make(map[string]string)

	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(GrpcFullServiceName))
	if err != nil {
		slog.WarnContext(ctx, "failed to find descriptor for "+GrpcFullServiceName, slog.Any("error", err))
		return nil
	}
	serviceDesc, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		slog.WarnContext(ctx, string(desc.FullName())+" is not a service descriptor")
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
