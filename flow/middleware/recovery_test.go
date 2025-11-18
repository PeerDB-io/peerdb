package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mock handlers
func panicHandler(ctx context.Context, req any) (any, error) {
	panic("something went wrong")
}

func nilPointerPanicHandler(ctx context.Context, req any) (any, error) {
	var ptr *string
	//nolint:nilness
	_ = *ptr
	return nil, nil
}

func normalErrorHandler(ctx context.Context, req any) (any, error) {
	return nil, errors.New("normal error")
}

func successHandler(ctx context.Context, req any) (any, error) {
	return "success", nil
}

func nilConnectionConfigsHandler(ctx context.Context, req any) (any, error) {
	panic("runtime error: invalid memory address or nil pointer dereference")
}

func TestRecoveryMiddleware_RecoversPanic(t *testing.T) {
	interceptor := RecoveryMiddleware()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/TestMethod",
	}

	resp, err := interceptor(t.Context(), "test request", info, panicHandler)

	require.Error(t, err)
	require.Nil(t, resp)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code())
}

func TestRecoveryMiddleware_RecoversNilPointerPanic(t *testing.T) {
	interceptor := RecoveryMiddleware()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/NilPointerTest",
	}

	resp, err := interceptor(t.Context(), "test request", info, nilPointerPanicHandler)

	require.Error(t, err)
	require.Nil(t, resp)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code())
}

func TestRecoveryMiddleware_NormalErrorPassesThrough(t *testing.T) {
	interceptor := RecoveryMiddleware()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/NormalError",
	}

	resp, err := interceptor(t.Context(), "test request", info, normalErrorHandler)

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, "normal error", err.Error())
}

func TestRecoveryMiddleware_SuccessPassesThrough(t *testing.T) {
	interceptor := RecoveryMiddleware()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Success",
	}

	resp, err := interceptor(t.Context(), "test request", info, successHandler)

	require.NoError(t, err)
	require.Equal(t, "success", resp)
}

func TestRecoveryMiddleware_CreateCDCFlowNilConfig(t *testing.T) {
	interceptor := RecoveryMiddleware()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/peerdb_flow.FlowService/CreateCDCFlow",
	}

	resp, err := interceptor(t.Context(), "test request", info, nilConnectionConfigsHandler)

	require.Error(t, err)
	require.Nil(t, resp)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code())
	require.Contains(t, err.Error(), "internal server error")
}
