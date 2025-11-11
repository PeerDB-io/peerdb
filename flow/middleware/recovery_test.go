package middleware

import (
    "context"
    "errors"
    "strings"
    "testing"
    
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

// Mock handler that panics
func panicHandler(ctx context.Context, req interface{}) (interface{}, error) {
    panic("something went wrong")
}

// Mock handler that panics with nil pointer
func nilPointerPanicHandler(ctx context.Context, req interface{}) (interface{}, error) {
    var ptr *string
    _ = *ptr // This will panic
    return nil, nil
}

// Mock handler that returns normal error
func normalErrorHandler(ctx context.Context, req interface{}) (interface{}, error) {
    return nil, errors.New("normal error")
}

// Mock handler that succeeds
func successHandler(ctx context.Context, req interface{}) (interface{}, error) {
    return "success", nil
}

func TestRecoveryMiddleware_CreateCDCFlowNilConfig(t *testing.T) {
    interceptor := RecoveryMiddleware()
    
    ctx := context.Background()
    
    // Just pass nil directly - the handler will panic when trying to use it
    req := "doesn't matter what this is"
    
    info := &grpc.UnaryServerInfo{
        FullMethod: "/peerdb_flow.FlowService/CreateCDCFlow",
    }
    
    // This should recover from the panic
    resp, err := interceptor(ctx, req, info, nilConnectionConfigsHandler)
    
    if err == nil {
        t.Fatal("expected error from panic recovery, got nil")
    }
    
    st, ok := status.FromError(err)
    if !ok {
        t.Fatalf("expected gRPC status error, got: %T - %v", err, err)
    }
    
    if st.Code() != codes.Internal {
        t.Errorf("expected Internal code, got %v", st.Code())
    }
    
    if resp != nil {
        t.Errorf("expected nil response, got %v", resp)
    }
    
    if !strings.Contains(err.Error(), "internal server error") {
        t.Errorf("expected 'internal server error' in message, got: %v", err.Error())
    }
}

// Simplified mock handler - just panics unconditionally to simulate the nil pointer issue
func nilConnectionConfigsHandler(ctx context.Context, req interface{}) (interface{}, error) {
    // Simulate what happens when ConnectionConfigs is nil and we try to access it
    panic("runtime error: invalid memory address or nil pointer dereference")
}

func TestRecoveryMiddleware_RecoversPanic(t *testing.T) {
    interceptor := RecoveryMiddleware()
    
    ctx := context.Background()
    req := "test request"
    
    info := &grpc.UnaryServerInfo{
        FullMethod: "/test.Service/TestMethod",
    }
    
    // Call the handler that panics
    resp, err := interceptor(ctx, req, info, panicHandler)
    
    // Should return error, not panic
    if err == nil {
        t.Fatal("expected error, got nil")
    }
    
    // Should be gRPC Internal error
    st, ok := status.FromError(err)
    if !ok {
        t.Fatal("expected gRPC status error")
    }
    
    if st.Code() != codes.Internal {
        t.Errorf("expected Internal code, got %v", st.Code())
    }
    
    if resp != nil {
        t.Errorf("expected nil response, got %v", resp)
    }
}

func TestRecoveryMiddleware_RecoversNilPointerPanic(t *testing.T) {
    interceptor := RecoveryMiddleware()
    
    ctx := context.Background()
    req := "test request"
    
    info := &grpc.UnaryServerInfo{
        FullMethod: "/test.Service/NilPointerTest",
    }
    
    resp, err := interceptor(ctx, req, info, nilPointerPanicHandler)
    
    if err == nil {
        t.Fatal("expected error, got nil")
    }
    
    st, ok := status.FromError(err)
    if !ok {
        t.Fatal("expected gRPC status error")
    }
    
    if st.Code() != codes.Internal {
        t.Errorf("expected Internal code, got %v", st.Code())
    }
    
    if resp != nil {
        t.Errorf("expected nil response, got %v", resp)
    }
}

func TestRecoveryMiddleware_NormalErrorPassesThrough(t *testing.T) {
    interceptor := RecoveryMiddleware()
    
    ctx := context.Background()
    req := "test request"
    
    info := &grpc.UnaryServerInfo{
        FullMethod: "/test.Service/NormalError",
    }
    
    resp, err := interceptor(ctx, req, info, normalErrorHandler)
    
    if err == nil {
        t.Fatal("expected error, got nil")
    }
    
    if err.Error() != "normal error" {
        t.Errorf("expected 'normal error', got %v", err)
    }
    
    if resp != nil {
        t.Errorf("expected nil response, got %v", resp)
    }
}

func TestRecoveryMiddleware_SuccessPassesThrough(t *testing.T) {
    interceptor := RecoveryMiddleware()
    
    ctx := context.Background()
    req := "test request"
    
    info := &grpc.UnaryServerInfo{
        FullMethod: "/test.Service/Success",
    }
    
    resp, err := interceptor(ctx, req, info, successHandler)
    
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    
    if resp != "success" {
        t.Errorf("expected 'success', got %v", resp)
    }
}