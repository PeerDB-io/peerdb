// Stub file so `go mod tidy` can resolve this package without running
// `buf generate`. Its presence doesn't interfere with the generated code.
// Blank imports must mirror what the generator emits, otherwise tidy
// will prune required modules from go.sum.
package protos

import (
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/utilities"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	_ "google.golang.org/grpc"
	_ "google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/grpclog"
	_ "google.golang.org/grpc/metadata"
	_ "google.golang.org/grpc/status"
	_ "google.golang.org/protobuf/proto"
	_ "google.golang.org/protobuf/reflect/protoreflect"
	_ "google.golang.org/protobuf/runtime/protoimpl"
	_ "google.golang.org/protobuf/types/descriptorpb"
	_ "google.golang.org/protobuf/types/known/durationpb"
	_ "google.golang.org/protobuf/types/known/timestamppb"
)
