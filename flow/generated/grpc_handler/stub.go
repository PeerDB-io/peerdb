// Stub file so `go mod tidy` can resolve this package without running
// codegen. Its presence doesn't interfere with the generated code.
// Blank imports must mirror what the generator emits, otherwise tidy
// will prune required modules from go.sum.
package grpc_handler

import (
	_ "google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/status"
)
