package cmd

import (
	"go/ast"
	"go/token"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"

	"github.com/PeerDB-io/peerdb/flow/middleware"
)

// TestRouteHandlersWrapErrors verifies that all FlowRequestHandler methods properly wrap errors
// in exceptions.New.*ApiError calls before returning them. This ensures consistent error handling
// across all gRPC route handlers.
func TestRouteHandlersWrapErrors(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes,
		Dir:  ".",
	}

	pkgs, err := packages.Load(cfg, ".")
	require.NoError(t, err)
	require.Len(t, pkgs, 1, "Expected exactly one package")

	pkg := pkgs[0]

	// Known route handler methods that should wrap errors
	// These correspond to the methods defined in the FlowServiceServer interface
	routeHandlerMethods := middleware.BuildHttpMethodMapping(t.Context())

	var violations []string

	for _, file := range pkg.Syntax {
		ast.Inspect(file, func(n ast.Node) bool {
			if fn, ok := n.(*ast.FuncDecl); ok {
				// Check if this is a FlowRequestHandler method that should wrap errors
				if _, isHandler := routeHandlerMethods["/"+middleware.GrpcFullServiceName+"/"+fn.Name.Name]; isHandler &&
					isFlowRequestHandlerMethod(fn) {
					t.Logf("Testing function: %s", fn.Name.Name)
					violations = append(violations, analyzeRouteHandlerErrorWrapping(t, pkg, fn)...)
				} else {
					t.Logf("Skipping function: %s", fn.Name.Name)
				}
			}
			return true
		})
	}

	if len(violations) > 0 {
		t.Errorf("Found %d error wrapping violations in route handlers:\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
}

// isFlowRequestHandlerMethod checks if a function is a method on FlowRequestHandler
func isFlowRequestHandlerMethod(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return false
	}

	recv := fn.Recv.List[0]
	if starExpr, ok := recv.Type.(*ast.StarExpr); ok {
		if ident, ok := starExpr.X.(*ast.Ident); ok {
			return ident.Name == reflect.TypeOf(FlowRequestHandler{}).Name()
		}
	}
	return false
}

// analyzeRouteHandlerErrorWrapping analyzes a route handler method for proper error wrapping
func analyzeRouteHandlerErrorWrapping(t *testing.T, pkg *packages.Package, fn *ast.FuncDecl) []string {
	t.Helper()
	var violations []string

	// Check if function returns an error
	if !returnsError(fn) {
		return violations
	}
	// Analyze all return statements in the function
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.ReturnStmt:
			ret := s
			if hasNoPeerTestComment(t, pkg, ret, fn) {
				return true // Skip this return statement
			}

			if hasUnwrappedError(ret) {
				pos := pkg.Fset.Position(ret.Pos())
				violations = append(violations,
					formatViolation(pos, fn.Name.Name, "returns unwrapped error, wrap it via one of `exceptions\\.New.*ApiError`"))
			}
		case *ast.FuncLit:
			t.Logf("Skipping function literal at: %s", pkg.Fset.Position(s.Pos()))
			return false
		}
		return true
	})

	return violations
}

// hasNoPeerTestComment checks if a return statement has the //nopeertest:grpcReturn comment on the line before
func hasNoPeerTestComment(t *testing.T, pkg *packages.Package, ret *ast.ReturnStmt, fn *ast.FuncDecl) bool {
	t.Helper()
	pos := pkg.Fset.Position(ret.Pos())

	// Find the source file
	for _, file := range pkg.Syntax {
		filePos := pkg.Fset.Position(file.Pos())
		if filePos.Filename == pos.Filename {
			// Look through comments to find one on the line before the return statement
			for _, commentGroup := range file.Comments {
				for _, comment := range commentGroup.List {
					commentPos := pkg.Fset.Position(comment.Pos())
					// Check if the comment is on the line immediately before the return statement
					if commentPos.Line == pos.Line-1 && strings.EqualFold(comment.Text, "//nopeertest:grpcReturn") {
						t.Log(formatViolation(pos, fn.Name.Name, "is being skipped due to nopeertest:grpcReturn comment"))
						return true
					}
				}
			}
			break
		}
	}
	return false
}

// returnsError checks if a function returns an error as its last return value
func returnsError(fn *ast.FuncDecl) bool {
	if fn.Type.Results == nil || len(fn.Type.Results.List) == 0 {
		return false
	}

	results := fn.Type.Results.List
	lastResult := results[len(results)-1]

	if ident, ok := lastResult.Type.(*ast.Ident); ok {
		return ident.Name == "error"
	}
	return false
}

// hasUnwrappedError checks if a return statement returns an unwrapped error
func hasUnwrappedError(ret *ast.ReturnStmt) bool {
	if len(ret.Results) == 1 {
		if fn, ok := ret.Results[0].(*ast.CallExpr); ok {
			if sel, ok := fn.Fun.(*ast.Ident); ok {
				// We sometimes use this utility function to wrap the error in a GRPC error
				if sel.Name == "wrapErrorAsFailedPrecondition" {
					return false
				}
			}
		}
		return true
	}

	// Check the last return value (should be error)
	lastResult := ret.Results[len(ret.Results)-1]

	// Skip nil returns (success cases)
	if isNilIdentifier(lastResult) {
		return false
	}

	// Check if it's a direct identifier (like "err") - this would be unwrapped
	if ident, ok := lastResult.(*ast.Ident); ok {
		return ident.Name == "err" || strings.HasSuffix(ident.Name, "Err") || strings.HasSuffix(ident.Name, "Error")
	}

	// Check if it's a function call - ensure it's an exceptions.New* call
	if call, ok := lastResult.(*ast.CallExpr); ok {
		return !isExceptionsNewCall(call)
	}

	return false
}

// isNilIdentifier checks if an expression is nil
func isNilIdentifier(expr ast.Expr) bool {
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name == "nil"
	}
	return false
}

var exceptionsNewCallRegex = regexp.MustCompile(`^New[A-Z].*ApiError$`)

// isExceptionsNewCall checks if a function call is an exceptions.New* call
func isExceptionsNewCall(call *ast.CallExpr) bool {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		if ident, ok := sel.X.(*ast.Ident); ok {
			return ident.Name == "exceptions" && exceptionsNewCallRegex.MatchString(sel.Sel.Name)
		}
	}
	return false
}

// formatViolation formats a violation message with file location
func formatViolation(pos token.Position, methodName, issue string) string {
	return pos.String() + ": " + methodName + " " + issue
}
