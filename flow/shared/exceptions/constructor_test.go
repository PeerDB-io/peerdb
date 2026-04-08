package exceptions

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"
)

// Tests that constructors return pointers, so that matching into a pointer like we do in the classifier works
func TestErrorConstructorsShouldReturnPointers(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedFiles | packages.NeedSyntax,
		Dir:  ".",
	}

	pkgs, err := packages.Load(cfg, ".")
	require.NoError(t, err)
	require.Len(t, pkgs, 1, "Expected exactly one package")

	pkg := pkgs[0]

	for _, file := range pkg.Syntax {
		ast.Inspect(file, func(n ast.Node) bool {
			if fn, ok := n.(*ast.FuncDecl); ok {
				// Check if function name starts with "New" and ends with "Error"
				if strings.HasPrefix(fn.Name.Name, "New") && strings.HasSuffix(fn.Name.Name, "Error") {
					// Check return type
					if fn.Type.Results != nil && len(fn.Type.Results.List) > 0 {
						returnType := fn.Type.Results.List[0].Type
						// If it's not a pointer (*Ident), it's a violation
						if _, isPointer := returnType.(*ast.StarExpr); !isPointer {
							// Although we are ok if the return type is error
							if ident, isIdent := returnType.(*ast.Ident); !(isIdent && ident.Name == "error") {
								pos := pkg.Fset.Position(fn.Pos())
								assert.Fail(
									t, "Error constructor should return pointer",
									"%s:%s should return *%s",
									pos.Filename, fn.Name.Name, strings.TrimPrefix(fn.Name.Name, "New"))
							}
						}
					}
				}
			}
			return true
		})
	}
}

// For consistency/convention, tests that all error types implement Error() method with pointer receiver
func TestErrorMethodsShouldHavePointerReceivers(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  ".",
	}

	pkgs, err := packages.Load(cfg, ".")
	require.NoError(t, err)
	require.Len(t, pkgs, 1, "Expected exactly one package")

	pkg := pkgs[0]

	// Track error type names (structs that end with "Error")
	errorTypes := make(map[string]bool)
	for _, file := range pkg.Syntax {
		ast.Inspect(file, func(n ast.Node) bool {
			// Find all struct types that end with "Error"
			if typeSpec, ok := n.(*ast.TypeSpec); ok {
				if _, isStruct := typeSpec.Type.(*ast.StructType); isStruct {
					if strings.HasSuffix(typeSpec.Name.Name, "Error") {
						errorTypes[typeSpec.Name.Name] = true
					}
				}
			}
			return true
		})
	}

	for _, file := range pkg.Syntax {
		ast.Inspect(file, func(n ast.Node) bool {
			if fn, ok := n.(*ast.FuncDecl); ok {
				// Look for receiver methods
				if fn.Recv != nil && len(fn.Recv.List) > 0 {
					recvType := fn.Recv.List[0].Type
					// Fail if found value receivers in error types
					if recv, ok := recvType.(*ast.Ident); ok && errorTypes[recv.Name] {
						pos := pkg.Fset.Position(fn.Pos())
						assert.Fail(
							t, "Error() method should have pointer receiver",
							"%s: func (%s) Error() should be func (*%s) Error()",
							pos.Filename, recv.Name, recv.Name)
					}
				}
			}
			return true
		})
	}
}
