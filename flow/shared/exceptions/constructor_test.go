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
