package main

import (
	"go/ast"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"
)

// TestSlogContextUsage verifies that slog calls use context-aware methods or struct loggers
// instead of the global slog functions. This ensures consistent and traceable logging.
func TestSlogContextUsage(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes,
		Dir:  ".", // Start from flow directory
	}

	// Load all packages under the flow directory
	pkgs, err := packages.Load(cfg, "./...")
	require.NoError(t, err)

	t.Logf("Loaded %d packages", len(pkgs))
	for i, pkg := range pkgs {
		t.Logf("Package %d: ID=%s, PkgPath=%s, Name=%s, Files=%d",
			i, pkg.ID, pkg.PkgPath, pkg.Name, len(pkg.Syntax))
		if len(pkg.Errors) > 0 {
			t.Logf("  Errors: %v", pkg.Errors)
		}
	}

	var violations []string

	for _, pkg := range pkgs {
		// Skip test files and generated files
		if strings.Contains(pkg.PkgPath, "test") ||
			strings.Contains(pkg.PkgPath, "generated") ||
			strings.HasSuffix(pkg.PkgPath, "_test") {
			t.Logf("Skipping test or generated package: %s", pkg.PkgPath)
			continue
		}

		t.Logf("Analyzing package: %s (%d files)", pkg.PkgPath, len(pkg.Syntax))

		for _, file := range pkg.Syntax {
			filePos := pkg.Fset.Position(file.Pos())
			// Skip test files and generated files by filename
			if strings.HasSuffix(filePos.Filename, "_test.go") ||
				strings.Contains(filePos.Filename, "generated") ||
				strings.Contains(filePos.Filename, ".pb.go") {
				t.Logf("Skipping test or generated file: %s", filePos.Filename)
				continue
			}

			t.Logf("Analyzing file: %s", filePos.Filename)
			fileViolations := analyzeSlogUsage(t, pkg, file)
			t.Logf("Found %d violations in %s", len(fileViolations), filePos.Filename)
			violations = append(violations, fileViolations...)
		}
	}

	if len(violations) > 0 {
		t.Errorf("Found %d improper slog usage violations:\n%s",
			len(violations), strings.Join(violations, "\n"))
	} else {
		t.Logf("No slog violations found!")
	}
}

// analyzeSlogUsage analyzes a file for improper slog usage
func analyzeSlogUsage(t *testing.T, pkg *packages.Package, file *ast.File) []string {
	t.Helper()
	var violations []string

	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			if violation := checkSlogCall(pkg, node); violation != "" {
				violations = append(violations, violation)
			}
		}
		return true
	})

	return violations
}

// checkSlogCall checks if a function call is an improper slog usage
func checkSlogCall(pkg *packages.Package, call *ast.CallExpr) string {
	// Check if it's a selector expression (e.g., slog.Info)
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}

	// Check if it's calling slog package
	ident, ok := sel.X.(*ast.Ident)
	if !ok || ident.Name != "slog" {
		return ""
	}

	// Check if it's one of the methods we want to replace
	methodName := sel.Sel.Name
	var contextMethod string
	switch methodName {
	case "Info":
		contextMethod = "InfoContext"
	case "Warn":
		contextMethod = "WarnContext"
	case "Error":
		contextMethod = "ErrorContext"
	case "Debug":
		contextMethod = "DebugContext"
	default:
		return "" // Not a method we're checking
	}

	pos := pkg.Fset.Position(call.Pos())

	// No context or struct logger available
	return formatLoggingViolation(pos, methodName, "slog."+contextMethod+"(ctx, ...) instead or re-use the logger on current struct (if available)")
}

// formatLoggingViolation formats a logging violation message
func formatLoggingViolation(pos token.Position, currentMethod, suggestion string) string {
	return pos.String() + ": slog." + currentMethod + " should be replaced with " + suggestion
}
