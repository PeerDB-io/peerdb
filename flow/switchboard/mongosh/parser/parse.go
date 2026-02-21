package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dop251/goja/ast"
	"github.com/dop251/goja/parser"
)

// ParseStatement parses a MongoDB shell statement using goja/parser
func ParseStatement(source string) (*Statement, error) {
	// Parse the JavaScript source
	prog, err := parser.ParseFile(nil, "", source, 0, parser.WithDisableSourceMaps)
	if err != nil {
		return nil, fmt.Errorf("parse error: %v", err)
	}

	// Validate we have exactly one statement
	if len(prog.Body) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(prog.Body))
	}

	// Must be an expression statement
	exprStmt, ok := prog.Body[0].(*ast.ExpressionStatement)
	if !ok {
		return nil, fmt.Errorf("expected expression statement, got %T", prog.Body[0])
	}

	// Extract the call chain
	return extractStatement(exprStmt.Expression)
}

// extractStatement extracts a MongoDB statement from an AST expression
func extractStatement(expr ast.Expression) (*Statement, error) {
	// Extract the call chain
	head, calls, err := extractCallChain(expr)
	if err != nil {
		return nil, err
	}

	// Validate the head starts with "db"
	if len(head) == 0 || head[0] != "db" {
		return nil, errors.New("statement must start with 'db'")
	}

	// Build the statement based on the structure
	stmt := &Statement{
		Database: "test", // default database
	}

	// Check for explain() wrapper: db.collection.explain().method()
	// In this case, calls[0] is "explain" and calls[1] is the actual method
	if len(calls) >= 2 && strings.EqualFold(calls[0].name, "explain") {
		stmt.IsExplain = true
		stmt.ExplainVerbosity = "queryPlanner" // default
		if len(calls[0].args) > 0 {
			if verbosity, err := evalLiteral(calls[0].args[0]); err == nil {
				if v, ok := verbosity.(string); ok {
					stmt.ExplainVerbosity = v
				}
			}
		}
		// Shift: remove explain from calls, the real method is next
		calls = calls[1:]
	}

	// Determine if this is a database or collection method
	if len(calls) == 0 {
		return nil, errors.New("expected method call")
	}

	// Check the structure based on head length
	if len(head) == 1 && head[0] == "db" {
		// Just "db" with a method call directly
		// This is db.method() - database method (like db.runCommand)
		if len(calls) > 0 && calls[0].name != "" {
			stmt.Type = "database"
			stmt.Method = calls[0].name
			stmt.Arguments, err = evaluateArguments(calls[0].args)
			if err != nil {
				return nil, err
			}

			// Database methods shouldn't have chainers
			if len(calls) > 1 {
				return nil, fmt.Errorf("database method %s does not support chainers", stmt.Method)
			}
		} else {
			return nil, errors.New("invalid statement structure: missing method")
		}
	} else if len(head) == 2 {
		// head is [db, X] where X could be:
		// - a collection name (e.g., users in db.users.find())
		// - a method that was called directly (e.g., runCommand in db.runCommand())

		// For db.runCommand(), the parser gives us head=[db, runCommand] and calls=[{args}]
		// For db.users.find(), the parser gives us head=[db, users] and calls=[{name:"find", args}]

		// Check if the first call has a name
		if len(calls) > 0 && calls[0].name != "" {
			// This is db.collection.method() - collection method
			stmt.Type = "collection"
			stmt.Collection = head[1]
			stmt.Method = calls[0].name
			stmt.Arguments, err = evaluateArguments(calls[0].args)
			if err != nil {
				return nil, err
			}

			// Process chainers (remaining calls)
			if len(calls) > 1 {
				stmt.Chainers = make([]Chainer, 0, len(calls)-1)
				for i := 1; i < len(calls); i++ {
					args, err := evaluateArguments(calls[i].args)
					if err != nil {
						return nil, err
					}
					stmt.Chainers = append(stmt.Chainers, Chainer{
						Name:      calls[i].name,
						Arguments: args,
					})
				}
			}
		} else {
			// The call has no name, which means head[1] is the method being called
			// This is db.method() - database method
			stmt.Type = "database"
			stmt.Method = head[1]
			stmt.Arguments, err = evaluateArguments(calls[0].args)
			if err != nil {
				return nil, err
			}

			// Database methods shouldn't have chainers
			if len(calls) > 1 {
				return nil, fmt.Errorf("database method %s does not support chainers", stmt.Method)
			}
		}
	} else if len(head) == 3 {
		// db.collection.method as head, with potential chainers
		// This means the method name was part of the head path
		// But in the new parser, methods are always calls
		// So this case might not occur with proper parsing
		// Let's handle it anyway for robustness
		stmt.Type = "collection"
		stmt.Collection = head[1]
		stmt.Method = head[2]

		// The first call might actually be the arguments for the method
		// Or it could be a chainer if method had no parens
		if len(calls) > 0 && calls[0].name == "" {
			// Method arguments
			stmt.Arguments, err = evaluateArguments(calls[0].args)
			if err != nil {
				return nil, err
			}
			// Chainers are the rest
			if len(calls) > 1 {
				stmt.Chainers = make([]Chainer, 0, len(calls)-1)
				for i := 1; i < len(calls); i++ {
					args, err := evaluateArguments(calls[i].args)
					if err != nil {
						return nil, err
					}
					stmt.Chainers = append(stmt.Chainers, Chainer{
						Name:      calls[i].name,
						Arguments: args,
					})
				}
			}
		} else {
			// No method arguments, all are chainers
			stmt.Arguments = []string{}
			stmt.Chainers = make([]Chainer, 0, len(calls))
			for _, c := range calls {
				args, err := evaluateArguments(c.args)
				if err != nil {
					return nil, err
				}
				stmt.Chainers = append(stmt.Chainers, Chainer{
					Name:      c.name,
					Arguments: args,
				})
			}
		}
	} else {
		return nil, errors.New("invalid statement structure")
	}

	// Normalize method names to lowercase
	stmt.Method = strings.ToLower(stmt.Method)
	for i := range stmt.Chainers {
		stmt.Chainers[i].Name = strings.ToLower(stmt.Chainers[i].Name)
	}

	// Check for help() calls
	if stmt.Method == "help" {
		stmt.IsHelp = true
		if stmt.Type == "database" {
			stmt.HelpContext = "database"
		} else {
			stmt.HelpContext = "collection"
		}
		return stmt, nil
	}

	// Check for help() as a chainer (e.g., db.coll.find().help())
	if len(stmt.Chainers) > 0 && stmt.Chainers[len(stmt.Chainers)-1].Name == "help" {
		stmt.IsHelp = true
		stmt.HelpContext = stmt.Method // e.g., "find", "aggregate"
		stmt.Chainers = stmt.Chainers[:len(stmt.Chainers)-1]
		return stmt, nil
	}

	return stmt, nil
}

// call represents a method call with its name and arguments
type call struct {
	name string
	args []ast.Expression
}

// extractCallChain extracts the call chain from an expression
func extractCallChain(expr ast.Expression) ([]string, []call, error) {
	// First, handle the chained method calls (working backwards)
	var calls []call

	// Keep extracting call expressions from the right
	for {
		callExpr, ok := expr.(*ast.CallExpression)
		if !ok {
			break
		}

		// Add this call to our list
		currentCall := call{
			args: callExpr.ArgumentList,
		}

		// Get the method name from the callee
		switch callee := callExpr.Callee.(type) {
		case *ast.DotExpression:
			currentCall.name = string(callee.Identifier.Name)
			expr = callee.Left
		case *ast.Identifier:
			currentCall.name = string(callee.Name)
			expr = nil
		default:
			return nil, nil, fmt.Errorf("unexpected callee type: %T", callee)
		}

		// Prepend to maintain order
		calls = append([]call{currentCall}, calls...)
	}

	// Now extract the head path (db.collection.method or db.method)
	var head []string
	for expr != nil {
		switch e := expr.(type) {
		case *ast.DotExpression:
			head = append([]string{string(e.Identifier.Name)}, head...)
			expr = e.Left

		case *ast.BracketExpression:
			key, err := evalLiteral(e.Member)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot evaluate bracket expression: %w", err)
			}
			keyStr, ok := key.(string)
			if !ok {
				return nil, nil, errors.New("bracket expression must evaluate to string")
			}
			head = append([]string{keyStr}, head...)
			expr = e.Left

		case *ast.Identifier:
			head = append([]string{string(e.Name)}, head...)
			expr = nil

		default:
			return nil, nil, fmt.Errorf("unexpected expression type: %T", e)
		}
	}

	return head, calls, nil
}

// evaluateArguments evaluates a list of AST expressions to strings
func evaluateArguments(exprs []ast.Expression) ([]string, error) {
	if len(exprs) == 0 {
		return []string{}, nil
	}

	args := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		// Convert the expression to JSON string representation
		jsonStr, err := exprToJSON(expr)
		if err != nil {
			return nil, err
		}
		args = append(args, jsonStr)
	}
	return args, nil
}
