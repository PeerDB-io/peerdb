// Package parser provides a MongoDB shell parser that compiles mongosh-style
// statements into Go-ready BSON command documents for use with the MongoDB Go driver.
package mongosh

import (
	"fmt"
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/pgwire/mongosh/command"
	"github.com/PeerDB-io/peerdb/flow/pgwire/mongosh/parser"
)

// ResultKind indicates whether a command returns a cursor or a scalar value.
type ResultKind = command.ResultKind

const (
	ResultScalar = command.ResultScalar // command returns a single value
	ResultCursor = command.ResultCursor // command returns a cursor
)

// ExecHints provides optional execution hints derived from shell sugar.
type ExecHints = command.ExecHints

// Namespace represents the database and collection parsed from the shell statement.
type Namespace struct {
	DB         string
	Collection string
}

// ExecSpec represents the compiled output of a MongoDB shell statement.
//
//nolint:govet // fieldalignment: readability preferred
type ExecSpec struct {
	Command    bson.D     // normalized command document
	ResultKind ResultKind // whether result is Cursor or Scalar
	Hints      ExecHints  // optional execution hints
	Namespace  Namespace  // parsed database and collection
	HelpText   string     // populated for help() requests
}

// ErrorKind represents the type of compilation error.
type ErrorKind int

const (
	ErrUnsupportedSyntax  ErrorKind = iota // unsupported shell syntax
	ErrParse                               // general parsing error
	ErrInvalidLiteral                      // invalid BSON literal
	ErrDeniedCommand                       // command not allowed
	ErrMultipleStatements                  // multiple statements provided
	ErrTrailingContent                     // unexpected content after statement
	ErrInvalidJSON                         // invalid JSON in arguments
)

func (e ErrorKind) String() string {
	switch e {
	case ErrUnsupportedSyntax:
		return "UnsupportedSyntax"
	case ErrParse:
		return "Parse"
	case ErrInvalidLiteral:
		return "InvalidLiteral"
	case ErrDeniedCommand:
		return "DeniedCommand"
	case ErrMultipleStatements:
		return "MultipleStatements"
	case ErrTrailingContent:
		return "TrailingContent"
	case ErrInvalidJSON:
		return "InvalidJSON"
	default:
		return "Unknown"
	}
}

// CompileError represents a rich compilation error with context.
//
//nolint:govet // fieldalignment: readability preferred
type CompileError struct {
	Kind     ErrorKind
	Detail   string
	Position int
	Hint     string
}

func NewCompileError(kind ErrorKind, detail string) *CompileError {
	return &CompileError{Kind: kind, Detail: detail}
}

func (e *CompileError) Error() string {
	msg := fmt.Sprintf("%s: %s", e.Kind, e.Detail)
	if e.Position > 0 {
		msg = fmt.Sprintf("%s (at position %d)", msg, e.Position)
	}
	if e.Hint != "" {
		msg = fmt.Sprintf("%s; %s", msg, e.Hint)
	}
	return msg
}

func (e *CompileError) WithPosition(pos int) *CompileError {
	e.Position = pos
	return e
}

func (e *CompileError) WithHint(hint string) *CompileError {
	e.Hint = hint
	return e
}

// Shell command patterns
var (
	showCollectionsRe = regexp.MustCompile(`^show\s+collections\s*;?\s*$`)
	showDatabasesRe   = regexp.MustCompile(`^show\s+(databases|dbs)\s*;?\s*$`)
	helpGlobalRe      = regexp.MustCompile(`^help\s*\(\s*\)\s*;?\s*$`)
)

func tryShellCommand(input string) (ExecSpec, bool) {
	if showCollectionsRe.MatchString(input) {
		return ExecSpec{
			Command: bson.D{
				{Key: "listCollections", Value: 1},
				{Key: "nameOnly", Value: true},
				{Key: "authorizedCollections", Value: true},
			},
			ResultKind: ResultCursor,
			Namespace:  Namespace{DB: "test"},
		}, true
	}

	if showDatabasesRe.MatchString(input) {
		return ExecSpec{
			Command: bson.D{
				{Key: "listDatabases", Value: 1},
				{Key: "nameOnly", Value: true},
				{Key: "authorizedDatabases", Value: true},
			},
			ResultKind: ResultScalar,
			Namespace:  Namespace{DB: "admin"},
		}, true
	}

	if helpGlobalRe.MatchString(input) {
		return ExecSpec{HelpText: command.GlobalHelp()}, true
	}

	return ExecSpec{}, false
}

// Compile parses and compiles a single mongosh-style statement into an ExecSpec.
func Compile(input string) (ExecSpec, error) {
	input = strings.TrimSpace(input)

	if input == "" {
		return ExecSpec{}, NewCompileError(ErrParse, "empty input")
	}

	if spec, ok := tryShellCommand(input); ok {
		return spec, nil
	}

	if err := command.ValidateInput(input); err != nil {
		return ExecSpec{}, NewCompileError(ErrParse, err.Error())
	}

	shellStmt, err := parser.ParseStatement(input)
	if err != nil {
		return ExecSpec{}, NewCompileError(ErrParse, err.Error()).
			WithHint("check syntax; supported: db.collection.find(), db.collection.aggregate(), etc.")
	}

	if shellStmt.IsHelp {
		return ExecSpec{HelpText: helpFor(shellStmt.HelpContext)}, nil
	}

	if err := validateStatement(shellStmt); err != nil {
		return ExecSpec{}, NewCompileError(ErrDeniedCommand, err.Error()).
			WithHint("check supported methods in registry")
	}

	cmd, hints, resultKind, err := buildCommandWithRegistry(shellStmt)
	if err != nil {
		return ExecSpec{}, NewCompileError(ErrParse, err.Error())
	}

	if err := command.ValidateCommand(cmd); err != nil {
		return ExecSpec{}, NewCompileError(ErrDeniedCommand, err.Error())
	}

	db := shellStmt.Database
	if db == "" {
		db = "test"
	}

	return ExecSpec{
		Command:    cmd,
		ResultKind: resultKind,
		Hints:      hints,
		Namespace:  Namespace{DB: db, Collection: shellStmt.Collection},
	}, nil
}

func helpFor(context string) string {
	switch context {
	case "database":
		return command.DatabaseHelp()
	case "collection":
		return command.CollectionHelp()
	default:
		return command.MethodHelp(context)
	}
}

// validateStatement validates a statement against the registry.
func validateStatement(stmt *parser.Statement) error {
	methodName := strings.ToLower(stmt.Method)
	spec, ok := command.Registry[methodName]
	if !ok {
		return fmt.Errorf("unsupported method: %s", stmt.Method)
	}

	for _, chainer := range stmt.Chainers {
		chainerName := strings.ToLower(chainer.Name)
		if _, ok := spec.Chainers[chainerName]; !ok {
			return fmt.Errorf("unsupported chainer .%s() for method %s", chainer.Name, stmt.Method)
		}
	}

	return nil
}

func buildCommandWithRegistry(stmt *parser.Statement) (bson.D, command.ExecHints, command.ResultKind, error) {
	methodName := strings.ToLower(stmt.Method)

	spec, ok := command.Registry[methodName]
	if !ok {
		return nil, command.ExecHints{}, command.ResultScalar,
			fmt.Errorf("unsupported method: %s", stmt.Method)
	}

	parsedArgs, err := command.ParseArgsAccordingTo(spec.Args, stmt.Arguments)
	if err != nil {
		return nil, command.ExecHints{}, command.ResultScalar,
			fmt.Errorf("method %s: %w", stmt.Method, err)
	}

	cmd, hints, kind, err := spec.Build(stmt.Collection, parsedArgs)
	if err != nil {
		return nil, hints, kind, err
	}

	for _, chainer := range stmt.Chainers {
		chainerName := strings.ToLower(chainer.Name)

		chainerSpec, ok := spec.Chainers[chainerName]
		if !ok {
			return nil, hints, kind,
				fmt.Errorf("unsupported chainer .%s() for method %s", chainer.Name, stmt.Method)
		}

		chainerArgs, err := command.ParseArgsAccordingTo(chainerSpec.Args, chainer.Arguments)
		if err != nil {
			return nil, hints, kind, fmt.Errorf(".%s(): %w", chainer.Name, err)
		}

		cmd, err = chainerSpec.Apply(cmd, &hints, chainerArgs)
		if err != nil {
			return nil, hints, kind, fmt.Errorf(".%s(): %w", chainer.Name, err)
		}
	}

	if stmt.IsExplain {
		cmd = command.WrapExplain(cmd, stmt.ExplainVerbosity)
		kind = command.ResultScalar
	}

	return cmd, hints, kind, nil
}
