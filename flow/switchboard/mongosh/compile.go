package mongosh

import (
	"fmt"
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/switchboard/mongosh/command"
)

// ResultKind indicates whether a command returns a cursor or a scalar value.
type ResultKind int

const (
	ResultScalar ResultKind = iota
	ResultCursor
)

// ExecSpec represents the compiled output of a MongoDB wire command.
//
//nolint:govet // fieldalignment: readability preferred
type ExecSpec struct {
	Command     bson.D     // wire command document
	ResultKind  ResultKind // whether result is Cursor or Scalar
	HelpColumns []string   // column names for help output
	HelpRows    [][]string // row data for help output
	AdminDB     bool       // if true, run against admin database
}

// CompileError represents a compilation error with context.
//
//nolint:govet // fieldalignment: readability preferred
type CompileError struct {
	Kind   ErrorKind
	Detail string
	Hint   string
}

// ErrorKind represents the type of compilation error.
type ErrorKind int

const (
	ErrParse         ErrorKind = iota // general parsing error
	ErrDeniedCommand                  // command not allowed
	ErrInvalidJSON                    // invalid JSON input
)

func (e ErrorKind) String() string {
	switch e {
	case ErrParse:
		return "Parse"
	case ErrDeniedCommand:
		return "DeniedCommand"
	case ErrInvalidJSON:
		return "InvalidJSON"
	default:
		return "Unknown"
	}
}

func NewCompileError(kind ErrorKind, detail string) *CompileError {
	return &CompileError{Kind: kind, Detail: detail}
}

func (e *CompileError) Error() string {
	msg := fmt.Sprintf("%s: %s", e.Kind, e.Detail)
	if e.Hint != "" {
		msg = fmt.Sprintf("%s; %s", msg, e.Hint)
	}
	return msg
}

func (e *CompileError) WithHint(hint string) *CompileError {
	e.Hint = hint
	return e
}

var helpRe = regexp.MustCompile(`^help\s*;?\s*$`)

// Compile parses and validates a MongoDB wire command from Extended JSON input.
func Compile(input string) (ExecSpec, error) {
	input = strings.TrimSpace(input)

	if input == "" {
		return ExecSpec{}, NewCompileError(ErrParse, "empty input")
	}

	if helpRe.MatchString(input) {
		cols, rows := command.GlobalHelp()
		return ExecSpec{HelpColumns: cols, HelpRows: rows}, nil
	}

	input = strings.TrimRight(input, "; \t\n\r")

	var cmd bson.D
	if err := bson.UnmarshalExtJSON([]byte(input), false, &cmd); err != nil {
		return ExecSpec{}, NewCompileError(ErrInvalidJSON, err.Error()).
			WithHint(`input must be a JSON object like {"find": "collection", "filter": {}}`)
	}

	if len(cmd) == 0 {
		return ExecSpec{}, NewCompileError(ErrParse, "empty command")
	}

	if err := command.ValidateCommand(cmd); err != nil {
		return ExecSpec{}, NewCompileError(ErrDeniedCommand, err.Error())
	}

	spec, _ := command.LookupCommand(cmd[0].Key)

	resultKind := ResultScalar
	if spec.ReturnsCursor {
		resultKind = ResultCursor
	}

	return ExecSpec{
		Command:    cmd,
		ResultKind: resultKind,
		AdminDB:    spec.AdminDB,
	}, nil
}
