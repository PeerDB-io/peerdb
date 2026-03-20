package mongodb

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ResultKind indicates whether a command returns a cursor or a scalar value.
type ResultKind int

const (
	resultScalar ResultKind = iota
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
	SwitchDB    string     // if non-empty, switch to this database
	AdminDB     bool       // if true, run against admin database
}

//nolint:govet // fieldalignment: readability preferred
type compileError struct {
	Kind   errorKind
	Detail string
	Hint   string
}

type errorKind int

const (
	errParse errorKind = iota
	errDeniedCommand
	errInvalidJSON
)

func (e errorKind) String() string {
	switch e {
	case errParse:
		return "Parse"
	case errDeniedCommand:
		return "DeniedCommand"
	case errInvalidJSON:
		return "InvalidJSON"
	default:
		return "Unknown"
	}
}

func newCompileError(kind errorKind, detail string) *compileError {
	return &compileError{Kind: kind, Detail: detail}
}

func (e *compileError) Error() string {
	msg := fmt.Sprintf("%s: %s", e.Kind, e.Detail)
	if e.Hint != "" {
		msg = fmt.Sprintf("%s; %s", msg, e.Hint)
	}
	return msg
}

func (e *compileError) withHint(hint string) *compileError {
	e.Hint = hint
	return e
}

var helpRe = regexp.MustCompile(`^help\s*;?\s*$`)
var useRe = regexp.MustCompile(`^use\s+(\w[\w.-]*)\s*;?\s*$`)

// Compile parses and validates a MongoDB wire command from Extended JSON input.
func Compile(input string) (ExecSpec, error) {
	input = strings.TrimSpace(input)

	if input == "" {
		return ExecSpec{}, newCompileError(errParse, "empty input")
	}

	if helpRe.MatchString(input) {
		cols, rows := globalHelp()
		return ExecSpec{HelpColumns: cols, HelpRows: rows}, nil
	}

	if m := useRe.FindStringSubmatch(input); m != nil {
		return ExecSpec{SwitchDB: m[1]}, nil
	}

	input = strings.TrimRight(input, "; \t\n\r")

	var cmd bson.D
	if err := bson.UnmarshalExtJSON([]byte(input), false, &cmd); err != nil {
		return ExecSpec{}, newCompileError(errInvalidJSON, err.Error()).
			withHint(`input must be a JSON object like {"find": "collection", "filter": {}}`)
	}

	if len(cmd) == 0 {
		return ExecSpec{}, newCompileError(errParse, "empty command")
	}

	if err := validateCommand(cmd); err != nil {
		return ExecSpec{}, newCompileError(errDeniedCommand, err.Error())
	}

	spec, _ := lookupCommand(cmd[0].Key)

	resultKind := resultScalar
	if spec.ReturnsCursor {
		resultKind = ResultCursor
	}

	return ExecSpec{
		Command:    cmd,
		ResultKind: resultKind,
		AdminDB:    spec.AdminDB,
	}, nil
}

const docsBaseURL = "https://www.mongodb.com/docs/manual/reference"

func globalHelp() ([]string, [][]string) {
	rows := [][]string{
		{"Input Format:", ""},
		{`  Extended JSON wire commands`, `e.g. {"find": "coll", "filter": {}}`},
		{"", ""},
		{"Shell Commands:", ""},
		{"  use <dbname>", "switch the current database"},
		{"", ""},
		{"Allowed Wire Commands:", ""},
	}

	sections := make([]string, 0, len(allowedCommands))
	for section := range allowedCommands {
		sections = append(sections, section)
	}
	sort.Strings(sections)

	for i, section := range sections {
		if i > 0 {
			rows = append(rows, []string{"", ""})
		}
		rows = append(rows, []string{"  " + section + ":", ""})
		for _, cmd := range allowedCommands[section] {
			rows = append(rows, []string{"    " + cmd.Name, docsBaseURL + "/command/" + cmd.Name + "/"})
		}
	}
	return []string{"command", "docs"}, rows
}
