package mongodb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompile(t *testing.T) {
	//nolint:govet // fieldalignment: test code, readability preferred
	tests := []struct {
		name         string
		input        string
		wantKind     ResultKind
		wantAdminDB  bool
		wantHelp     bool
		wantSwitchDB string
		wantErr      bool
		wantErrKind  errorKind
		wantErrMsg   string
	}{
		// Result paths
		{name: "cursor", input: `{"find": "coll", "filter": {}}`, wantKind: ResultCursor},
		{name: "scalar", input: `{"ping": 1}`, wantKind: resultScalar},
		{name: "adminDB", input: `{"listDatabases": 1}`, wantKind: resultScalar, wantAdminDB: true},
		{name: "trailingSemicolon", input: `{"ping": 1};`, wantKind: resultScalar},
		{name: "help", input: "show help", wantHelp: true},
		{name: "help/semicolon", input: "show help;", wantHelp: true},
		{name: "help/spaces", input: "  show  help  ", wantHelp: true},
		{name: "useDB", input: "use mydb", wantSwitchDB: "mydb"},
		{name: "useDB/semicolon", input: "use mydb;", wantSwitchDB: "mydb"},
		{name: "useDB/spaces", input: "  use  mydb  ", wantSwitchDB: "mydb"},

		// Errors
		{name: "error/empty", input: "", wantErr: true, wantErrKind: errParse},
		{name: "error/invalidJSON", input: `{invalid`, wantErr: true, wantErrKind: errInvalidJSON},
		{name: "error/notObject", input: `"hello"`, wantErr: true, wantErrKind: errInvalidJSON},
		{name: "error/emptyObject", input: `{}`, wantErr: true, wantErrKind: errParse, wantErrMsg: "empty command"},
		{name: "error/denied", input: `{"insert": "users"}`, wantErr: true, wantErrKind: errDeniedCommand},
		{name: "error/unknown", input: `{"fakeCommand": 1}`, wantErr: true, wantErrKind: errDeniedCommand},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compile(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				ce, ok := err.(*compileError)
				require.True(t, ok, "expected *compileError, got %T", err)
				require.Equal(t, tt.wantErrKind, ce.Kind)
				if tt.wantErrMsg != "" {
					require.Contains(t, ce.Error(), tt.wantErrMsg)
				}
				return
			}

			require.NoError(t, err)

			if tt.wantHelp {
				require.NotEmpty(t, got.HelpRows)
				return
			}

			if tt.wantSwitchDB != "" {
				require.Equal(t, tt.wantSwitchDB, got.SwitchDB)
				return
			}

			require.NotEmpty(t, got.Command)
			require.Equal(t, tt.wantKind, got.ResultKind)
			require.Equal(t, tt.wantAdminDB, got.AdminDB)
		})
	}
}

func flattenRows(rows [][]string) string {
	var parts []string
	for _, row := range rows {
		parts = append(parts, strings.Join(row, " "))
	}
	return strings.Join(parts, "\n")
}

func TestGlobalHelp(t *testing.T) {
	cols, rows := globalHelp()
	if cols[0] != "command" || cols[1] != "docs" {
		t.Errorf("expected [command docs], got %v", cols)
	}
	help := flattenRows(rows)
	wants := []string{
		"Allowed Wire Commands:", "Input Format:", "Shell Commands:", "use <dbname>",
		"ping", "find", "/reference/command/", "listCollections", "listDatabases",
	}
	for _, want := range wants {
		if !strings.Contains(help, want) {
			t.Errorf("missing %q in global help", want)
		}
	}
}

func TestPrintAllHelp(t *testing.T) {
	cols, rows := globalHelp()

	t.Logf("\n=== help() ===")
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	header := make([]string, len(cols))
	sep := make([]string, len(cols))
	for i, c := range cols {
		header[i] = fmt.Sprintf(" %-*s ", widths[i], c)
		sep[i] = strings.Repeat("-", widths[i]+2)
	}
	t.Log(strings.Join(header, "|"))
	t.Log(strings.Join(sep, "+"))

	for _, row := range rows {
		cells := make([]string, len(cols))
		for i := range cols {
			v := ""
			if i < len(row) {
				v = row[i]
			}
			cells[i] = fmt.Sprintf(" %-*s ", widths[i], v)
		}
		t.Log(strings.Join(cells, "|"))
	}
}
