package mongosh

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompile(t *testing.T) {
	//nolint:govet // fieldalignment: test code, readability preferred
	tests := []struct {
		name        string
		input       string
		wantKind    ResultKind
		wantAdminDB bool
		wantHelp    bool
		wantErr     bool
		wantErrKind ErrorKind
		wantErrMsg  string
	}{
		// Result paths
		{name: "cursor", input: `{"find": "coll", "filter": {}}`, wantKind: ResultCursor},
		{name: "scalar", input: `{"ping": 1}`, wantKind: ResultScalar},
		{name: "adminDB", input: `{"listDatabases": 1}`, wantKind: ResultScalar, wantAdminDB: true},
		{name: "trailingSemicolon", input: `{"ping": 1};`, wantKind: ResultScalar},
		{name: "help", input: "help", wantHelp: true},

		// Errors
		{name: "error/empty", input: "", wantErr: true, wantErrKind: ErrParse},
		{name: "error/invalidJSON", input: `{invalid`, wantErr: true, wantErrKind: ErrInvalidJSON},
		{name: "error/notObject", input: `"hello"`, wantErr: true, wantErrKind: ErrInvalidJSON},
		{name: "error/emptyObject", input: `{}`, wantErr: true, wantErrKind: ErrParse, wantErrMsg: "empty command"},
		{name: "error/denied", input: `{"insert": "users"}`, wantErr: true, wantErrKind: ErrDeniedCommand},
		{name: "error/unknown", input: `{"fakeCommand": 1}`, wantErr: true, wantErrKind: ErrDeniedCommand},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compile(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				ce, ok := err.(*CompileError)
				require.True(t, ok, "expected *CompileError, got %T", err)
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

			require.NotEmpty(t, got.Command)
			require.Equal(t, tt.wantKind, got.ResultKind)
			require.Equal(t, tt.wantAdminDB, got.AdminDB)
		})
	}
}
