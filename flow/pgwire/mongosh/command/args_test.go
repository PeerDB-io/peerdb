package command

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseArgsAccordingTo(t *testing.T) {
	tests := []struct {
		name    string
		kinds   []ArgKind
		rawArgs []string
		wantLen int
		wantErr bool
	}{
		{
			name:    "single JSON arg",
			kinds:   []ArgKind{ArgJSON},
			rawArgs: []string{`{"name": "test"}`},
			wantLen: 1,
		},
		{
			name:    "string arg",
			kinds:   []ArgKind{ArgString},
			rawArgs: []string{`"email"`},
			wantLen: 1,
		},
		{
			name:    "int arg",
			kinds:   []ArgKind{ArgInt},
			rawArgs: []string{"10"},
			wantLen: 1,
		},
		{
			name:    "bool arg",
			kinds:   []ArgKind{ArgBool},
			rawArgs: []string{"true"},
			wantLen: 1,
		},
		{
			name:    "optional arg present",
			kinds:   []ArgKind{ArgJSON, ArgOptional, ArgJSON},
			rawArgs: []string{`{"filter": 1}`, `{"projection": 1}`},
			wantLen: 2,
		},
		{
			name:    "optional arg missing",
			kinds:   []ArgKind{ArgJSON, ArgOptional, ArgJSON},
			rawArgs: []string{`{"filter": 1}`},
			wantLen: 2, // Second will be nil
		},
		{
			name:    "missing required arg",
			kinds:   []ArgKind{ArgJSON, ArgJSON},
			rawArgs: []string{`{"filter": 1}`},
			wantErr: true,
		},
		{
			name:    "invalid int",
			kinds:   []ArgKind{ArgInt},
			rawArgs: []string{"not_a_number"},
			wantErr: true,
		},
		{
			name:    "invalid bool",
			kinds:   []ArgKind{ArgBool},
			rawArgs: []string{"yes"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseArgsAccordingTo(tt.kinds, tt.rawArgs)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, tt.wantLen)
		})
	}
}

func TestParseArgument(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		kind    ArgKind
		wantNil bool
		wantErr bool
	}{
		{
			name: "valid JSON object",
			raw:  `{"key": "value"}`,
			kind: ArgJSON,
		},
		{
			name: "valid JSON array",
			raw:  `[1, 2, 3]`,
			kind: ArgJSON,
		},
		{
			name: "quoted string",
			raw:  `"test"`,
			kind: ArgString,
		},
		{
			name: "unquoted string",
			raw:  `test`,
			kind: ArgString,
		},
		{
			name: "valid int",
			raw:  "42",
			kind: ArgInt,
		},
		{
			name: "valid bool true",
			raw:  "true",
			kind: ArgBool,
		},
		{
			name: "valid bool false",
			raw:  "false",
			kind: ArgBool,
		},
		{
			name:    "invalid JSON",
			raw:     `{bad json`,
			kind:    ArgJSON,
			wantErr: true,
		},
		{
			name:    "invalid int",
			raw:     "abc",
			kind:    ArgInt,
			wantErr: true,
		},
		{
			name:    "invalid bool",
			raw:     "maybe",
			kind:    ArgBool,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgument(tt.raw, tt.kind)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.wantNil {
				require.Nil(t, got)
			}
		})
	}
}
