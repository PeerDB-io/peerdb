package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTableIdentifier(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantNS    string
		wantTable string
		wantErr   bool
	}{
		{name: "simple", input: "schema.table", wantNS: "schema", wantTable: "table"},
		{name: "dot in namespace", input: "my..db.table", wantNS: "my.db", wantTable: "table"},
		{name: "dot in table", input: "schema.my..table", wantNS: "schema", wantTable: "my.table"},
		{name: "dots in both", input: "my..db.my..table", wantNS: "my.db", wantTable: "my.table"},
		{name: "multiple dots in namespace", input: "a..b..c.table", wantNS: "a.b.c", wantTable: "table"},
		{name: "trailing dot in namespace", input: "a...table", wantNS: "a.", wantTable: "table"},
		{name: "leading dot in table", input: "schema...table", wantNS: "schema.", wantTable: "table"},
		{name: "empty", input: "", wantErr: true},
		{name: "no dot", input: "table", wantErr: true},
		{name: "empty namespace", input: ".table", wantErr: true},
		{name: "empty table", input: "schema.", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTableIdentifier(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantNS, got.Namespace)
			require.Equal(t, tt.wantTable, got.Table)
		})
	}
}

func TestDeparseRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		ns    string
		table string
	}{
		{name: "simple", ns: "schema", table: "table"},
		{name: "dot in namespace", ns: "my.db", table: "table"},
		{name: "dot in table", ns: "schema", table: "my.table"},
		{name: "dots in both", ns: "my.db", table: "my.table"},
		{name: "multiple dots", ns: "a.b.c", table: "d.e"},
		{name: "consecutive dots", ns: "a..b", table: "table"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qt := &QualifiedTable{Namespace: tt.ns, Table: tt.table}
			deparsed := qt.Deparse()
			parsed, err := ParseTableIdentifier(deparsed)
			require.NoError(t, err)
			require.Equal(t, tt.ns, parsed.Namespace)
			require.Equal(t, tt.table, parsed.Table)
		})
	}
}
