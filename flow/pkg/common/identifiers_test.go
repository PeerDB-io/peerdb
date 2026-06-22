package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeTableIdentifier(t *testing.T) {
	tests := []struct {
		identifier string
		expected   QualifiedTable
	}{
		{"table", QualifiedTable{Table: "table"}},
		{"schema.table", QualifiedTable{Namespace: "schema", Table: "table"}},
		{"a.b.c", QualifiedTable{Namespace: "a", Table: "b.c"}},
		{"a.b.c.d", QualifiedTable{Namespace: "a", Table: "b.c.d"}},
		{"", QualifiedTable{}},
		{".table", QualifiedTable{Namespace: "", Table: "table"}},
		{"schema.", QualifiedTable{Namespace: "schema", Table: ""}},
		{`"quo"ted".table`, QualifiedTable{Namespace: `"quo"ted"`, Table: "table"}},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, NormalizeTableIdentifier(tt.identifier), "identifier %q", tt.identifier)
	}
}

func TestLegacyDotted(t *testing.T) {
	assert.Equal(t, "schema.table", QualifiedTable{Namespace: "schema", Table: "table"}.LegacyDotted())
	assert.Equal(t, "table", QualifiedTable{Table: "table"}.LegacyDotted())
	assert.Equal(t, "sch.ema.ta.ble", QualifiedTable{Namespace: "sch.ema", Table: "ta.ble"}.LegacyDotted())
}

// normalizing a dotless-component identifier's legacy form must round-trip;
// dotted components are where LegacyDotted is ambiguous by design
func TestLegacyDottedRoundTrip(t *testing.T) {
	for _, qt := range []QualifiedTable{
		{Namespace: "schema", Table: "table"},
		{Table: "table"},
	} {
		assert.Equal(t, qt, NormalizeTableIdentifier(qt.LegacyDotted()))
	}
	dotted := QualifiedTable{Namespace: "sch.ema", Table: "table"}
	assert.NotEqual(t, dotted, NormalizeTableIdentifier(dotted.LegacyDotted()))
}

func TestQualifiedTableString(t *testing.T) {
	assert.Equal(t, `"sch.ema"."ta""ble"`, QualifiedTable{Namespace: "sch.ema", Table: `ta"ble`}.String())
	assert.Equal(t, "`sch.ema`.`ta``ble`", QualifiedTable{Namespace: "sch.ema", Table: "ta`ble"}.MySQL())
}
