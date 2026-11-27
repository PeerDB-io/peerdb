package connsnowflake

import "testing"

func TestGenerateAddColumnSQL(t *testing.T) {
	const table = `"PUBLIC"."T"`
	tests := []struct {
		name     string
		column   string
		expected string
	}{
		{"camelCase keeps case", "pulseArmor", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "pulseArmor" STRING`},
		{"PascalCase keeps case", "PulseArmor", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "PulseArmor" STRING`},
		{"camelCase with digit", "col2Name", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "col2Name" STRING`},
		{"snake_case is uppercased", "pulse_armor", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "PULSE_ARMOR" STRING`},
		{"lowercase is uppercased", "pulse", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "PULSE" STRING`},
		{"uppercase is unchanged", "PULSE", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "PULSE" STRING`},
		{"space is quoted", "pulse armor", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "PULSE ARMOR" STRING`},
		{"mixed case with space is quoted", "Pulse Armor", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "Pulse Armor" STRING`},
		{"leading digit is quoted", "1st_col", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "1ST_COL" STRING`},
		{"reserved word is quoted", "order", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "ORDER" STRING`},
		{"uppercase non-ASCII is unchanged", "CARIÑO", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "CARIÑO" STRING`},
		{"uncased non-ASCII is unchanged", "カラム", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "カラム" STRING`},
		{"hyphen is quoted", "pulse-armor", `ALTER TABLE "PUBLIC"."T" ADD COLUMN IF NOT EXISTS "PULSE-ARMOR" STRING`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateAddColumnSQL(table, tt.column, "STRING")
			if got != tt.expected {
				t.Errorf("generateAddColumnSQL(%q) =\n  %s\nwant\n  %s", tt.column, got, tt.expected)
			}
		})
	}
}

// A column added by a schema change must be named as MERGE and CREATE TABLE name it,
// otherwise the next MERGE fails with "invalid identifier".
func TestGenerateAddColumnSQLMatchesIdentifierNormalize(t *testing.T) {
	for _, column := range []string{"pulseArmor", "pulse_armor", "pulse", "PULSE", "pulse armor", "Pulse Armor"} {
		want := "ALTER TABLE T ADD COLUMN IF NOT EXISTS " + SnowflakeIdentifierNormalize(column) + " STRING"
		if got := generateAddColumnSQL("T", column, "STRING"); got != want {
			t.Errorf("column %q: got %s, want %s", column, got, want)
		}
	}
}
