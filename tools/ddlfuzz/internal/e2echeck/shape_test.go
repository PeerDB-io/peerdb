package e2echeck

import (
	"strings"
	"testing"
)

func TestShapeFor(t *testing.T) {
	tests := []struct {
		name  string
		class string
		meta  map[string]any
		want  string
	}{
		{"status vars", ClassStatusVarWalk, nil, "status-vars"},
		{"sql mode", ClassSQLModeMismatch, nil, "sql-mode"},
		{"query rewrite", ClassQueryRewrite, nil, "query-rewrite"},
		{"plumbing", ClassPlumbingSig, nil, "plumbing-sig"},
		{"panic", ClassPanic, nil, "parser-panic"},
		{"parse error", ClassParseErrorLiveAccept, nil, "parse-error-live-accept"},
		{"position", ClassPositionMissed, nil, "position-missed"},
		{"col attr valid", ClassColumnAttr, map[string]any{"attribute": "nullability", "column": "secret"}, "col-attr(nullability)"},
		{"col attr invalid", ClassColumnAttr, map[string]any{"attribute": "column_type"}, "col-attr(?)"},
		{"missed added", ClassMissedColumnEffect, map[string]any{"added_column": "secret"}, "missed-effect(added)"},
		{"missed dropped", ClassMissedColumnEffect, map[string]any{"dropped_name": "secret"}, "missed-effect(dropped)"},
		{"missed changed", ClassMissedColumnEffect, map[string]any{"changed_unexpected": "secret"}, "missed-effect(changed)"},
		{"missed benign", ClassMissedColumnEffect, map[string]any{"benign_classification": "secret"}, "missed-effect(benign)"},
		{"missed unknown", ClassMissedColumnEffect, map[string]any{"column": "secret"}, "missed-effect(?)"},
		{"unknown", "e2e-new-class", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShapeFor(tt.class, tt.meta)
			if got != tt.want {
				t.Fatalf("ShapeFor()=%q, want %q", got, tt.want)
			}
			if strings.Contains(got, "secret") {
				t.Fatalf("shape leaks identifier text: %q", got)
			}
		})
	}
}
