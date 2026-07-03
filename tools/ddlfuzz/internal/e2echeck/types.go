package e2echeck

import (
	"encoding/json"
	"slices"
)

const (
	ClassStatusVarWalk        = "e2e-statusvar-walk"
	ClassSQLModeMismatch      = "e2e-sqlmode-mismatch"
	ClassPlumbingSig          = "e2e-plumbing-sig"
	ClassPanic                = "e2e-panic"
	ClassQueryRewrite         = "e2e-query-rewrite"
	ClassParseErrorLiveAccept = "e2e-parse-error-live-accept"
	ClassMissedColumnEffect   = "e2e-missed-column-effect"
	ClassColumnAttr           = "e2e-col-attr"
	ClassPositionMissed       = "e2e-position-missed"
)

const (
	SQLModeANSIQuotes         uint64 = 1 << 2
	SQLModeOracle             uint64 = 1 << 9
	SQLModeMSSQL              uint64 = 1 << 10
	SQLModeNoBackslashEscapes uint64 = 1 << 20
	RelevantSQLModeMask              = SQLModeANSIQuotes | SQLModeOracle | SQLModeMSSQL | SQLModeNoBackslashEscapes
)

// Input is reconstructed from e2e finding meta and never needs a live database.
type Input struct {
	Engine        string
	IsMariaDB     bool
	SQLMode       uint64
	Submitted     string
	BinlogQuery   string
	StatusVarsHex string
	Before        Snapshot
	After         Snapshot
	Delta         Delta
	Class         string

	ExpectedRelevant *uint64
	ActualRelevant   *uint64
}

type Result struct {
	Reconciled bool
	Class      string
	Shape      string
	Detail     string
}

type ParsedStmts struct {
	Stmts []ParsedStmt `json:"stmts"`
}

type ParsedStmt struct {
	Kind   string       `json:"kind"`
	Schema string       `json:"schema,omitempty"`
	Table  string       `json:"table,omitempty"`
	Specs  []ParsedSpec `json:"specs,omitempty"`
	Pairs  []ParsedPair `json:"pairs,omitempty"`
}

type ParsedSpec struct {
	Op          string      `json:"op"`
	OldName     string      `json:"old_name,omitempty"`
	NewName     string      `json:"new_name,omitempty"`
	Cols        []ParsedCol `json:"cols,omitempty"`
	HasPosition bool        `json:"has_position"`
}

type ParsedCol struct {
	Name      string `json:"name"`
	TypeStr   string `json:"type_str"`
	NotNull   bool   `json:"not_null"`
	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
}

type ParsedPair struct {
	OldSchema string `json:"old_schema"`
	OldTable  string `json:"old_table"`
	NewSchema string `json:"new_schema"`
	NewTable  string `json:"new_table"`
}

type ColRow struct {
	Name       string `json:"name"`
	Ordinal    int    `json:"ordinal_position"`
	ColumnType string `json:"column_type"`
	IsNullable string `json:"is_nullable"`
	ColumnKey  string `json:"column_key"`
	NumPrec    *int64 `json:"numeric_precision"`
	NumScale   *int64 `json:"numeric_scale"`
}

type Snapshot map[string]ColRow

type ColumnChange struct {
	Name   string `json:"name"`
	Before ColRow `json:"before"`
	After  ColRow `json:"after"`
}

type Delta struct {
	Added   []ColRow        `json:"added"`
	Dropped []ColRow        `json:"dropped"`
	Changed []ColumnChange  `json:"changed"`
	Renamed []RenameSummary `json:"renamed,omitempty"`
}

type RenameSummary struct {
	Old string `json:"old"`
	New string `json:"new"`
}

func (d Delta) MarshalJSON() ([]byte, error) {
	type deltaJSON struct {
		Added   []ColRow        `json:"added"`
		Dropped []ColRow        `json:"dropped"`
		Changed []ColumnChange  `json:"changed"`
		Renamed []RenameSummary `json:"renamed,omitempty"`
	}
	out := deltaJSON{
		Added:   d.Added,
		Dropped: d.Dropped,
		Changed: d.Changed,
		Renamed: d.Renamed,
	}
	if out.Added == nil {
		out.Added = []ColRow{}
	}
	if out.Dropped == nil {
		out.Dropped = []ColRow{}
	}
	if out.Changed == nil {
		out.Changed = []ColumnChange{}
	}
	return json.Marshal(out)
}

func (s Snapshot) ColumnsByOrdinal() []string {
	rows := make([]ColRow, 0, len(s))
	for _, row := range s {
		rows = append(rows, row)
	}
	slices.SortFunc(rows, func(a, b ColRow) int { return a.Ordinal - b.Ordinal })
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = row.Name
	}
	return out
}

func (d Delta) Empty() bool {
	return len(d.Added) == 0 && len(d.Dropped) == 0 && len(d.Changed) == 0 && len(d.Renamed) == 0
}

func DiffSnapshots(before, after Snapshot) Delta {
	var d Delta
	for name, row := range after {
		prev, ok := before[name]
		if !ok {
			d.Added = append(d.Added, row)
			continue
		}
		if prev.ColumnType != row.ColumnType || prev.IsNullable != row.IsNullable ||
			prev.Ordinal != row.Ordinal || prev.ColumnKey != row.ColumnKey ||
			!sameNullableInt(prev.NumPrec, row.NumPrec) || !sameNullableInt(prev.NumScale, row.NumScale) {
			d.Changed = append(d.Changed, ColumnChange{Name: name, Before: prev, After: row})
		}
	}
	for name, row := range before {
		if _, ok := after[name]; !ok {
			d.Dropped = append(d.Dropped, row)
		}
	}
	slices.SortFunc(d.Added, func(a, b ColRow) int { return a.Ordinal - b.Ordinal })
	slices.SortFunc(d.Dropped, func(a, b ColRow) int { return a.Ordinal - b.Ordinal })
	slices.SortFunc(d.Changed, func(a, b ColumnChange) int { return a.After.Ordinal - b.After.Ordinal })
	return d
}

func sameNullableInt(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func TableSetDelta(before, after map[string]bool) (dropped, added []string) {
	for name := range before {
		if !after[name] {
			dropped = append(dropped, name)
		}
	}
	for name := range after {
		if !before[name] {
			added = append(added, name)
		}
	}
	slices.Sort(dropped)
	slices.Sort(added)
	return dropped, added
}
