package connmysql

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// FuzzDDLSignature parses query with parseQueryEvent and reduces the result to
// the ddlfuzz comparison signature. Panics are NOT recovered here; the fuzz
// driver contains them.
func FuzzDDLSignature(query []byte, sqlMode uint64, isMariaDB bool) (string, error) {
	stmts, err := parseQueryEvent(query, sqlMode, isMariaDB)
	if err != nil {
		return "", err
	}
	return fuzzDDLSignature(stmts), nil
}

// FuzzSQLModeFromStatusVars re-exports the binlog status-var sql_mode walker for
// the e2e lane's plumbing check against real QueryEvent status vars.
func FuzzSQLModeFromStatusVars(statusVars []byte) (uint64, bool) {
	return sqlModeFromStatusVars(statusVars)
}

// FuzzParseForE2E parses like the production QueryEvent path and returns the
// parsed statements as JSON structurally parallel to the oracle digest, per
// 30-e2e-lane.md Interfaces (precision/scale ints carry ddlColumnDef verbatim,
// -1 = not written; no "other" entries). err mirrors parseQueryEvent; on err the
// JSON is the literal null.
func FuzzParseForE2E(query []byte, sqlMode uint64, isMariaDB bool) ([]byte, error) {
	stmts, err := parseQueryEvent(query, sqlMode, isMariaDB)
	if err != nil {
		return []byte("null"), err
	}
	return json.Marshal(fuzzDDLStmtsToE2E(stmts))
}

func fuzzDDLColSig(c ddlColumnDef) string {
	var sb strings.Builder
	sb.WriteString(fuzzDDLSigIdent(c.Name))
	sb.WriteByte('=')
	kind, err := QkindFromMysqlColumnType(fuzzDDLComparisonTypeStr(c.TypeStr), true, 0)
	if err != nil {
		sb.WriteString("ERR")
	} else {
		sb.WriteString(string(kind))
	}
	if kind == types.QValueKindNumeric {
		fmt.Fprintf(&sb, "(%d,%d)", c.Precision, c.Scale)
	}
	if c.NotNull {
		sb.WriteString(" nn")
	}
	return sb.String()
}

func fuzzDDLComparisonTypeStr(typeStr string) string {
	return strings.TrimSuffix(typeStr, " zerofill")
}

func fuzzDDLSpecSig(sp ddlAlterSpec) string {
	var sb strings.Builder
	switch {
	case sp.RenameColumn:
		sb.WriteString("ren " + fuzzDDLSigIdent(sp.OldColumnName) + ">" + fuzzDDLSigIdent(sp.NewColumnName))
	case len(sp.NewColumns) > 0:
		if sp.ChangeColumn {
			sb.WriteString("chg " + fuzzDDLSigIdent(sp.OldColumnName) + " ")
		} else {
			sb.WriteString("col ")
		}
		for i, c := range sp.NewColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fuzzDDLColSig(c))
		}
	default:
		sb.WriteString("drop " + fuzzDDLSigIdent(sp.OldColumnName))
	}
	if sp.HasPosition {
		sb.WriteString(" @pos")
	}
	return sb.String()
}

func fuzzDDLQual(schema, table string) string {
	if schema == "" {
		return fuzzDDLSigIdent(table)
	}
	return fuzzDDLSigIdent(schema) + "." + fuzzDDLSigIdent(table)
}

func fuzzDDLSigIdent(s string) string {
	if s != "" && !strings.HasPrefix(s, "`") && !strings.ContainsAny(s, " \t\r\n\v\f=,;(){}|>.") {
		return s
	}
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func fuzzDDLSignature(stmts []ddlStatement) string {
	var parts []string
	for _, s := range stmts {
		switch st := s.(type) {
		case *ddlAlterTable:
			specs := make([]string, len(st.Specs))
			for i, sp := range st.Specs {
				specs[i] = fuzzDDLSpecSig(sp)
			}
			parts = append(parts, "alter "+fuzzDDLQual(st.Schema, st.Table)+"{"+strings.Join(specs, "; ")+"}")
		case *ddlRenameTable:
			prs := make([]string, len(st.Pairs))
			for i, p := range st.Pairs {
				prs[i] = fuzzDDLQual(p.OldSchema, p.OldTable) + ">" + fuzzDDLQual(p.NewSchema, p.NewTable)
			}
			parts = append(parts, "rename "+strings.Join(prs, ", "))
		}
	}
	return strings.Join(parts, " | ")
}

// --- e2e JSON (30-e2e-lane.md Interfaces is normative for the shape) ---

type e2eCol struct {
	Name      string `json:"name"`
	TypeStr   string `json:"type_str"`
	NotNull   bool   `json:"not_null"`
	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
}
type e2eSpec struct {
	Op          string   `json:"op"` // add | change | rename_col | drop
	OldName     string   `json:"old_name,omitempty"`
	NewName     string   `json:"new_name,omitempty"`
	Cols        []e2eCol `json:"cols,omitempty"`
	HasPosition bool     `json:"has_position"`
	IfExists    bool     `json:"if_exists,omitempty"`
}
type e2ePair struct {
	OldSchema string `json:"old_schema"`
	OldTable  string `json:"old_table"`
	NewSchema string `json:"new_schema"`
	NewTable  string `json:"new_table"`
}
type e2eStmt struct {
	Kind   string    `json:"kind"` // alter_table | rename_table
	Schema string    `json:"schema,omitempty"`
	Table  string    `json:"table,omitempty"`
	Specs  []e2eSpec `json:"specs,omitempty"`
	Pairs  []e2ePair `json:"pairs,omitempty"`
}
type e2eStmts struct {
	Stmts []e2eStmt `json:"stmts"`
}

func fuzzDDLColToE2E(c ddlColumnDef) e2eCol {
	return e2eCol{Name: c.Name, TypeStr: c.TypeStr, NotNull: c.NotNull, Precision: c.Precision, Scale: c.Scale}
}

func fuzzDDLE2ESpecs(specs []ddlAlterSpec) []ddlAlterSpec {
	knownColumns := make(map[string]struct{})
	out := make([]ddlAlterSpec, 0, len(specs))
	for _, spec := range specs {
		filtered, ok := fuzzDDLE2EFilterConditionalAdd(spec, knownColumns)
		if !ok {
			continue
		}
		fuzzDDLE2EApplySpecColumnNames(knownColumns, filtered)
		out = append(out, filtered)
	}
	return out
}

func fuzzDDLE2EFilterConditionalAdd(spec ddlAlterSpec, knownColumns map[string]struct{}) (ddlAlterSpec, bool) {
	if !spec.AddIfNotExists {
		return spec, true
	}
	spec.AddIfNotExists = false
	cols := make([]ddlColumnDef, 0, len(spec.NewColumns))
	for _, col := range spec.NewColumns {
		if _, ok := knownColumns[fuzzDDLE2EColumnNameKey(col.Name)]; ok {
			continue
		}
		cols = append(cols, col)
	}
	spec.NewColumns = cols
	if len(spec.NewColumns) == 0 && spec.OldColumnName == "" && !spec.RenameColumn {
		return ddlAlterSpec{}, false
	}
	return spec, true
}

func fuzzDDLE2EApplySpecColumnNames(knownColumns map[string]struct{}, spec ddlAlterSpec) {
	var removes, adds []string
	switch {
	case spec.RenameColumn:
		removes = append(removes, spec.OldColumnName)
		adds = append(adds, spec.NewColumnName)
	case len(spec.NewColumns) > 0:
		if spec.ChangeColumn && len(spec.NewColumns) == 1 && spec.OldColumnName != spec.NewColumns[0].Name {
			removes = append(removes, spec.OldColumnName)
		}
		for _, col := range spec.NewColumns {
			adds = append(adds, col.Name)
		}
	case spec.OldColumnName != "":
		removes = append(removes, spec.OldColumnName)
	}
	for _, name := range removes {
		delete(knownColumns, fuzzDDLE2EColumnNameKey(name))
	}
	for _, name := range adds {
		knownColumns[fuzzDDLE2EColumnNameKey(name)] = struct{}{}
	}
}

func fuzzDDLE2EColumnNameKey(name string) string {
	return strings.ToLower(name)
}

func fuzzDDLStmtsToE2E(stmts []ddlStatement) e2eStmts {
	out := e2eStmts{Stmts: []e2eStmt{}}
	for _, s := range stmts {
		switch st := s.(type) {
		case *ddlAlterTable:
			es := e2eStmt{Kind: "alter_table", Schema: st.Schema, Table: st.Table, Specs: []e2eSpec{}}
			specs := fuzzDDLE2ESpecs(st.Specs)
			implicitPositionShift := ddlAlterSpecsHaveImplicitPositionShift(specs)
			for i, sp := range specs {
				var spec e2eSpec
				spec.HasPosition = sp.HasPosition
				if implicitPositionShift && i == 0 {
					spec.HasPosition = true
				}
				switch {
				case sp.RenameColumn:
					spec.Op = "rename_col"
					spec.OldName = sp.OldColumnName
					spec.NewName = sp.NewColumnName
					spec.IfExists = sp.RenameIfExists
				case len(sp.NewColumns) > 0 && (sp.ChangeColumn || sp.ModifyIfExists):
					spec.Op = "change"
					if sp.ChangeColumn {
						spec.OldName = sp.OldColumnName
						spec.IfExists = sp.ChangeIfExists
					} else {
						spec.OldName = sp.NewColumns[0].Name
						spec.IfExists = sp.ModifyIfExists
					}
					for _, c := range sp.NewColumns {
						spec.Cols = append(spec.Cols, fuzzDDLColToE2E(c))
					}
				case len(sp.NewColumns) > 0:
					spec.Op = "add"
					for _, c := range sp.NewColumns {
						spec.Cols = append(spec.Cols, fuzzDDLColToE2E(c))
					}
				default:
					spec.Op = "drop"
					spec.OldName = sp.OldColumnName
					spec.IfExists = sp.DropIfExists
				}
				es.Specs = append(es.Specs, spec)
			}
			out.Stmts = append(out.Stmts, es)
		case *ddlRenameTable:
			es := e2eStmt{Kind: "rename_table"}
			for _, p := range st.Pairs {
				es.Pairs = append(es.Pairs, e2ePair(p))
			}
			out.Stmts = append(out.Stmts, es)
		}
	}
	return out
}
