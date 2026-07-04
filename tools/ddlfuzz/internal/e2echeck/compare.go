package e2echeck

import (
	"slices"
	"strings"
)

type SemanticInput struct {
	Before                     Snapshot
	After                      Snapshot
	Actual                     Delta
	CaseInsensitiveTableRename bool
}

type SemanticFinding struct {
	Class string
	Meta  map[string]any
}

type predicted struct {
	Added       map[string]ParsedCol
	Dropped     map[string]bool
	Changed     map[string]ParsedCol
	Renamed     map[string]string
	RenameAttrs map[string]ColRow
	HasPosition bool
	TablePairs  []ParsedPair
}

func applyPredicted(before Snapshot, parsed ParsedStmts) predicted {
	p := predicted{
		Added:       make(map[string]ParsedCol),
		Dropped:     make(map[string]bool),
		Changed:     make(map[string]ParsedCol),
		Renamed:     make(map[string]string),
		RenameAttrs: make(map[string]ColRow),
	}
	for _, stmt := range parsed.Stmts {
		switch stmt.Kind {
		case "alter_table":
			for _, spec := range stmt.Specs {
				if spec.HasPosition {
					p.HasPosition = true
				}
				switch spec.Op {
				case "add":
					for _, col := range spec.Cols {
						if _, existed := before[col.Name]; existed {
							p.Changed[col.Name] = col
						} else {
							p.Added[col.Name] = col
						}
					}
				case "change":
					if len(spec.Cols) == 0 {
						continue
					}
					col := spec.Cols[0]
					if col.Name == spec.OldName {
						p.Changed[col.Name] = col
					} else {
						p.Dropped[spec.OldName] = true
						p.produceColumn(before, col.Name, col)
					}
				case "rename_col":
					if spec.OldName == spec.NewName {
						continue
					}
					p.Dropped[spec.OldName] = true
					p.produceColumn(before, spec.NewName, ParsedCol{Name: spec.NewName})
					p.Renamed[spec.OldName] = spec.NewName
					if old, ok := before[spec.OldName]; ok {
						p.RenameAttrs[spec.NewName] = old
					}
				case "drop":
					p.Dropped[spec.OldName] = true
				}
			}
		case "rename_table":
			p.TablePairs = append(p.TablePairs, stmt.Pairs...)
		}
	}
	for name := range p.Dropped {
		if _, ok := p.Added[name]; ok {
			delete(p.Dropped, name)
			p.HasPosition = true
			continue
		}
		if _, ok := p.Changed[name]; ok {
			delete(p.Dropped, name)
			p.HasPosition = true
		}
	}
	return p
}

func (p *predicted) produceColumn(before Snapshot, name string, col ParsedCol) {
	if _, existed := before[name]; existed {
		p.Changed[name] = col
	} else {
		p.Added[name] = col
	}
}

func CompareSemantics(in SemanticInput, parsed ParsedStmts) []SemanticFinding {
	actual := in.Actual
	if len(parsed.Stmts) == 0 {
		if !actual.Empty() {
			return []SemanticFinding{{Class: ClassMissedColumnEffect, Meta: map[string]any{"benign_classification": true}}}
		}
		return nil
	}

	pred := applyPredicted(in.Before, parsed)
	var out []SemanticFinding
	actualAdded := rowsToSet(actual.Added)
	actualDropped := rowsToSet(actual.Dropped)
	actualChanged := changesToSet(actual.Changed)

	if len(pred.TablePairs) > 0 {
		for _, pair := range pred.TablePairs {
			if !containsRename(actual.Renamed, pair.OldTable, pair.NewTable, in.CaseInsensitiveTableRename) {
				out = append(out, SemanticFinding{
					Class: ClassMissedColumnEffect,
					Meta: map[string]any{
						"table_rename_expected": pair,
						"table_renamed_actual":  actual.Renamed,
					},
				})
			}
		}
		if len(pred.Added) == 0 && len(pred.Dropped) == 0 && len(pred.Changed) == 0 && len(pred.Renamed) == 0 {
			return out
		}
	}

	if missing, unexpected := compareSetMap(pred.Added, actualAdded); len(missing) > 0 || len(unexpected) > 0 {
		out = append(out, SemanticFinding{
			Class: ClassMissedColumnEffect,
			Meta:  map[string]any{"added_missing": missing, "added_unexpected": unexpected},
		})
	}
	if missing, unexpected := compareBoolSet(pred.Dropped, actualDropped); len(missing) > 0 || len(unexpected) > 0 {
		out = append(out, SemanticFinding{
			Class: ClassMissedColumnEffect,
			Meta:  map[string]any{"dropped_missing": missing, "dropped_unexpected": unexpected},
		})
	}
	for name := range actualChanged {
		if _, ok := pred.Changed[name]; !ok && !pred.Dropped[name] {
			if ch, ok := changeByName(actual.Changed, name); ok && (ordinalOnlyChange(ch) || columnKeyOnlyChange(ch)) {
				continue
			}
			out = append(out, SemanticFinding{
				Class: ClassMissedColumnEffect,
				Meta:  map[string]any{"changed_unexpected": name},
			})
		}
	}

	for name, col := range pred.Added {
		row, ok := in.After[name]
		if !ok {
			continue
		}
		if old, renamed := pred.RenameAttrs[name]; renamed {
			if row.ColumnType != old.ColumnType {
				out = append(out, SemanticFinding{
					Class: ClassColumnAttr,
					Meta:  map[string]any{"column": name, "attribute": "rename_column_type", "want": old.ColumnType, "got": row.ColumnType},
				})
			}
			continue
		}
		out = append(out, compareColumnAttrs(name, col, row)...)
	}
	for name, col := range pred.Changed {
		row, ok := in.After[name]
		if !ok {
			continue
		}
		if old, renamed := pred.RenameAttrs[name]; renamed {
			if row.ColumnType != old.ColumnType {
				out = append(out, SemanticFinding{
					Class: ClassColumnAttr,
					Meta:  map[string]any{"column": name, "attribute": "rename_column_type", "want": old.ColumnType, "got": row.ColumnType},
				})
			}
			continue
		}
		out = append(out, compareColumnAttrs(name, col, row)...)
	}

	if !pred.HasPosition && len(actual.Dropped) == 0 {
		for _, ch := range actual.Changed {
			if ch.Before.Ordinal != ch.After.Ordinal {
				out = append(out, SemanticFinding{
					Class: ClassPositionMissed,
					Meta:  map[string]any{"column": ch.Name, "before_ordinal": ch.Before.Ordinal, "after_ordinal": ch.After.Ordinal},
				})
				break
			}
		}
	}

	return out
}

func compareColumnAttrs(name string, col ParsedCol, row ColRow) []SemanticFinding {
	var out []SemanticFinding
	wantKind := qkindString(col.TypeStr)
	gotKind := qkindString(row.ColumnType)
	if wantKind != gotKind {
		out = append(out, SemanticFinding{
			Class: ClassColumnAttr,
			Meta:  map[string]any{"column": name, "attribute": "qkind", "want": wantKind, "got": gotKind, "want_type": col.TypeStr, "got_type": row.ColumnType},
		})
	}

	wantNotNull := col.NotNull
	gotNotNull := strings.EqualFold(row.IsNullable, "NO")
	if wantNotNull != gotNotNull && !(gotNotNull && !wantNotNull && nullableImpliedNotNull(row)) {
		out = append(out, SemanticFinding{
			Class: ClassColumnAttr,
			Meta:  map[string]any{"column": name, "attribute": "nullability", "want_not_null": wantNotNull, "got_not_null": gotNotNull, "column_key": row.ColumnKey},
		})
	}

	if gotKind == "numeric" {
		if col.Precision >= 0 && (row.NumPrec == nil || *row.NumPrec != int64(col.Precision)) {
			out = append(out, SemanticFinding{
				Class: ClassColumnAttr,
				Meta:  map[string]any{"column": name, "attribute": "numeric_precision", "want": col.Precision, "got": row.NumPrec},
			})
		}
		if col.Scale >= 0 && (row.NumScale == nil || *row.NumScale != int64(col.Scale)) {
			out = append(out, SemanticFinding{
				Class: ClassColumnAttr,
				Meta:  map[string]any{"column": name, "attribute": "numeric_scale", "want": col.Scale, "got": row.NumScale},
			})
		}
	}
	return out
}

func nullableImpliedNotNull(row ColRow) bool {
	if row.ColumnKey == "PRI" {
		return true
	}
	t := strings.ToLower(row.ColumnType)
	return strings.Contains(t, "auto_increment") || strings.HasPrefix(t, "serial")
}

func rowsToSet(rows []ColRow) map[string]bool {
	out := make(map[string]bool, len(rows))
	for _, row := range rows {
		out[row.Name] = true
	}
	return out
}

func changesToSet(changes []ColumnChange) map[string]bool {
	out := make(map[string]bool, len(changes))
	for _, ch := range changes {
		out[ch.Name] = true
	}
	return out
}

func changeByName(changes []ColumnChange, name string) (ColumnChange, bool) {
	for _, ch := range changes {
		if ch.Name == name {
			return ch, true
		}
	}
	return ColumnChange{}, false
}

func ordinalOnlyChange(ch ColumnChange) bool {
	before := ch.Before
	after := ch.After
	before.Ordinal = after.Ordinal
	return colRowsEqual(before, after)
}

func columnKeyOnlyChange(ch ColumnChange) bool {
	before := ch.Before
	after := ch.After
	before.ColumnKey = after.ColumnKey
	return colRowsEqual(before, after)
}

func colRowsEqual(a, b ColRow) bool {
	return a.Name == b.Name &&
		a.Ordinal == b.Ordinal &&
		a.ColumnType == b.ColumnType &&
		a.IsNullable == b.IsNullable &&
		sameNullableInt(a.NumPrec, b.NumPrec) &&
		sameNullableInt(a.NumScale, b.NumScale)
}

func compareSetMap(pred map[string]ParsedCol, actual map[string]bool) (missing, unexpected []string) {
	for name := range pred {
		if !actual[name] {
			missing = append(missing, name)
		}
	}
	for name := range actual {
		if _, ok := pred[name]; !ok {
			unexpected = append(unexpected, name)
		}
	}
	slices.Sort(missing)
	slices.Sort(unexpected)
	return missing, unexpected
}

func compareBoolSet(pred map[string]bool, actual map[string]bool) (missing, unexpected []string) {
	for name := range pred {
		if !actual[name] {
			missing = append(missing, name)
		}
	}
	for name := range actual {
		if !pred[name] {
			unexpected = append(unexpected, name)
		}
	}
	slices.Sort(missing)
	slices.Sort(unexpected)
	return missing, unexpected
}

func containsRename(items []RenameSummary, oldName, newName string, caseInsensitive bool) bool {
	for _, item := range items {
		if item.Old == oldName && item.New == newName {
			return true
		}
		if caseInsensitive && strings.EqualFold(item.Old, oldName) && strings.EqualFold(item.New, newName) {
			return true
		}
	}
	return false
}
