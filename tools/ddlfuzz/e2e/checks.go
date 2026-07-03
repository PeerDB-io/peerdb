package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/findings"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/minimize"
	"github.com/go-mysql-org/go-mysql/replication"
)

type parsedStmts struct {
	Stmts []parsedStmt `json:"stmts"`
}

type parsedStmt struct {
	Kind   string       `json:"kind"`
	Schema string       `json:"schema,omitempty"`
	Table  string       `json:"table,omitempty"`
	Specs  []parsedSpec `json:"specs,omitempty"`
	Pairs  []parsedPair `json:"pairs,omitempty"`
}

type parsedSpec struct {
	Op          string      `json:"op"`
	OldName     string      `json:"old_name,omitempty"`
	NewName     string      `json:"new_name,omitempty"`
	Cols        []parsedCol `json:"cols,omitempty"`
	HasPosition bool        `json:"has_position"`
}

type parsedCol struct {
	Name      string `json:"name"`
	TypeStr   string `json:"type_str"`
	NotNull   bool   `json:"not_null"`
	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
}

type parsedPair struct {
	OldSchema string `json:"old_schema"`
	OldTable  string `json:"old_table"`
	NewSchema string `json:"new_schema"`
	NewTable  string `json:"new_table"`
}

type findingInput struct {
	Class       string
	Engine      engineConfig
	Statement   []byte
	SQLMode     uint64
	SQLModeName string
	OurSig      string
	OurError    string
	Delta       delta
	RawEvent    []byte
	StatusVars  []byte
	Submitted   string
	BinlogText  string
	Meta        map[string]any
}

func checkLiveDDL(ctx context.Context, ec engineConfig, stateDir string, stats *Stats, sides *sideChannels, ev *replication.BinlogEvent, qe *replication.QueryEvent, exp caseExpect) {
	query := slices.Clone(qe.Query)
	statusVars := slices.Clone(qe.StatusVars)
	raw := slices.Clone(ev.RawData)
	actualDelta := diffSnapshots(exp.Before, exp.After)
	actualDelta.Renamed = inferRenames(exp.BeforeTables, exp.AfterTables)
	findingCount := 0

	mode, ok := safeSQLModeFromStatusVars(statusVars)
	if !ok {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-statusvar-walk",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     0,
			SQLModeName: exp.SQLModeName,
			Delta:       actualDelta,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta:        map[string]any{"status_vars_hex": hex.EncodeToString(statusVars)},
		})
	}
	if ok && mode&relevantMask != exp.SQLModeRelevant {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-sqlmode-mismatch",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     mode,
			SQLModeName: exp.SQLModeName,
			Delta:       actualDelta,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta: map[string]any{
				"expected_relevant": exp.SQLModeRelevant,
				"actual_relevant":   mode & relevantMask,
			},
		})
	}

	if string(query) != exp.Submitted {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-query-rewrite",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     mode,
			SQLModeName: exp.SQLModeName,
			Delta:       actualDelta,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta: map[string]any{
				"submitted_text": exp.Submitted,
				"binlog_text":    string(query),
			},
		})
	}

	liveSig, liveErr, livePanic := safeDDLSignature(query, mode, ec.IsMariaDB)
	subSig, subErr, subPanic := safeDDLSignature([]byte(exp.Submitted), exp.SQLModeRelevant, ec.IsMariaDB)
	if livePanic != nil || subPanic != nil {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-panic",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     mode,
			SQLModeName: exp.SQLModeName,
			OurSig:      liveSig,
			OurError:    errString(liveErr),
			Delta:       actualDelta,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta: map[string]any{
				"live_panic":      fmt.Sprint(livePanic),
				"submitted_panic": fmt.Sprint(subPanic),
			},
		})
	} else if liveSig != subSig || (liveErr != nil) != (subErr != nil) {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-plumbing-sig",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     mode,
			SQLModeName: exp.SQLModeName,
			OurSig:      liveSig,
			OurError:    errString(liveErr),
			Delta:       actualDelta,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta: map[string]any{
				"submitted_sig":   subSig,
				"submitted_error": errString(subErr),
				"live_sig":        liveSig,
				"live_error":      errString(liveErr),
			},
		})
	}

	parsed, parseErr, parsePanic := safeParseForE2E(query, mode, ec.IsMariaDB)
	if parsePanic != nil {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-panic",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     mode,
			SQLModeName: exp.SQLModeName,
			OurSig:      liveSig,
			OurError:    errString(parseErr),
			Delta:       actualDelta,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta:        map[string]any{"parse_panic": fmt.Sprint(parsePanic)},
		})
	} else if parseErr != nil {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-parse-error-live-accept",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     mode,
			SQLModeName: exp.SQLModeName,
			OurSig:      liveSig,
			OurError:    errString(parseErr),
			Delta:       actualDelta,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta:        map[string]any{"schema_changed": !actualDelta.empty()},
		})
	} else {
		for _, sf := range compareSemantics(exp, parsed, actualDelta) {
			findingCount += recordE2EFinding(stateDir, stats, findingInput{
				Class:       sf.Class,
				Engine:      ec,
				Statement:   []byte(exp.Submitted),
				SQLMode:     mode,
				SQLModeName: exp.SQLModeName,
				OurSig:      liveSig,
				OurError:    errString(liveErr),
				Delta:       actualDelta,
				RawEvent:    raw,
				StatusVars:  statusVars,
				Submitted:   exp.Submitted,
				BinlogText:  string(query),
				Meta:        sf.Meta,
			})
		}
	}

	stats.IncMatchedDDL(ec.Name)
	matched := stats.Snapshot()[ec.Name].MatchedDDLs
	if matched%50 == 0 {
		_ = sides.AppendLiveAccepted(liveAcceptedRecord{Engine: ec.Name, SQLMode: mode, Statement: string(query)})
	}

	if exp.Queue != nil {
		result := "confirmed-fixed"
		if findingCount > 0 {
			result = "still-diverges"
		}
		_ = completeQueueItem(stateDir, *exp.Queue, queueResult{Sig: exp.Queue.Sig, Result: result})
		if result == "confirmed-fixed" {
			stats.IncQueueDone(ec.Name)
		}
	}

	_ = ctx
}

func safeSQLModeFromStatusVars(statusVars []byte) (mode uint64, ok bool) {
	defer func() {
		if recover() != nil {
			mode, ok = 0, false
		}
	}()
	return connmysql.FuzzSQLModeFromStatusVars(statusVars)
}

func safeDDLSignature(query []byte, mode uint64, isMariaDB bool) (sig string, err error, panicked any) {
	defer func() {
		if r := recover(); r != nil {
			panicked = r
		}
	}()
	sig, err = connmysql.FuzzDDLSignature(query, mode, isMariaDB)
	return sig, err, nil
}

func safeParseForE2E(query []byte, mode uint64, isMariaDB bool) (out parsedStmts, err error, panicked any) {
	defer func() {
		if r := recover(); r != nil {
			panicked = r
		}
	}()
	data, err := connmysql.FuzzParseForE2E(query, mode, isMariaDB)
	if err != nil {
		return out, err, nil
	}
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return out, nil, nil
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err, nil
	}
	return out, nil, panicked
}

type semanticFinding struct {
	Class string
	Meta  map[string]any
}

type predicted struct {
	Added       map[string]parsedCol
	Dropped     map[string]bool
	Changed     map[string]parsedCol
	Renamed     map[string]string
	RenameAttrs map[string]colRow
	HasPosition bool
	TablePairs  []parsedPair
}

func applyPredicted(before snapshot, parsed parsedStmts) predicted {
	p := predicted{
		Added:       make(map[string]parsedCol),
		Dropped:     make(map[string]bool),
		Changed:     make(map[string]parsedCol),
		Renamed:     make(map[string]string),
		RenameAttrs: make(map[string]colRow),
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
						p.Added[col.Name] = col
					}
				case "rename_col":
					p.Dropped[spec.OldName] = true
					p.Added[spec.NewName] = parsedCol{Name: spec.NewName}
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
	return p
}

func compareSemantics(exp caseExpect, parsed parsedStmts, actual delta) []semanticFinding {
	if len(parsed.Stmts) == 0 {
		if !actual.empty() {
			return []semanticFinding{{Class: "e2e-missed-column-effect", Meta: map[string]any{"benign_classification": true}}}
		}
		return nil
	}

	pred := applyPredicted(exp.Before, parsed)
	var out []semanticFinding
	actualAdded := rowsToSet(actual.Added)
	actualDropped := rowsToSet(actual.Dropped)
	actualChanged := changesToSet(actual.Changed)

	if len(pred.TablePairs) > 0 {
		droppedTables, addedTables := tableSetDelta(exp.BeforeTables, exp.AfterTables)
		for _, pair := range pred.TablePairs {
			if !contains(droppedTables, pair.OldTable) || !contains(addedTables, pair.NewTable) {
				out = append(out, semanticFinding{
					Class: "e2e-missed-column-effect",
					Meta: map[string]any{
						"table_rename_expected": pair,
						"table_dropped_actual":  droppedTables,
						"table_added_actual":    addedTables,
					},
				})
			}
		}
	}

	if missing, unexpected := compareSetMap(pred.Added, actualAdded); len(missing) > 0 || len(unexpected) > 0 {
		out = append(out, semanticFinding{
			Class: "e2e-missed-column-effect",
			Meta:  map[string]any{"added_missing": missing, "added_unexpected": unexpected},
		})
	}
	if missing, unexpected := compareBoolSet(pred.Dropped, actualDropped); len(missing) > 0 || len(unexpected) > 0 {
		out = append(out, semanticFinding{
			Class: "e2e-missed-column-effect",
			Meta:  map[string]any{"dropped_missing": missing, "dropped_unexpected": unexpected},
		})
	}
	for name := range actualChanged {
		if _, ok := pred.Changed[name]; !ok && !pred.Dropped[name] {
			if ch, ok := changeByName(actual.Changed, name); ok && ordinalOnlyChange(ch) {
				continue
			}
			out = append(out, semanticFinding{
				Class: "e2e-missed-column-effect",
				Meta:  map[string]any{"changed_unexpected": name},
			})
		}
	}

	for name, col := range pred.Added {
		row, ok := exp.After[name]
		if !ok {
			continue
		}
		if old, renamed := pred.RenameAttrs[name]; renamed {
			if row.ColumnType != old.ColumnType {
				out = append(out, semanticFinding{
					Class: "e2e-col-attr",
					Meta:  map[string]any{"column": name, "attribute": "rename_column_type", "want": old.ColumnType, "got": row.ColumnType},
				})
			}
			continue
		}
		out = append(out, compareColumnAttrs(name, col, row)...)
	}
	for name, col := range pred.Changed {
		row, ok := exp.After[name]
		if !ok {
			continue
		}
		out = append(out, compareColumnAttrs(name, col, row)...)
	}

	if !pred.HasPosition && len(actual.Dropped) == 0 {
		for _, ch := range actual.Changed {
			if ch.Before.Ordinal != ch.After.Ordinal {
				out = append(out, semanticFinding{
					Class: "e2e-position-missed",
					Meta:  map[string]any{"column": ch.Name, "before_ordinal": ch.Before.Ordinal, "after_ordinal": ch.After.Ordinal},
				})
				break
			}
		}
	}

	return out
}

func compareColumnAttrs(name string, col parsedCol, row colRow) []semanticFinding {
	var out []semanticFinding
	wantKind := qkindString(col.TypeStr)
	gotKind := qkindString(row.ColumnType)
	if wantKind != gotKind {
		out = append(out, semanticFinding{
			Class: "e2e-col-attr",
			Meta:  map[string]any{"column": name, "attribute": "qkind", "want": wantKind, "got": gotKind, "want_type": col.TypeStr, "got_type": row.ColumnType},
		})
	}

	wantNotNull := col.NotNull
	gotNotNull := strings.EqualFold(row.IsNullable, "NO")
	if wantNotNull != gotNotNull && !(gotNotNull && !wantNotNull && nullableImpliedNotNull(row)) {
		out = append(out, semanticFinding{
			Class: "e2e-col-attr",
			Meta:  map[string]any{"column": name, "attribute": "nullability", "want_not_null": wantNotNull, "got_not_null": gotNotNull, "column_key": row.ColumnKey},
		})
	}

	if gotKind == string(types.QValueKindNumeric) {
		if col.Precision >= 0 && (!row.NumPrec.Valid || row.NumPrec.Int64 != int64(col.Precision)) {
			out = append(out, semanticFinding{
				Class: "e2e-col-attr",
				Meta:  map[string]any{"column": name, "attribute": "numeric_precision", "want": col.Precision, "got": row.NumPrec},
			})
		}
		if col.Scale >= 0 && (!row.NumScale.Valid || row.NumScale.Int64 != int64(col.Scale)) {
			out = append(out, semanticFinding{
				Class: "e2e-col-attr",
				Meta:  map[string]any{"column": name, "attribute": "numeric_scale", "want": col.Scale, "got": row.NumScale},
			})
		}
	}
	return out
}

func nullableImpliedNotNull(row colRow) bool {
	if row.ColumnKey == "PRI" {
		return true
	}
	t := strings.ToLower(row.ColumnType)
	return strings.Contains(t, "auto_increment") || strings.HasPrefix(t, "serial")
}

func qkindString(typeStr string) string {
	kind, err := connmysql.QkindFromMysqlColumnType(typeStr, true, 0)
	if err != nil {
		return "ERR"
	}
	return string(kind)
}

func rowsToSet(rows []colRow) map[string]bool {
	out := make(map[string]bool, len(rows))
	for _, row := range rows {
		out[row.Name] = true
	}
	return out
}

func changesToSet(changes []columnChange) map[string]bool {
	out := make(map[string]bool, len(changes))
	for _, ch := range changes {
		out[ch.Name] = true
	}
	return out
}

func changeByName(changes []columnChange, name string) (columnChange, bool) {
	for _, ch := range changes {
		if ch.Name == name {
			return ch, true
		}
	}
	return columnChange{}, false
}

func ordinalOnlyChange(ch columnChange) bool {
	before := ch.Before
	after := ch.After
	before.Ordinal = after.Ordinal
	return before == after
}

func compareSetMap(pred map[string]parsedCol, actual map[string]bool) (missing, unexpected []string) {
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

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func inferRenames(before, after map[string]bool) []renameSummary {
	dropped, added := tableSetDelta(before, after)
	if len(dropped) != 1 || len(added) != 1 {
		return nil
	}
	return []renameSummary{{Old: dropped[0], New: added[0]}}
}

func recordE2EFinding(stateDir string, stats *Stats, in findingInput) int {
	if in.Submitted == "" {
		in.Submitted = string(in.Statement)
	}
	meta := map[string]any{
		"lane":                "e2e",
		"class":               in.Class,
		"engine":              in.Engine.Name,
		"sql_mode":            in.SQLMode,
		"sql_mode_name":       in.SQLModeName,
		"submitted_text":      in.Submitted,
		"binlog_text_differs": in.BinlogText != "" && in.BinlogText != in.Submitted,
		"info_schema_delta":   in.Delta,
		"binlog_event_hex":    hex.EncodeToString(in.RawEvent),
		"status_vars_hex":     hex.EncodeToString(in.StatusVars),
		"server_image":        in.Engine.Image,
		"oracle_digest":       nil,
		"minimized":           false,
	}
	for k, v := range in.Meta {
		meta[k] = v
	}

	stmt := slices.Clone(in.Statement)
	if min, ok := tryMinimize(stmt, in.SQLMode, in.Engine.Name); ok && len(min) > 0 {
		stmt = min
		meta["minimized"] = !bytes.Equal(min, in.Statement)
	}

	f := findings.Finding{
		Class:     in.Class,
		Engine:    in.Engine.Name,
		SQLMode:   in.SQLMode,
		Lane:      "e2e",
		Statement: stmt,
		OurSig:    in.OurSig,
		OurError:  in.OurError,
		Meta:      meta,
	}
	if sig, isNew, err := callFindingsRecord(stateDir, f); err == nil {
		meta["sig"] = sig
		if isNew && stats != nil {
			stats.IncFinding(in.Engine.Name)
		}
		return 1
	}
	if sig, err := fallbackRecordFinding(stateDir, f); err == nil {
		meta["sig"] = sig
		if stats != nil {
			stats.IncFinding(in.Engine.Name)
		}
		return 1
	}
	return 0
}

func callFindingsRecord(stateDir string, f findings.Finding) (sig string, isNew bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("findings.Record panic: %v", r)
		}
	}()
	return findings.Record(stateDir, f)
}

func tryMinimize(stmt []byte, sqlMode uint64, engine string) (out []byte, ok bool) {
	defer func() {
		if recover() != nil {
			out, ok = nil, false
		}
	}()
	out = minimize.Minimize(stmt, sqlMode, engine, func(candidate []byte) bool {
		return bytes.Equal(candidate, stmt)
	})
	return out, true
}

func fallbackRecordFinding(stateDir string, f findings.Finding) (string, error) {
	desc := fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", f.Lane, f.Class, f.SQLMode, f.Engine, f.OurSig)
	sum := sha256.Sum256([]byte(desc))
	sig := hex.EncodeToString(sum[:])[:12]
	dir := filepath.Join(stateDir, "findings", sig)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "repro.sql"), f.Statement, 0o644); err != nil {
		return "", err
	}
	meta := map[string]any{
		"sig":           sig,
		"engine":        f.Engine,
		"sql_mode":      f.SQLMode,
		"lane":          f.Lane,
		"class":         f.Class,
		"our_sig":       f.OurSig,
		"our_error":     f.OurError,
		"status":        "open",
		"discovered_at": time.Now().UTC().Format(time.RFC3339Nano),
		"minimized":     false,
	}
	for k, v := range f.Meta {
		meta[k] = v
	}
	if err := writeJSONAtomic(filepath.Join(dir, "meta.json"), meta); err != nil {
		return "", err
	}
	idx, err := os.OpenFile(filepath.Join(stateDir, "findings", "index.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err == nil {
		_ = json.NewEncoder(idx).Encode(meta)
		_ = idx.Close()
	}
	return sig, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
