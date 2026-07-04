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
	"time"

	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/e2echeck"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/findings"
	"github.com/go-mysql-org/go-mysql/replication"
)

type parsedStmts = e2echeck.ParsedStmts
type parsedStmt = e2echeck.ParsedStmt
type parsedSpec = e2echeck.ParsedSpec
type parsedCol = e2echeck.ParsedCol
type parsedPair = e2echeck.ParsedPair
type semanticFinding = e2echeck.SemanticFinding

type findingInput struct {
	Class       string
	Engine      engineConfig
	Statement   []byte
	SQLMode     uint64
	SQLModeName string
	OurSig      string
	OurError    string
	Delta       delta
	Before      snapshot
	After       snapshot
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
	expectedRelevant := e2echeck.ExpectedEventSQLModeRelevant(exp.Submitted, exp.SQLModeRelevant, ec.IsMariaDB)

	mode, ok := safeSQLModeFromStatusVars(statusVars)
	if !ok {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-statusvar-walk",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     0,
			SQLModeName: exp.SQLModeName,
			Delta:       actualDelta,
			Before:      exp.Before,
			After:       exp.After,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta: map[string]any{
				"status_vars_hex":   hex.EncodeToString(statusVars),
				"expected_relevant": expectedRelevant,
			},
		})
	}
	if ok && mode&relevantMask != expectedRelevant {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-sqlmode-mismatch",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     mode,
			SQLModeName: exp.SQLModeName,
			Delta:       actualDelta,
			Before:      exp.Before,
			After:       exp.After,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta: map[string]any{
				"expected_relevant": expectedRelevant,
				"actual_relevant":   mode & relevantMask,
			},
		})
	}

	if !e2echeck.EquivalentQueryText(exp.Submitted, string(query), ec.IsMariaDB) {
		findingCount += recordE2EFinding(stateDir, stats, findingInput{
			Class:       "e2e-query-rewrite",
			Engine:      ec,
			Statement:   []byte(exp.Submitted),
			SQLMode:     mode,
			SQLModeName: exp.SQLModeName,
			Delta:       actualDelta,
			Before:      exp.Before,
			After:       exp.After,
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
	subSig, subErr, subPanic := safeDDLSignature([]byte(exp.Submitted), expectedRelevant, ec.IsMariaDB)
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
			Before:      exp.Before,
			After:       exp.After,
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
			Before:      exp.Before,
			After:       exp.After,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta: map[string]any{
				"submitted_sig":     subSig,
				"submitted_error":   errString(subErr),
				"live_sig":          liveSig,
				"live_error":        errString(liveErr),
				"expected_relevant": expectedRelevant,
				"actual_relevant":   mode & relevantMask,
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
			Before:      exp.Before,
			After:       exp.After,
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
			Before:      exp.Before,
			After:       exp.After,
			RawEvent:    raw,
			StatusVars:  statusVars,
			Submitted:   exp.Submitted,
			BinlogText:  string(query),
			Meta:        map[string]any{"schema_changed": !actualDelta.Empty()},
		})
	} else {
		for _, sf := range e2echeck.CompareSemantics(e2echeck.SemanticInput{
			Before: exp.Before,
			After:  exp.After,
			Actual: actualDelta,
		}, parsed) {
			findingCount += recordE2EFinding(stateDir, stats, findingInput{
				Class:       sf.Class,
				Engine:      ec,
				Statement:   []byte(exp.Submitted),
				SQLMode:     mode,
				SQLModeName: exp.SQLModeName,
				OurSig:      liveSig,
				OurError:    errString(liveErr),
				Delta:       actualDelta,
				Before:      exp.Before,
				After:       exp.After,
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
	if in.BinlogText == "" {
		in.BinlogText = in.Submitted
	}
	if in.Before == nil {
		in.Before = snapshot{}
	}
	if in.After == nil {
		in.After = snapshot{}
	}
	meta := map[string]any{
		"lane":                "e2e",
		"class":               in.Class,
		"engine":              in.Engine.Name,
		"sql_mode":            in.SQLMode,
		"sql_mode_name":       in.SQLModeName,
		"submitted_text":      in.Submitted,
		"binlog_query":        in.BinlogText,
		"binlog_text":         in.BinlogText,
		"binlog_text_differs": in.BinlogText != in.Submitted,
		"info_schema_delta":   in.Delta,
		"before_snapshot":     in.Before,
		"after_snapshot":      in.After,
		"binlog_event_hex":    hex.EncodeToString(in.RawEvent),
		"status_vars_hex":     hex.EncodeToString(in.StatusVars),
		"server_image":        in.Engine.Image,
		"oracle_digest":       nil,
		"minimized":           false,
	}
	for k, v := range in.Meta {
		if isE2ERequiredMeta(k) {
			continue
		}
		meta[k] = v
	}
	if _, ok := meta["shape"]; !ok {
		if shape := e2echeck.ShapeFor(in.Class, meta); shape != "" {
			meta["shape"] = shape
		}
	}

	stmt := slices.Clone(in.Statement)

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

func isE2ERequiredMeta(key string) bool {
	switch key {
	case "lane", "class", "engine", "sql_mode", "sql_mode_name",
		"submitted_text", "binlog_query", "binlog_text_differs",
		"status_vars_hex", "info_schema_delta", "before_snapshot",
		"after_snapshot", "server_image", "oracle_digest":
		return true
	default:
		return false
	}
}

func callFindingsRecord(stateDir string, f findings.Finding) (sig string, isNew bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("findings.Record panic: %v", r)
		}
	}()
	return findings.Record(stateDir, f)
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
