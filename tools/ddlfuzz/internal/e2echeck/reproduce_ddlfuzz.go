package e2echeck

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"

	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
)

func Reproduce(in Input) (Result, error) {
	if in.Engine != "mysql" && in.Engine != "mariadb" {
		return Result{}, fmt.Errorf("bad engine %q", in.Engine)
	}
	if in.Submitted == "" {
		return Result{}, fmt.Errorf("missing submitted_text")
	}
	if in.BinlogQuery == "" {
		return Result{}, fmt.Errorf("missing binlog_query")
	}
	if in.Engine == "mariadb" {
		in.IsMariaDB = true
	}

	switch in.Class {
	case ClassStatusVarWalk, ClassSQLModeMismatch, ClassPlumbingSig:
		return reproducePlumbing(in)
	case ClassQueryRewrite:
		if IsHarnessControlQuery(in.BinlogQuery) && !EquivalentQueryText(in.Submitted, in.BinlogQuery, in.IsMariaDB) {
			return Result{Reconciled: true}, nil
		}
		if !EquivalentQueryText(in.Submitted, in.BinlogQuery, in.IsMariaDB) {
			return diverged(ClassQueryRewrite, "query-rewrite", fmt.Sprintf("submitted %d bytes, binlog %d bytes", len(in.Submitted), len(in.BinlogQuery))), nil
		}
		return Result{Reconciled: true}, nil
	case ClassPanic:
		return reproducePanic(in)
	case ClassParseErrorLiveAccept, ClassMissedColumnEffect, ClassColumnAttr, ClassPositionMissed:
		return reproduceSemantics(in)
	default:
		return Result{}, fmt.Errorf("unsupported e2e class %q", in.Class)
	}
}

func reproducePlumbing(in Input) (Result, error) {
	statusVars, err := decodeStatusVars(in.StatusVarsHex)
	if err != nil {
		return Result{}, err
	}
	mode, ok, panicked := safeSQLModeFromStatusVars(statusVars)
	if panicked != nil {
		return diverged(ClassStatusVarWalk, "status-vars", fmt.Sprint(panicked)), nil
	}
	if !ok {
		return diverged(ClassStatusVarWalk, "status-vars", "sql_mode status var not decoded"), nil
	}
	submittedMode := in.SQLMode & RelevantSQLModeMask
	if in.ExpectedRelevant != nil {
		submittedMode = *in.ExpectedRelevant & RelevantSQLModeMask
	}
	submittedMode = ExpectedEventSQLModeRelevant(in.Submitted, submittedMode, in.IsMariaDB)
	if in.Class == ClassSQLModeMismatch {
		expected := in.SQLMode & RelevantSQLModeMask
		if in.ExpectedRelevant != nil {
			expected = *in.ExpectedRelevant & RelevantSQLModeMask
		}
		expected = ExpectedEventSQLModeRelevant(in.Submitted, expected, in.IsMariaDB)
		// actual is recomputed from the status vars so a walker fix
		// reconciles here; the divergence-time value is not reused.
		if actual := mode & RelevantSQLModeMask; actual != expected {
			return diverged(ClassSQLModeMismatch, "sql-mode", fmt.Sprintf("expected relevant %d, got %d", expected, actual)), nil
		}
	}
	if in.Class == ClassPlumbingSig && IsHarnessControlQuery(in.BinlogQuery) &&
		!EquivalentQueryText(in.Submitted, in.BinlogQuery, in.IsMariaDB) {
		return Result{Reconciled: true}, nil
	}

	submittedForSig := in.Submitted
	if EquivalentQueryText(in.Submitted, in.BinlogQuery, in.IsMariaDB) {
		submittedForSig = in.BinlogQuery
	}
	liveSig, liveErr, livePanic := safeDDLSignature([]byte(in.BinlogQuery), mode, in.IsMariaDB)
	subSig, subErr, subPanic := safeDDLSignature([]byte(submittedForSig), submittedMode, in.IsMariaDB)
	if livePanic != nil || subPanic != nil {
		return diverged(ClassPanic, "parser-panic", fmt.Sprintf("live=%v submitted=%v", livePanic, subPanic)), nil
	}
	if liveSig != subSig || (liveErr != nil) != (subErr != nil) {
		return diverged(ClassPlumbingSig, "plumbing-sig", fmt.Sprintf("live_sig=%q live_err=%v submitted_sig=%q submitted_err=%v", liveSig, liveErr, subSig, subErr)), nil
	}
	return Result{Reconciled: true}, nil
}

func reproducePanic(in Input) (Result, error) {
	statusVars, err := decodeStatusVars(in.StatusVarsHex)
	if err != nil {
		return Result{}, err
	}
	mode, ok, panicked := safeSQLModeFromStatusVars(statusVars)
	if panicked != nil {
		return diverged(ClassPanic, "status-var-panic", fmt.Sprint(panicked)), nil
	}
	if !ok {
		mode = in.SQLMode
	}
	if _, _, p := safeDDLSignature([]byte(in.BinlogQuery), mode, in.IsMariaDB); p != nil {
		return diverged(ClassPanic, "parser-panic", fmt.Sprint(p)), nil
	}
	if _, _, p := safeDDLSignature([]byte(in.Submitted), in.SQLMode, in.IsMariaDB); p != nil {
		return diverged(ClassPanic, "parser-panic", fmt.Sprint(p)), nil
	}
	if _, _, p := safeParseForE2E([]byte(in.BinlogQuery), mode, in.IsMariaDB); p != nil {
		return diverged(ClassPanic, "parse-panic", fmt.Sprint(p)), nil
	}
	return Result{Reconciled: true}, nil
}

func reproduceSemantics(in Input) (Result, error) {
	parsed, err, panicked := safeParseForE2E([]byte(in.BinlogQuery), in.SQLMode, in.IsMariaDB)
	if panicked != nil {
		return diverged(ClassPanic, "parse-panic", fmt.Sprint(panicked)), nil
	}
	if err != nil {
		return diverged(ClassParseErrorLiveAccept, "parse-error-live-accept", err.Error()), nil
	}
	findings := CompareSemantics(SemanticInput{
		Before: in.Before,
		After:  in.After,
		Actual: in.Delta,
	}, parsed)
	return resultFromFindings(findings), nil
}

func resultFromFindings(findings []SemanticFinding) Result {
	if len(findings) == 0 {
		return Result{Reconciled: true}
	}
	first := findings[0]
	detail := ""
	if len(first.Meta) > 0 {
		if b, err := json.Marshal(first.Meta); err == nil {
			detail = string(b)
		}
	}
	return diverged(first.Class, first.Class, detail)
}

func diverged(class, shape, detail string) Result {
	return Result{Class: class, Shape: shape, Detail: detail}
}

func decodeStatusVars(raw string) ([]byte, error) {
	if raw == "" {
		return nil, fmt.Errorf("missing status_vars_hex")
	}
	out, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decode status_vars_hex: %w", err)
	}
	return out, nil
}

func safeSQLModeFromStatusVars(statusVars []byte) (mode uint64, ok bool, panicked any) {
	defer func() {
		if r := recover(); r != nil {
			mode, ok, panicked = 0, false, r
		}
	}()
	mode, ok = connmysql.FuzzSQLModeFromStatusVars(statusVars)
	return mode, ok, nil
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

func safeParseForE2E(query []byte, mode uint64, isMariaDB bool) (out ParsedStmts, err error, panicked any) {
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
