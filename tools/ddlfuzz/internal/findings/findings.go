package findings

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

// Finding is one divergence to record. Class must be from the D4 vocabulary
// (00-overview). Meta is merged verbatim into meta.json (additive keys).
type Finding struct {
	Class     string
	Engine    string // "mysql" | "mariadb"
	SQLMode   uint64
	Lane      string // "fast" | "e2e"
	Statement []byte
	OurSig    string
	OurError  string
	Meta      map[string]any
}

var validClasses = map[string]struct{}{
	"sig_mismatch":                {},
	"we_error":                    {},
	"panic":                       {},
	"timeout":                     {},
	"oracle_crash":                {},
	"oracle_timeout":              {},
	"e2e-statusvar-walk":          {},
	"e2e-sqlmode-mismatch":        {},
	"e2e-plumbing-sig":            {},
	"e2e-panic":                   {},
	"e2e-query-rewrite":           {},
	"e2e-parse-error-live-accept": {},
	"e2e-missed-column-effect":    {},
	"e2e-col-attr":                {},
	"e2e-position-missed":         {},
	"oracle-reject-live-accept":   {},
}

// Record computes the canonical descriptor + <sig>, dedups against ledger.jsonl /
// parked.list, and writes state/findings/<sig>/{repro.sql,meta.json} atomically.
// isNew is false when the sig already existed (times_seen bumped).
func Record(stateDir string, f Finding) (sig string, isNew bool, err error) {
	if _, ok := validClasses[f.Class]; !ok {
		return "", false, fmt.Errorf("ddlfuzz findings: invalid class %q", f.Class)
	}
	if f.Engine != "mysql" && f.Engine != "mariadb" {
		return "", false, fmt.Errorf("ddlfuzz findings: invalid engine %q", f.Engine)
	}
	if f.Lane == "" {
		f.Lane = "fast"
	}
	if f.Lane != "fast" && f.Lane != "e2e" {
		return "", false, fmt.Errorf("ddlfuzz findings: invalid lane %q", f.Lane)
	}
	shape := shapeFromFinding(f)
	desc := compare.Descriptor{
		V:       1,
		Engine:  f.Engine,
		SQLMode: compare.MaskSQLMode(f.SQLMode),
		Class:   f.Class,
		Lane:    f.Lane,
		Shape:   shape,
	}
	sig = compare.DescriptorSig(desc)
	if suppressed, err := isSuppressed(stateDir, sig); err != nil {
		return "", false, err
	} else if suppressed {
		return sig, false, nil
	}

	dir := filepath.Join(stateDir, "findings", sig)
	metaPath := filepath.Join(dir, "meta.json")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", false, err
	}

	meta := map[string]any{}
	existed := false
	if b, err := os.ReadFile(metaPath); err == nil {
		existed = true
		_ = json.Unmarshal(b, &meta)
	} else if !os.IsNotExist(err) {
		return "", false, err
	}

	timesSeen := intFromAny(meta["times_seen"]) + 1
	now := time.Now().UTC().Format(time.RFC3339)
	status, _ := meta["status"].(string)
	rewriteRepro := !existed || status == "fixed"
	if !existed {
		if err := writeAtomic(filepath.Join(dir, "repro.sql"), f.Statement, 0o644); err != nil {
			return "", false, err
		}
		meta["discovered_at"] = now
		meta["status"] = "open"
		meta["minimized"] = false
	} else if status == "fixed" {
		if err := archiveRepro(dir); err != nil {
			return "", false, err
		}
		if err := writeAtomic(filepath.Join(dir, "repro.sql"), f.Statement, 0o644); err != nil {
			return "", false, err
		}
		meta["status"] = "open"
		meta["reopened_count"] = intFromAny(meta["reopened_count"]) + 1
	}

	meta["sig"] = sig
	meta["engine"] = f.Engine
	meta["sql_mode"] = f.SQLMode
	meta["lane"] = f.Lane
	meta["class"] = f.Class
	meta["descriptor_v"] = 1
	meta["times_seen"] = timesSeen
	meta["last_seen_at"] = now
	meta["descriptor"] = json.RawMessage(compare.DescriptorBytes(desc))
	if rewriteRepro {
		meta["our_sig"] = f.OurSig
		meta["our_error"] = f.OurError
		meta["shape"] = shape
		for k, v := range f.Meta {
			if k == "sig" || k == "times_seen" || k == "descriptor" {
				continue
			}
			meta[k] = v
		}
	}
	if _, ok := meta["status"]; !ok {
		meta["status"] = "open"
	}

	mb, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return "", false, err
	}
	mb = append(mb, '\n')
	if err := writeAtomic(metaPath, mb, 0o644); err != nil {
		return "", false, err
	}
	if !existed {
		if err := appendIndex(stateDir, sig, f.Class); err != nil {
			return "", false, err
		}
	}
	return sig, !existed, nil
}

func archiveRepro(dir string) error {
	repro := filepath.Join(dir, "repro.sql")
	if _, err := os.Stat(repro); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(dir, "repro.prev3.sql"))
	for i := 2; i >= 1; i-- {
		oldPath := filepath.Join(dir, fmt.Sprintf("repro.prev%d.sql", i))
		newPath := filepath.Join(dir, fmt.Sprintf("repro.prev%d.sql", i+1))
		if err := os.Rename(oldPath, newPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.Rename(repro, filepath.Join(dir, "repro.prev1.sql"))
}

func shapeFromFinding(f Finding) string {
	if f.Meta != nil {
		if s, ok := f.Meta["shape"].(string); ok && s != "" {
			return s
		}
	}
	switch f.Class {
	case "we_error":
		return compare.NormalizeError(f.OurError)
	case "timeout", "oracle_crash", "oracle_timeout":
		return "head=" + compare.HeadWord(f.Statement)
	case "panic":
		return compare.NormalizeError(f.OurError)
	default:
		if f.OurError != "" {
			return compare.NormalizeError(f.OurError)
		}
		return "unspecified"
	}
}

func isSuppressed(stateDir, sig string) (bool, error) {
	parked, err := readParked(filepath.Join(stateDir, "parked.list"))
	if err != nil {
		return false, err
	}
	if parked[sig] {
		return true, nil
	}
	ledgered, err := readLedger(filepath.Join(stateDir, "ledger.jsonl"))
	if err != nil {
		return false, err
	}
	return ledgered[sig], nil
}

func readParked(path string) (map[string]bool, error) {
	out := map[string]bool{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			out[line] = true
		}
	}
	return out, sc.Err()
}

func readLedger(path string) (map[string]bool, error) {
	out := map[string]bool{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var row map[string]any
		if json.Unmarshal([]byte(line), &row) == nil {
			if s, ok := row["sig"].(string); ok && s != "" {
				out[s] = true
				continue
			}
		}
		fields := strings.Fields(line)
		if len(fields) > 0 && len(fields[0]) == 12 {
			out[fields[0]] = true
		}
	}
	return out, sc.Err()
}

func appendIndex(stateDir, sig, class string) error {
	dir := filepath.Join(stateDir, "findings")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "index.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	row := map[string]any{"sig": sig, "class": class, "ts": time.Now().UTC().Format(time.RFC3339)}
	b, _ := json.Marshal(row)
	_, err = f.Write(append(b, '\n'))
	return err
}

func writeAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

func intFromAny(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case json.Number:
		i, _ := x.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(x)
		return i
	default:
		return 0
	}
}

func FindingFromDivergence(c run.Case, div *compare.Divergence, digestJSON json.RawMessage) Finding {
	engine := run.EngineName(c.Engine)
	meta := map[string]any{
		"shape":         div.Shape,
		"origin":        run.OriginName(c.Origin),
		"seed":          fmt.Sprintf("0x%x", c.Seed),
		"oracle_digest": digestJSON,
	}
	return Finding{
		Class:     div.Class,
		Engine:    engine,
		SQLMode:   c.SQLMode,
		Lane:      "fast",
		Statement: c.SQL,
		OurSig:    div.OurSig,
		OurError:  div.OurError,
		Meta:      meta,
	}
}
