package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type FindingMeta struct {
	Sig               string          `json:"sig"`
	Engine            string          `json:"engine"`
	SQLMode           uint64          `json:"sql_mode"`
	SQLModeName       string          `json:"sql_mode_name,omitempty"`
	Lane              string          `json:"lane"`
	OurSig            string          `json:"our_sig"`
	OurError          string          `json:"our_error"`
	OracleDigest      json.RawMessage `json:"oracle_digest"`
	Status            string          `json:"status"`
	DiscoveredAt      string          `json:"discovered_at"`
	Minimized         bool            `json:"minimized,omitempty"`
	Class             string          `json:"class,omitempty"`
	Shape             string          `json:"shape,omitempty"`
	RediscoveredCount int             `json:"rediscovered_count,omitempty"`
	FixedBy           string          `json:"fixed_by,omitempty"`
}

type Finding struct {
	Sig      string
	Path     string
	MetaPath string
	Meta     FindingMeta
	Group    GroupInfo
}

type GroupInfo struct {
	Key   string
	Class string
	Shape string
}

type TokenUsage struct {
	Input       int64 `json:"input"`
	CachedInput int64 `json:"cached_input"`
	Output      int64 `json:"output"`
}

func (t TokenUsage) Total() int64 {
	return t.Input + t.Output
}

func (t *TokenUsage) Add(other TokenUsage) {
	t.Input += other.Input
	t.CachedInput += other.CachedInput
	t.Output += other.Output
}

type AttemptRecord struct {
	Attempt     int        `json:"attempt"`
	Sig         string     `json:"sig"`
	StartedAt   time.Time  `json:"started_at"`
	EndedAt     time.Time  `json:"ended_at"`
	Outcome     string     `json:"outcome"`
	Detail      string     `json:"detail,omitempty"`
	Tokens      TokenUsage `json:"tokens"`
	Commits     []string   `json:"commits,omitempty"`
	ThreadID    string     `json:"thread_id,omitempty"`
	Transcript  string     `json:"transcript"`
	LastMessage string     `json:"last_message"`
	Diff        string     `json:"diff,omitempty"`
}

type SpendRecord struct {
	Tokens         TokenUsage `json:"tokens"`
	Attempts       int        `json:"attempts"`
	AttemptSeconds int64      `json:"attempt_seconds"`
}

type CodexParseResult struct {
	Tokens          TokenUsage
	ThreadID        string
	AgentMessages   []string
	HadError        bool
	ResultOutcome   string
	ResultSig       string
	ResultDetail    string
	LastAgentText   string
	TerminalErrText string
}

var (
	quotedRe = regexp.MustCompile("`[^`]*`|'(?:\\\\.|[^'])*'|\"(?:\\\\.|[^\"])*\"")
	hexRe    = regexp.MustCompile(`(?i)\b[0-9a-f]{12,64}\b`)
	digitRe  = regexp.MustCompile(`\b\d+\b`)
	spaceRe  = regexp.MustCompile(`\s+`)
)

func ScanFindings(cfg Config) ([]Finding, error) {
	root := filepath.Join(cfg.StateDir, "findings")
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var findings []Finding
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		sig := ent.Name()
		metaPath := filepath.Join(root, sig, "meta.json")
		meta, err := loadFindingMeta(metaPath)
		if err != nil {
			continue
		}
		if meta.Sig == "" {
			meta.Sig = sig
		}
		findings = append(findings, Finding{
			Sig:      sig,
			Path:     filepath.Join(root, sig),
			MetaPath: metaPath,
			Meta:     meta,
			Group:    GroupInfoForMeta(meta),
		})
	}
	sort.Slice(findings, func(i, j int) bool {
		ti := parseLooseTime(findings[i].Meta.DiscoveredAt)
		tj := parseLooseTime(findings[j].Meta.DiscoveredAt)
		if ti.Equal(tj) {
			return findings[i].Sig < findings[j].Sig
		}
		return ti.Before(tj)
	})
	return findings, nil
}

func loadFindingMeta(path string) (FindingMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FindingMeta{}, err
	}
	var meta FindingMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return FindingMeta{}, err
	}
	return meta, nil
}

func writeFindingMeta(path string, meta FindingMeta) error {
	return atomicWriteJSON(path, meta, 0o644)
}

func GroupInfoForMeta(meta FindingMeta) GroupInfo {
	class := strings.TrimSpace(meta.Class)
	if class == "" {
		lowerErr := strings.ToLower(meta.OurError)
		switch {
		case strings.Contains(lowerErr, "panic"):
			class = "panic"
		case strings.Contains(lowerErr, "timeout") || strings.Contains(lowerErr, "hang"):
			class = "hang"
		case meta.OurError != "":
			class = "error-diverge"
		default:
			class = "sig-diverge"
		}
	}
	shape := strings.TrimSpace(meta.Shape)
	if shape == "" {
		if meta.OurError != "" {
			shape = NormalizeShape(meta.OurError)
		} else {
			shape = NormalizeShape(firstDiffLine(meta.OurSig, oracleSignature(meta.OracleDigest)))
		}
	} else {
		shape = NormalizeShape(shape)
	}
	return GroupInfo{Key: GroupKey(class, shape), Class: class, Shape: shape}
}

func NormalizeShape(s string) string {
	s = strings.ToLower(s)
	s = quotedRe.ReplaceAllString(s, "<id>")
	s = hexRe.ReplaceAllString(s, "<sig>")
	s = digitRe.ReplaceAllString(s, "<n>")
	s = spaceRe.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func GroupKey(class, shape string) string {
	sum := sha256.Sum256([]byte(class + "|" + shape))
	return hex.EncodeToString(sum[:])[:12]
}

func firstDiffLine(ours, oracle string) string {
	ol := strings.Split(ours, "\n")
	rl := strings.Split(oracle, "\n")
	n := len(ol)
	if len(rl) > n {
		n = len(rl)
	}
	for i := 0; i < n; i++ {
		var a, b string
		if i < len(ol) {
			a = ol[i]
		}
		if i < len(rl) {
			b = rl[i]
		}
		if a != b {
			return "our=" + a + " oracle=" + b
		}
	}
	return "no diff"
}

func oracleSignature(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil {
		for _, key := range []string{"oracle_sig", "signature", "sig"} {
			if v, ok := obj[key].(string); ok {
				return v
			}
		}
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err == nil {
		return buf.String()
	}
	return string(raw)
}

func LoadAttemptRecords(path string) ([]AttemptRecord, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var records []AttemptRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec AttemptRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, sc.Err()
}

func AppendAttemptRecord(path string, rec AttemptRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func NextAttemptNumber(records []AttemptRecord) int {
	maxAttempt := 0
	for _, rec := range records {
		if rec.Attempt > maxAttempt {
			maxAttempt = rec.Attempt
		}
	}
	return maxAttempt + 1
}

func AttemptsWall(records []AttemptRecord) time.Duration {
	var d time.Duration
	for _, rec := range records {
		if !rec.StartedAt.IsZero() && !rec.EndedAt.IsZero() && rec.EndedAt.After(rec.StartedAt) {
			d += rec.EndedAt.Sub(rec.StartedAt)
		}
	}
	return d
}

func BudgetExhausted(records []AttemptRecord, maxAttempts int, maxWall time.Duration) bool {
	if len(records) >= maxAttempts {
		return true
	}
	return AttemptsWall(records) >= maxWall
}

func ParseCodexJSONL(streamPath, lastMessagePath string) (CodexParseResult, error) {
	var parsed CodexParseResult
	if streamPath != "" {
		f, err := os.Open(streamPath)
		if err != nil {
			return parsed, err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		// Codex --json emits one JSON object per line; agent messages and embedded
		// command output routinely exceed bufio.Scanner's 64 KiB default, which would
		// error the whole parse and cause the supervisor to discard a successful fix.
		sc.Buffer(make([]byte, 0, 1<<20), 256<<20)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var evt map[string]any
			if err := json.Unmarshal(line, &evt); err != nil {
				return parsed, err
			}
			if parsed.ThreadID == "" {
				parsed.ThreadID = findStringKey(evt, "thread_id", "id")
			}
			if typ, _ := evt["type"].(string); typ == "turn.completed" {
				parsed.Tokens.Add(usageFromEvent(evt))
			} else if typ == "error" {
				parsed.HadError = true
				parsed.TerminalErrText = findStringKey(evt, "message", "text", "error")
			}
			item, _ := evt["item"].(map[string]any)
			if item != nil {
				switch itemType, _ := item["type"].(string); itemType {
				case "agent_message":
					text := findStringKey(item, "text", "message")
					if text == "" {
						text = extractContentText(item["content"])
					}
					if text != "" {
						parsed.AgentMessages = append(parsed.AgentMessages, text)
						parsed.LastAgentText = text
					}
				case "error":
					parsed.HadError = true
					parsed.TerminalErrText = findStringKey(item, "message", "text", "error")
				}
			}
		}
		if err := sc.Err(); err != nil {
			return parsed, err
		}
	}
	var resultText string
	if lastMessagePath != "" {
		if data, err := os.ReadFile(lastMessagePath); err == nil {
			resultText = string(data)
		}
	}
	if resultText == "" {
		resultText = parsed.LastAgentText
	}
	outcome, sig, detail := ParseResultLine(resultText)
	parsed.ResultOutcome = outcome
	parsed.ResultSig = sig
	parsed.ResultDetail = detail
	return parsed, nil
}

func usageFromEvent(evt map[string]any) TokenUsage {
	usage, _ := evt["usage"].(map[string]any)
	return TokenUsage{
		Input:       int64FromAny(usage["input_tokens"]),
		CachedInput: int64FromAny(usage["cached_input_tokens"]),
		Output:      int64FromAny(usage["output_tokens"]),
	}
}

func int64FromAny(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}

func findStringKey(v any, keys ...string) string {
	switch x := v.(type) {
	case map[string]any:
		for _, key := range keys {
			if s, ok := x[key].(string); ok && s != "" {
				return s
			}
		}
		for _, child := range x {
			if s := findStringKey(child, keys...); s != "" {
				return s
			}
		}
	case []any:
		for _, child := range x {
			if s := findStringKey(child, keys...); s != "" {
				return s
			}
		}
	}
	return ""
}

func extractContentText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []any:
		var parts []string
		for _, item := range x {
			if s := findStringKey(item, "text"); s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func ParseResultLine(text string) (outcome, sig, detail string) {
	lines := strings.Split(text, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(line, "RESULT:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		if fields[1] != "fixed" && fields[1] != "ledgered" && fields[1] != "failed" {
			continue
		}
		outcome = fields[1]
		sig = fields[2]
		if len(fields) > 3 {
			detail = strings.Join(fields[3:], " ")
		}
		return outcome, sig, detail
	}
	return "", "", ""
}

func renderPrompt(cfg Config, finding Finding, attemptN int, siblings []Finding, prior []AttemptRecord) (string, error) {
	data, err := os.ReadFile(cfg.PromptPath)
	if err != nil {
		return "", err
	}
	metaJSON, err := json.MarshalIndent(finding.Meta, "", "  ")
	if err != nil {
		return "", err
	}
	repls := map[string]string{
		"{SIG}":                    finding.Sig,
		"{ATTEMPT_N}":              strconv.Itoa(attemptN),
		"{META_JSON}":              string(metaJSON),
		"{SIBLINGS}":               renderSiblings(cfg, siblings),
		"{PRIOR_ATTEMPTS_SUMMARY}": renderPriorAttempts(prior),
	}
	out := string(data)
	for old, new := range repls {
		out = strings.ReplaceAll(out, old, new)
	}
	return out, nil
}

func renderSiblings(cfg Config, siblings []Finding) string {
	if len(siblings) == 0 {
		return "none"
	}
	var b strings.Builder
	for _, sib := range siblings {
		fmt.Fprintf(&b, "- %s - %s/%s - %s\n", sib.Sig, sib.Group.Class, sib.Group.Shape, relTo(cfg.Root, filepath.Join(sib.Path, "repro.sql")))
	}
	return strings.TrimRight(b.String(), "\n")
}

func renderPriorAttempts(records []AttemptRecord) string {
	if len(records) == 0 {
		return "none"
	}
	var b strings.Builder
	for _, rec := range records {
		fmt.Fprintf(&b, "- attempt %d: %s", rec.Attempt, rec.Outcome)
		if rec.Detail != "" {
			fmt.Fprintf(&b, " - %s", rec.Detail)
		}
		if rec.Diff != "" {
			fmt.Fprintf(&b, " (diff: %s)", rec.Diff)
		}
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

func SelectFinding(cfg Config, findings []Finding, parked map[string]bool, groups map[string]*GroupRecord) (Finding, []Finding, bool) {
	byGroup := make(map[string][]Finding)
	for _, f := range findings {
		if f.Meta.Status != "" && f.Meta.Status != "open" {
			continue
		}
		if parked[f.Sig] {
			continue
		}
		if g := groups[f.Group.Key]; g != nil && g.Parked {
			continue
		}
		attempts, _ := LoadAttemptRecords(filepath.Join(cfg.StateDir, "attempts", f.Sig+".jsonl"))
		if BudgetExhausted(attempts, 3, 150*time.Minute) {
			continue
		}
		byGroup[f.Group.Key] = append(byGroup[f.Group.Key], f)
	}
	var candidates []Finding
	for _, group := range byGroup {
		sort.Slice(group, func(i, j int) bool {
			return parseLooseTime(group[i].Meta.DiscoveredAt).Before(parseLooseTime(group[j].Meta.DiscoveredAt))
		})
		candidates = append(candidates, group[0])
	}
	if len(candidates) == 0 {
		return Finding{}, nil, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		return parseLooseTime(candidates[i].Meta.DiscoveredAt).Before(parseLooseTime(candidates[j].Meta.DiscoveredAt))
	})
	primary := candidates[0]
	sibs := byGroup[primary.Group.Key]
	sort.Slice(sibs, func(i, j int) bool {
		return parseLooseTime(sibs[i].Meta.DiscoveredAt).Before(parseLooseTime(sibs[j].Meta.DiscoveredAt))
	})
	var siblings []Finding
	for _, f := range sibs {
		if f.Sig == primary.Sig {
			continue
		}
		siblings = append(siblings, f)
		if len(siblings) == 8 {
			break
		}
	}
	return primary, siblings, true
}

func parseLooseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func RunFixLoop(ctx context.Context, cfg Config, restarter *FuzzerManager, e2e *E2EManager, logf func(string, ...any)) {
	ticker := time.NewTicker(cfg.FindingEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if time.Until(cfg.Deadline) <= 3*time.Hour {
				continue
			}
			if cfg.MaxTokens > 0 {
				spend, _ := LoadSpend(cfg)
				if spend.Tokens.Total() >= cfg.MaxTokens {
					_ = writeRunEscalation(cfg, "run-spend-cap.md", fmt.Sprintf("token cap exceeded: %d >= %d", spend.Tokens.Total(), cfg.MaxTokens))
					continue
				}
			}
			findings, err := ScanFindings(cfg)
			if err != nil {
				logf("findings scan failed: %v", err)
				continue
			}
			for i := range findings {
				if findings[i].Meta.Status != "open" && findings[i].Meta.Status != "parked" {
					continue
				}
				records, err := LoadAttemptRecords(filepath.Join(cfg.StateDir, "attempts", findings[i].Sig+".jsonl"))
				if err != nil {
					logf("attempt records load failed for %s: %v", findings[i].Sig, err)
					continue
				}
				if priorFixEvidence(findings[i].Meta, records) && confirmFixed(ctx, cfg, findings[i], logf) {
					findings[i].Meta.Status = "fixed"
				}
			}
			groups, _ := LoadGroups(cfg)
			parked, _ := LoadParkedList(cfg)
			_ = ApplyFlapScanAndPark(cfg, groups, findings)
			primary, siblings, ok := SelectFinding(cfg, findings, parked, groups)
			if !ok {
				continue
			}
			if err := FixOnce(ctx, cfg, primary.Sig, false, restarter, e2e, siblings, logf); err != nil {
				logf("fix attempt for %s failed: %v", primary.Sig, err)
			}
		}
	}
}

func FixOnce(ctx context.Context, cfg Config, sig string, skipFuzzer bool, restarter *FuzzerManager, e2e *E2EManager, siblings []Finding, logf func(string, ...any)) error {
	findings, err := ScanFindings(cfg)
	if err != nil {
		return err
	}
	var finding Finding
	for _, f := range findings {
		if f.Sig == sig {
			finding = f
			break
		}
	}
	if finding.Sig == "" {
		return fmt.Errorf("finding %s not found", sig)
	}
	attemptLog := filepath.Join(cfg.StateDir, "attempts", sig+".jsonl")
	records, err := LoadAttemptRecords(attemptLog)
	if err != nil {
		return err
	}
	attemptN := NextAttemptNumber(records)
	if priorFixEvidence(finding.Meta, records) && confirmFixed(ctx, cfg, finding, logf) {
		return nil
	}
	if BudgetExhausted(records, 3, 150*time.Minute) {
		return ParkSignature(cfg, finding, false, "attempt budget exhausted")
	}

	lastGood, err := loadLastGoodCommit(cfg)
	if err != nil {
		head, headErr := GitHead(ctx, cfg)
		if headErr != nil {
			return err
		}
		lastGood = head
	}
	if err := ensureAttemptGitReady(ctx, cfg, lastGood); err != nil {
		_ = writeRunEscalation(cfg, "run-git-drift.md", err.Error())
		return err
	}
	beforeUntracked, _ := GitUntrackedSet(ctx, cfg)
	started := time.Now()
	streamPath := filepath.Join(cfg.StateDir, "attempts", fmt.Sprintf("%s.attempt%d.stream.jsonl", sig, attemptN))
	lastPath := filepath.Join(cfg.StateDir, "attempts", fmt.Sprintf("%s.attempt%d.last.txt", sig, attemptN))
	stderrPath := filepath.Join(cfg.StateDir, "attempts", fmt.Sprintf("%s.attempt%d.stderr", sig, attemptN))
	diffPath := filepath.Join(cfg.StateDir, "attempts", fmt.Sprintf("%s.attempt%d.diff", sig, attemptN))

	prompt, err := renderPrompt(cfg, finding, attemptN, siblings, records)
	if err != nil {
		return err
	}
	outcome := "failed"
	detail := ""
	var tokens TokenUsage
	var threadID string
	var commits []string
	if logf != nil {
		logf("starting codex attempt %d for %s", attemptN, sig)
	}
	codexExit, timedOut, err := runCodexAttempt(ctx, cfg, prompt, streamPath, lastPath, stderrPath)
	ended := time.Now()
	if timedOut {
		outcome = "timeout"
		detail = "codex attempt timed out"
	} else if err != nil || codexExit != 0 {
		outcome = "failed"
		detail = fmt.Sprintf("codex exit=%d err=%v", codexExit, err)
	}
	parsed, parseErr := ParseCodexJSONL(streamPath, lastPath)
	if parseErr == nil {
		tokens = parsed.Tokens
		threadID = parsed.ThreadID
		if parsed.HadError && detail == "" {
			detail = parsed.TerminalErrText
			outcome = "failed"
		}
		if outcome == "failed" && detail == "" && parsed.ResultOutcome == "" {
			outcome = "no_result"
			detail = "missing RESULT line"
		}
		if parsed.ResultOutcome != "" && outcome != "timeout" && codexExit == 0 && !parsed.HadError {
			outcome = parsed.ResultOutcome
			detail = parsed.ResultDetail
		}
	} else if detail == "" {
		outcome = "failed"
		detail = parseErr.Error()
	}
	if spendErr := AddSpend(cfg, tokens, ended.Sub(started)); spendErr != nil && logf != nil {
		logf("spend accounting failed: %v", spendErr)
	}

	if outcome == "failed" || outcome == "timeout" || outcome == "no_result" {
		_ = rollbackAttempt(ctx, cfg, lastGood, beforeUntracked)
		rec := AttemptRecord{Attempt: attemptN, Sig: sig, StartedAt: started, EndedAt: ended, Outcome: outcome, Detail: detail, Tokens: tokens, ThreadID: threadID, Transcript: relTo(cfg.StateDir, streamPath), LastMessage: relTo(cfg.StateDir, lastPath), Diff: relTo(cfg.StateDir, diffPath)}
		_ = AppendAttemptRecord(attemptLog, rec)
		records = append(records, rec)
		if BudgetExhausted(records, 3, 150*time.Minute) {
			return ParkSignature(cfg, finding, false, "attempt budget exhausted")
		}
		return nil
	}

	_ = GitSaveDiff(ctx, cfg, lastGood, diffPath)
	commitsSince, err := GitCommitsSince(ctx, cfg, lastGood)
	if err != nil {
		outcome, detail = "failed", err.Error()
	} else {
		for _, c := range commitsSince {
			commits = append(commits, c.SHA)
		}
		if len(commitsSince) < 1 || len(commitsSince) > 3 {
			outcome, detail = "forbidden_paths", fmt.Sprintf("commit count %d outside 1..3", len(commitsSince))
		}
	}
	var paths []string
	if outcome == "fixed" || outcome == "ledgered" {
		paths, err = GitTouchedPaths(ctx, cfg, lastGood)
		if err != nil {
			outcome, detail = "failed", err.Error()
		} else if bad := forbiddenTouchedPaths(paths, sig); len(bad) > 0 {
			outcome, detail = "forbidden_paths", strings.Join(bad, ", ")
		}
	}
	if outcome == "fixed" || outcome == "ledgered" {
		if err := RunGate(ctx, cfg); err != nil {
			outcome, detail = "gate_failed", err.Error()
		}
	}
	if outcome == "fixed" || outcome == "ledgered" {
		if err := validateReplayResolution(ctx, cfg, sig, outcome); err != nil {
			outcome, detail = "replay_failed", err.Error()
		}
	}
	if outcome == "fixed" || outcome == "ledgered" {
		engines := touchedOracleEngines(paths)
		if len(engines) > 0 {
			if err := RebuildOracles(ctx, cfg, engines); err != nil {
				outcome, detail = "golden_failed", err.Error()
			}
		}
		if (outcome == "fixed" || outcome == "ledgered") && touchedParserOrOracle(paths) {
			if err := RunGolden(ctx, cfg); err != nil {
				outcome, detail = "golden_failed", err.Error()
			}
		}
	}
	if outcome == "fixed" || outcome == "ledgered" {
		if err := preflightReplayAll(ctx, cfg); err != nil {
			outcome, detail = "regression", err.Error()
		}
	}
	if outcome == "fixed" || outcome == "ledgered" {
		head, err := GitHead(ctx, cfg)
		if err != nil {
			outcome, detail = "failed", err.Error()
		} else if err := atomicWriteFile(filepath.Join(cfg.StateDir, "last_good_commit"), []byte(head+"\n"), 0o644); err != nil {
			outcome, detail = "failed", err.Error()
		} else {
			RecordGroupFix(cfg, finding.Group.Key, sig)
			if err := applySuccessfulStatuses(ctx, cfg, sig, outcome); err != nil && logf != nil {
				logf("status update after %s failed: %v", sig, err)
			}
			if err := rebuildAndHotRestart(ctx, cfg, skipFuzzer, restarter); err != nil {
				outcome, detail = "failed", err.Error()
			} else {
				if touchesE2EBinary(paths) {
					if err := rebuildAndHotRestartE2E(ctx, cfg, e2e); err != nil {
						if logf != nil {
							logf("e2e rebuild after %s failed: %v", sig, err)
						}
						_ = writeRunEscalation(cfg, "run-e2e-rebuild-failed.md", err.Error())
					}
				}
				if outcome == "fixed" {
					_ = enqueueE2EConfirmation(cfg, finding)
				}
			}
		}
	}
	if outcome != "fixed" && outcome != "ledgered" {
		_ = rollbackAttempt(ctx, cfg, lastGood, beforeUntracked)
	}
	rec := AttemptRecord{Attempt: attemptN, Sig: sig, StartedAt: started, EndedAt: time.Now(), Outcome: outcome, Detail: detail, Tokens: tokens, Commits: commits, ThreadID: threadID, Transcript: relTo(cfg.StateDir, streamPath), LastMessage: relTo(cfg.StateDir, lastPath), Diff: relTo(cfg.StateDir, diffPath)}
	_ = AppendAttemptRecord(attemptLog, rec)
	records = append(records, rec)
	if outcome != "fixed" && outcome != "ledgered" && BudgetExhausted(records, 3, 150*time.Minute) {
		return ParkSignature(cfg, finding, false, "attempt budget exhausted")
	}
	return nil
}

func runCodexAttempt(ctx context.Context, cfg Config, prompt, streamPath, lastPath, stderrPath string) (exit int, timedOut bool, err error) {
	if err := os.MkdirAll(filepath.Dir(streamPath), 0o755); err != nil {
		return -1, false, err
	}
	stream, err := os.Create(streamPath)
	if err != nil {
		return -1, false, err
	}
	defer stream.Close()
	stderr, err := os.Create(stderrPath)
	if err != nil {
		return -1, false, err
	}
	defer stderr.Close()
	args := codexArgs(cfg, "workspace-write", lastPath)
	p, err := StartWithStdin(cfg.Root, strings.NewReader(prompt), stream, stderr, "codex", args...)
	if err != nil {
		return -1, false, err
	}
	timer := time.NewTimer(cfg.AttemptTO)
	defer timer.Stop()
	select {
	case <-p.Done():
		waitErr := p.Err()
		return exitCode(waitErr), false, waitErr
	case <-timer.C:
		p.Kill()
		return -1, true, errors.New("codex attempt timed out")
	case <-ctx.Done():
		p.Kill()
		return -1, false, ctx.Err()
	}
}

func codexArgs(cfg Config, sandbox, lastPath string) []string {
	args := []string{"exec", "-C", cfg.Root}
	if cfg.CodexBypass && sandbox == "workspace-write" {
		args = append(args, "--dangerously-bypass-approvals-and-sandbox")
	} else {
		args = append(args,
			"-s", sandbox,
			"-c", "approval_policy=never",
			"-c", "sandbox_workspace_write.network_access=true",
			"--add-dir", cfg.GoCache,
			"--add-dir", cfg.GoModCache,
			"--add-dir", cfg.LintCache,
		)
	}
	args = append(args,
		"--ignore-user-config", "--ignore-rules", "--skip-git-repo-check",
		"-m", cfg.FixModel, "-c", "model_reasoning_effort=xhigh",
		"--json", "-o", lastPath,
		"-",
	)
	return args
}

func ensureAttemptGitReady(ctx context.Context, cfg Config, lastGood string) error {
	head, err := GitHead(ctx, cfg)
	if err != nil {
		return err
	}
	if head != lastGood {
		return fmt.Errorf("HEAD drift: got %s, want last_good_commit %s", head, lastGood)
	}
	clean, dirty, err := GitTrackedClean(ctx, cfg)
	if err != nil {
		return err
	}
	if !clean {
		return fmt.Errorf("tracked tree dirty before attempt:\n%s", dirty)
	}
	baseline, err := loadUntrackedBaseline(cfg)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		current, err := GitUntrackedSet(ctx, cfg)
		if err != nil {
			return err
		}
		if err := writeUntrackedBaseline(cfg, current); err != nil {
			return err
		}
		baseline = current
	}
	current, err := GitUntrackedSet(ctx, cfg)
	if err != nil {
		return err
	}
	for p := range current {
		if _, ok := baseline[p]; ok {
			continue
		}
		if strings.HasPrefix(p, "tools/ddlfuzz/state/") || strings.HasPrefix(p, "tools/ddlfuzz/build/") {
			continue
		}
		return fmt.Errorf("unexpected untracked file before attempt: %s", p)
	}
	return nil
}

func rollbackAttempt(ctx context.Context, cfg Config, lastGood string, before pathSet) error {
	if err := GitResetHard(ctx, cfg, lastGood); err != nil {
		return err
	}
	after, err := GitUntrackedSet(ctx, cfg)
	if err != nil {
		return err
	}
	_, err = DeleteNewUntracked(cfg, before, after)
	return err
}

func loadLastGoodCommit(cfg Config) (string, error) {
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "last_good_commit"))
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(string(data))
	if sha == "" {
		return "", errors.New("last_good_commit is empty")
	}
	return sha, nil
}

func validateReplayResolution(ctx context.Context, cfg Config, sig, outcome string) error {
	res, err := RunReplay(ctx, cfg, sig)
	if outcome == "fixed" {
		if err != nil || res.ExitCode != 0 {
			if err == nil {
				err = fmt.Errorf("exit code %d", res.ExitCode)
			}
			return fmt.Errorf("replay %s did not reconcile: %w\n%s", sig, err, resultOutputTail(res, 4000))
		}
		return nil
	}
	if outcome == "ledgered" {
		meta, err := loadFindingMeta(filepath.Join(cfg.StateDir, "findings", sig, "meta.json"))
		if err != nil {
			return err
		}
		if meta.Status != "ledgered" {
			return fmt.Errorf("agent claimed ledgered but meta status=%q", meta.Status)
		}
		if !ledgerHasCitation(cfg, sig) {
			return fmt.Errorf("ledger entry for %s missing non-empty citation", sig)
		}
		if err != nil && res.ExitCode != 10 {
			return fmt.Errorf("ledgered replay exit=%d: %w", res.ExitCode, err)
		}
		return nil
	}
	return fmt.Errorf("unknown resolution %q", outcome)
}

func priorFixEvidence(meta FindingMeta, records []AttemptRecord) bool {
	if meta.FixedBy != "" {
		return true
	}
	for _, rec := range records {
		if rec.Outcome == "fixed" {
			return true
		}
	}
	return false
}

func confirmFixed(ctx context.Context, cfg Config, f Finding, logf func(string, ...any)) bool {
	res, err := RunReplay(ctx, cfg, f.Sig)
	if err != nil || res.ExitCode != 0 {
		return false
	}
	f.Meta.Status = "fixed"
	if err := writeFindingMeta(f.MetaPath, f.Meta); err != nil {
		if logf != nil {
			logf("confirm-fixed meta update for %s failed: %v", f.Sig, err)
		}
		return false
	}
	if logf != nil {
		logf("confirm-fixed %s", f.Sig)
	}
	return true
}

func RunReplay(ctx context.Context, cfg Config, sig string) (Result, error) {
	return RunTimeout(ctx, cfg.DDLDir, 10*time.Minute, nil, cfg.DDLfuzzBin, "replay", sig)
}

func ledgerHasCitation(cfg Config, sig string) bool {
	f, err := os.Open(filepath.Join(cfg.StateDir, "ledger.jsonl"))
	if err != nil {
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var obj map[string]any
		if json.Unmarshal(sc.Bytes(), &obj) != nil {
			continue
		}
		if obj["sig"] == sig {
			if citation, _ := obj["citation"].(string); strings.TrimSpace(citation) != "" {
				return true
			}
		}
	}
	return false
}

func preflightReplayAll(ctx context.Context, cfg Config) error {
	findings, err := ScanFindings(cfg)
	if err != nil {
		return err
	}
	for _, f := range findings {
		status := f.Meta.Status
		res, err := RunReplay(ctx, cfg, f.Sig)
		switch status {
		case "fixed":
			if err != nil || res.ExitCode != 0 {
				return fmt.Errorf("fixed finding %s regressed: exit=%d err=%v", f.Sig, res.ExitCode, err)
			}
		case "ledgered", "parked":
			if res.ExitCode == 11 || res.ExitCode == 1 {
				return fmt.Errorf("%s finding %s cannot be evaluated: exit=%d", status, f.Sig, res.ExitCode)
			}
		default:
			if res.ExitCode == 11 || res.ExitCode == 1 {
				return fmt.Errorf("open finding %s cannot be evaluated: exit=%d", f.Sig, res.ExitCode)
			}
		}
	}
	return nil
}

func applySuccessfulStatuses(ctx context.Context, cfg Config, primarySig, outcome string) error {
	findings, err := ScanFindings(cfg)
	if err != nil {
		return err
	}
	for _, f := range findings {
		if f.Sig == primarySig {
			f.Meta.Status = outcome
			if err := writeFindingMeta(f.MetaPath, f.Meta); err != nil {
				return err
			}
			continue
		}
		if f.Meta.Status != "" && f.Meta.Status != "open" {
			continue
		}
		res, err := RunReplay(ctx, cfg, f.Sig)
		if err == nil && res.ExitCode == 0 {
			f.Meta.Status = "fixed"
			f.Meta.FixedBy = primarySig
			if err := writeFindingMeta(f.MetaPath, f.Meta); err != nil {
				return err
			}
		}
	}
	return nil
}

func rebuildAndHotRestart(ctx context.Context, cfg Config, skipFuzzer bool, restarter *FuzzerManager) error {
	newBin := filepath.Join(cfg.BuildDir, "ddlfuzz.new")
	res, err := RunTimeout(ctx, cfg.DDLDir, 10*time.Minute, nil, "go", "build", "-tags", "ddlfuzz", "-o", newBin, "./cmd/ddlfuzz")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("rebuild ddlfuzz.new failed: %w\n%s", err, resultOutputTail(res, 8000))
	}
	if skipFuzzer || restarter == nil {
		return os.Rename(newBin, cfg.DDLfuzzBin)
	}
	return restarter.HotRestart(ctx, newBin)
}

func rebuildAndHotRestartE2E(ctx context.Context, cfg Config, e2e *E2EManager) error {
	newBin := filepath.Join(cfg.BuildDir, "ddlfuzz-e2e.new")
	res, err := RunTimeout(ctx, cfg.DDLDir, 10*time.Minute, nil, "go", "build", "-tags", "ddlfuzz", "-o", newBin, "./cmd/ddlfuzz-e2e")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("rebuild ddlfuzz-e2e.new failed: %w\n%s", err, resultOutputTail(res, 8000))
	}
	if e2e == nil {
		return os.Rename(newBin, cfg.E2EBin)
	}
	return e2e.HotRestart(ctx, newBin)
}

func enqueueE2EConfirmation(cfg Config, finding Finding) error {
	repro, err := os.ReadFile(filepath.Join(finding.Path, "repro.sql"))
	if err != nil {
		return err
	}
	msg := map[string]any{
		"sig":           finding.Sig,
		"engine":        finding.Meta.Engine,
		"sql_mode":      finding.Meta.SQLMode,
		"sql_mode_name": finding.Meta.SQLModeName,
		"statement":     string(repro),
	}
	return atomicWriteJSON(filepath.Join(cfg.StateDir, "e2e-queue", "pending", finding.Sig+".json"), msg, 0o644)
}

func LoadSpend(cfg Config) (SpendRecord, error) {
	var spend SpendRecord
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "spend.json"))
	if os.IsNotExist(err) {
		return spend, nil
	}
	if err != nil {
		return spend, err
	}
	return spend, json.Unmarshal(data, &spend)
}

func AddSpend(cfg Config, tokens TokenUsage, wall time.Duration) error {
	spend, err := LoadSpend(cfg)
	if err != nil {
		return err
	}
	spend.Tokens.Add(tokens)
	spend.Attempts++
	spend.AttemptSeconds += int64(wall.Seconds())
	return atomicWriteJSON(filepath.Join(cfg.StateDir, "spend.json"), spend, 0o644)
}

func copyFile(dst string, src io.Reader) error {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, src); err != nil {
		return err
	}
	return atomicWriteFile(dst, buf.Bytes(), 0o644)
}
