package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type GroupRecord struct {
	Sigs      []string `json:"sigs"`
	FixCount  int      `json:"fix_count"`
	LastFixTS string   `json:"last_fix_ts,omitempty"`
	Parked    bool     `json:"parked,omitempty"`
}

type GroupedFinding struct {
	Sig           string
	GroupKey      string
	Status        string
	ReopenedCount int
	Reproduces    bool
}

func LoadParkedList(cfg Config) (map[string]bool, error) {
	out := make(map[string]bool)
	f, err := os.Open(filepath.Join(cfg.StateDir, "parked.list"))
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		sig := strings.TrimSpace(sc.Text())
		if sig != "" {
			out[sig] = true
		}
	}
	return out, sc.Err()
}

func appendParked(cfg Config, sig string) error {
	parked, err := LoadParkedList(cfg)
	if err != nil {
		return err
	}
	if parked[sig] {
		return nil
	}
	path := filepath.Join(cfg.StateDir, "parked.list")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := fmt.Fprintln(f, sig); err != nil {
		return err
	}
	return f.Sync()
}

func LoadGroups(cfg Config) (map[string]*GroupRecord, error) {
	groups := make(map[string]*GroupRecord)
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "groups.json"))
	if os.IsNotExist(err) {
		return groups, nil
	}
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return groups, nil
	}
	return groups, json.Unmarshal(data, &groups)
}

func SaveGroups(cfg Config, groups map[string]*GroupRecord) error {
	return atomicWriteJSON(filepath.Join(cfg.StateDir, "groups.json"), groups, 0o644)
}

func RecordGroupFix(cfg Config, groupKey, sig string) {
	groups, _ := LoadGroups(cfg)
	g := groups[groupKey]
	if g == nil {
		g = &GroupRecord{}
		groups[groupKey] = g
	}
	if !containsString(g.Sigs, sig) {
		g.Sigs = append(g.Sigs, sig)
		sort.Strings(g.Sigs)
	}
	g.FixCount++
	g.LastFixTS = time.Now().UTC().Format(time.RFC3339)
	_ = SaveGroups(cfg, groups)
}

func ApplyFlapDetector(groups map[string]*GroupRecord, findings []GroupedFinding) []string {
	frozen := make(map[string]bool)
	for _, f := range findings {
		g := groups[f.GroupKey]
		if g == nil {
			g = &GroupRecord{}
			groups[f.GroupKey] = g
		}
		previouslyKnown := containsString(g.Sigs, f.Sig)
		if !previouslyKnown {
			g.Sigs = append(g.Sigs, f.Sig)
			sort.Strings(g.Sigs)
		}
		open := f.Status == "" || f.Status == "open"
		if open && g.FixCount >= 2 && (!previouslyKnown || f.ReopenedCount > 0) && f.Reproduces {
			wasParked := g.Parked
			g.Parked = true
			if !wasParked {
				frozen[f.GroupKey] = true
			}
		}
	}
	out := make([]string, 0, len(frozen))
	for groupKey := range frozen {
		out = append(out, groupKey)
	}
	sort.Strings(out)
	return out
}

func flapCandidate(groups map[string]*GroupRecord, f Finding) bool {
	g := groups[f.Group.Key]
	if g == nil || g.FixCount < 2 {
		return false
	}
	open := f.Meta.Status == "" || f.Meta.Status == "open"
	if !open {
		return false
	}
	return !containsString(g.Sigs, f.Sig) || f.Meta.ReopenedCount > 0
}

func replayReproduces(cfg Config, sig string) bool {
	res, err := RunReplay(context.Background(), cfg, sig)
	return res.ExitCode == 10 && (err == nil || err != context.Canceled)
}

func runFlapEscalationName(groupKey string) string {
	groupKey = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_':
			return r
		default:
			return '-'
		}
	}, groupKey)
	return "run-flap-" + groupKey + ".md"
}

func ApplyFlapScanAndPark(cfg Config, groups map[string]*GroupRecord, findings []Finding) error {
	grouped := make([]GroupedFinding, 0, len(findings))
	for _, f := range findings {
		reproduces := false
		if flapCandidate(groups, f) {
			reproduces = replayReproduces(cfg, f.Sig)
		}
		grouped = append(grouped, GroupedFinding{Sig: f.Sig, GroupKey: f.Group.Key, Status: f.Meta.Status, ReopenedCount: f.Meta.ReopenedCount, Reproduces: reproduces})
	}
	newlyFrozen := ApplyFlapDetector(groups, grouped)
	if err := SaveGroups(cfg, groups); err != nil {
		return err
	}
	for _, groupKey := range newlyFrozen {
		detail := fmt.Sprintf("group %s frozen after a reproducing open/reopened finding in a group with at least two fixes", groupKey)
		if err := writeRunEscalation(cfg, runFlapEscalationName(groupKey), detail); err != nil {
			return err
		}
	}
	return nil
}

func ParkSignature(cfg Config, finding Finding, flap bool, reason string) error {
	if err := os.MkdirAll(filepath.Join(cfg.StateDir, "escalations"), 0o755); err != nil {
		return err
	}
	if err := writeEscalation(cfg, finding, flap, reason); err != nil {
		return err
	}
	if err := appendParked(cfg, finding.Sig); err != nil {
		return err
	}
	finding.Meta.Status = "parked"
	if err := writeFindingMetaFields(finding.MetaPath, map[string]any{"status": "parked"}); err != nil {
		return err
	}
	groups, _ := LoadGroups(cfg)
	g := groups[finding.Group.Key]
	if g == nil {
		g = &GroupRecord{}
		groups[finding.Group.Key] = g
	}
	if !containsString(g.Sigs, finding.Sig) {
		g.Sigs = append(g.Sigs, finding.Sig)
		sort.Strings(g.Sigs)
	}
	if flap {
		g.Parked = true
	}
	return SaveGroups(cfg, groups)
}

func writeEscalation(cfg Config, finding Finding, flap bool, reason string) error {
	attemptPath := filepath.Join(cfg.StateDir, "attempts", finding.Sig+".jsonl")
	records, _ := LoadAttemptRecords(attemptPath)
	siblings := siblingSigs(cfg, finding)
	var b strings.Builder
	fmt.Fprintf(&b, "# Escalation %s            status: parked   parked_at: %s\n", finding.Sig, time.Now().UTC().Format(time.RFC3339))
	flapWord := "no"
	if flap {
		flapWord = "yes"
	}
	fmt.Fprintf(&b, "group: %s (class=%s, shape=%s)   flap: %s\n", finding.Group.Key, finding.Group.Class, finding.Group.Shape, flapWord)
	if reason != "" {
		fmt.Fprintf(&b, "reason: %s\n", reason)
	}
	b.WriteString("## Finding\n")
	fmt.Fprintf(&b, "- repro: findings/%s/repro.sql  (engine=%s, sql_mode=%d, lane=%s)\n", finding.Sig, finding.Meta.Engine, finding.Meta.SQLMode, finding.Meta.Lane)
	fmt.Fprintf(&b, "- our signature: %s   our error: %s\n", oneLine(finding.Meta.OurSig), oneLine(finding.Meta.OurError))
	fmt.Fprintf(&b, "- oracle digest: %s\n", oneLine(string(finding.Meta.OracleDigest)))
	b.WriteString("## Attempts\n")
	if len(records) == 0 {
		b.WriteString("none\n")
	}
	for _, rec := range records {
		fmt.Fprintf(&b, "### Attempt %d  (%s-%s, %s, %d/%d tokens)\n", rec.Attempt, rec.StartedAt.Format(time.RFC3339), rec.EndedAt.Format(time.RFC3339), rec.Outcome, rec.Tokens.Input, rec.Tokens.Output)
		diagnosis := extractDiagnosis(filepath.Join(cfg.StateDir, rec.Transcript))
		fmt.Fprintf(&b, "- diagnosis (last assistant messages, extracted from attempt transcript): %s\n", diagnosis)
		diff := rec.Diff
		if diff == "" {
			diff = fmt.Sprintf("attempts/%s.attempt%d.diff", finding.Sig, rec.Attempt)
		}
		fmt.Fprintf(&b, "- diff tried: %s (%s)\n", diff, diffSummary(filepath.Join(cfg.StateDir, diff)))
		fmt.Fprintf(&b, "- gate/validation failure: %s\n", rec.Detail)
	}
	b.WriteString("## Siblings\n")
	if len(siblings) == 0 {
		b.WriteString("none\n")
	} else {
		for _, sig := range siblings {
			fmt.Fprintf(&b, "- %s\n", sig)
		}
	}
	return atomicWriteFile(filepath.Join(cfg.StateDir, "escalations", finding.Sig+".md"), []byte(b.String()), 0o644)
}

func siblingSigs(cfg Config, finding Finding) []string {
	findings, err := ScanFindings(cfg)
	if err != nil {
		return nil
	}
	var sigs []string
	for _, f := range findings {
		if f.Sig != finding.Sig && f.Group.Key == finding.Group.Key {
			sigs = append(sigs, f.Sig)
		}
	}
	sort.Strings(sigs)
	return sigs
}

func extractDiagnosis(streamPath string) string {
	parsed, err := ParseCodexJSONL(streamPath, "")
	if err != nil {
		return "unavailable"
	}
	if len(parsed.AgentMessages) == 0 {
		return "none"
	}
	text := strings.Join(parsed.AgentMessages, "\n")
	lines := strings.Split(text, "\n")
	if len(lines) > 40 {
		lines = lines[len(lines)-40:]
	}
	return oneLine(strings.Join(lines, " "))
}

func diffSummary(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "diff unavailable"
	}
	plus, minus := 0, 0
	files := make(map[string]bool)
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "+++ b/") {
			files[strings.TrimPrefix(line, "+++ b/")] = true
			continue
		}
		if strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++") {
			plus++
		}
		if strings.HasPrefix(line, "-") && !strings.HasPrefix(line, "---") {
			minus++
		}
	}
	var fileList []string
	for f := range files {
		fileList = append(fileList, f)
	}
	sort.Strings(fileList)
	return fmt.Sprintf("+%d/-%d lines over: %s", plus, minus, strings.Join(fileList, ", "))
}

func writeRunEscalation(cfg Config, name, detail string) error {
	path := filepath.Join(cfg.StateDir, "escalations", name)
	var b strings.Builder
	fmt.Fprintf(&b, "# Run escalation %s\n\n", name)
	fmt.Fprintf(&b, "time: %s\n\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "%s\n", detail)
	return atomicWriteFile(path, []byte(b.String()), 0o644)
}

func oneLine(s string) string {
	s = strings.TrimSpace(spaceRe.ReplaceAllString(s, " "))
	if len(s) > 1000 {
		return s[:1000] + "..."
	}
	if s == "" {
		return "<empty>"
	}
	return s
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
