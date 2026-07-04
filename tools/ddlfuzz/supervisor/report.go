package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type FastStats struct {
	TS                   string           `json:"ts"`
	ExecsTotal           int64            `json:"execs_total"`
	ExecsPerSec          float64          `json:"execs_per_sec"`
	CorpusCount          map[string]int64 `json:"corpus_count"`
	Edges                map[string]int64 `json:"edges"`
	OracleRestarts       map[string]int64 `json:"oracle_restarts"`
	Suppressed           int64            `json:"suppressed"`
	FindingsEmittedTotal int64            `json:"findings_emitted_total"`
	Extra                map[string]any   `json:"-"`
	Raw                  map[string]any   `json:"-"`
}

type FindingCounts struct {
	Open     int
	Fixed    int
	Ledgered int
	Parked   int
}

type ComponentSnapshot struct {
	FuzzerUp       bool
	FuzzerRestarts int
	E2EUp          bool
	E2ERestarts    int
	DiskFreeBytes  int64
	Degraded       bool
}

func RenderReport(cfg Config, status string, comps ComponentSnapshot) (string, error) {
	now := time.Now()
	fast := readFastStats(cfg)
	e2e := readGenericJSON(filepath.Join(cfg.StateDir, "e2e-stats.json"))
	counts, openGroups := findingsSummary(cfg)
	spend, _ := LoadSpend(cfg)
	attemptCounts := attemptSummary(cfg)
	edgesDelta := lastEdgeDelta(cfg, fast.Edges)

	var b strings.Builder
	fmt.Fprintf(&b, "# ddlfuzz run report        started: %s   deadline: %s   now: %s   status: %s\n", cfg.StartedAt.UTC().Format(time.RFC3339), cfg.Deadline.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339), status)
	b.WriteString("## Fast lane   (from state/stats.json; stale>5min flagged)\n")
	if fast.TS == "" {
		b.WriteString("stats: unavailable\n")
	} else {
		stale := ""
		if ts := parseLooseTime(fast.TS); !ts.IsZero() && time.Since(ts) > 5*time.Minute {
			stale = " stale"
		}
		fmt.Fprintf(&b, "execs/s %.0f, execs_total %d%s, corpus counts %s, edges %s, edges delta last hour %s, oracle restarts %s\n",
			fast.ExecsPerSec, fast.ExecsTotal, stale, formatIntMap(fast.CorpusCount), formatIntMap(fast.Edges), formatIntMap(edgesDelta), formatIntMap(fast.OracleRestarts))
	}
	b.WriteString("## E2E lane    (from state/e2e-stats.json)\n")
	if len(e2e) == 0 {
		b.WriteString("stats: unavailable\n")
	} else {
		fmt.Fprintf(&b, "updated_at %s, counters %s, confirm queue depth %d\n", fmt.Sprint(e2e["updated_at"]), formatAnyCounters(e2e), confirmQueueDepth(cfg))
	}
	b.WriteString("## Findings\n")
	fmt.Fprintf(&b, "open %d / fixed %d / ledgered %d / parked %d\n", counts.Open, counts.Fixed, counts.Ledgered, counts.Parked)
	if len(openGroups) > 0 {
		b.WriteString("open groups:\n")
		for _, line := range openGroups {
			fmt.Fprintf(&b, "- %s\n", line)
		}
	}
	b.WriteString("## Fix agent\n")
	fmt.Fprintf(&b, "attempts total %d, fixed %d, ledgered %d, failed %d, timeouts %d; current attempt none; spend: input %d cached %d output %d, wall-hours %.2f, cap %d\n",
		attemptCounts["total"], attemptCounts["fixed"], attemptCounts["ledgered"], attemptCounts["failed"], attemptCounts["timeout"],
		spend.Tokens.Input, spend.Tokens.CachedInput, spend.Tokens.Output, float64(spend.AttemptSeconds)/3600.0, cfg.MaxTokens)
	b.WriteString("## Components\n")
	fmt.Fprintf(&b, "fuzzer up %t/restarts %d, e2e up %t/restarts %d, disk free %s, degraded %t\n", comps.FuzzerUp, comps.FuzzerRestarts, comps.E2EUp, comps.E2ERestarts, formatGiB(comps.DiskFreeBytes), comps.Degraded)
	b.WriteString("## Coverage history (state/coverage/history/edges.csv: ts,go,mysql,mariadb - appended hourly)\n")
	for _, line := range renderCoverageHistory(cfg) {
		fmt.Fprintf(&b, "%s\n", line)
	}
	if status == "final" {
		b.WriteString("## Final\n")
		plateau := plateauStatus(cfg)
		work := "clean"
		if counts.Open > 0 || counts.Parked > 0 {
			work = "work-remaining"
		}
		fmt.Fprintf(&b, "plateau %s; findings open %d fixed %d ledgered %d parked %d; parked escalations %s; status %s\n",
			plateau, counts.Open, counts.Fixed, counts.Ledgered, counts.Parked, parkedEscalationsStatus(cfg), work)
		escalations := escalationIndex(cfg)
		if len(escalations) > 0 {
			b.WriteString("escalations:\n")
			for _, e := range escalations {
				fmt.Fprintf(&b, "- %s\n", e)
			}
		}
	}
	return b.String(), nil
}

func WriteReport(cfg Config, status string, comps ComponentSnapshot) error {
	report, err := RenderReport(cfg, status, comps)
	if err != nil {
		return err
	}
	return atomicWriteFile(filepath.Join(cfg.StateDir, "report.md"), []byte(report), 0o644)
}

func AppendCoverageHistory(cfg Config) error {
	fast := readFastStats(cfg)
	if len(fast.Edges) == 0 {
		return nil
	}
	path := filepath.Join(cfg.StateDir, "coverage", "history", "edges.csv")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	newFile := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		newFile = true
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if newFile {
		_ = w.Write([]string{"ts", "go", "mysql", "mariadb"})
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	if fast.TS != "" {
		ts = fast.TS
	}
	if err := w.Write([]string{ts, strconv.FormatInt(fast.Edges["go"], 10), strconv.FormatInt(fast.Edges["mysql"], 10), strconv.FormatInt(fast.Edges["mariadb"], 10)}); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func readFastStats(cfg Config) FastStats {
	var fast FastStats
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "stats.json"))
	if err != nil {
		return fast
	}
	_ = json.Unmarshal(data, &fast)
	_ = json.Unmarshal(data, &fast.Raw)
	if fast.CorpusCount == nil {
		fast.CorpusCount = map[string]int64{}
	}
	if fast.Edges == nil {
		fast.Edges = map[string]int64{}
	}
	if fast.OracleRestarts == nil {
		fast.OracleRestarts = map[string]int64{}
	}
	return fast
}

func readGenericJSON(path string) map[string]any {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var obj map[string]any
	if json.Unmarshal(data, &obj) != nil {
		return nil
	}
	return obj
}

func findingsSummary(cfg Config) (FindingCounts, []string) {
	findings, _ := ScanFindings(cfg)
	var counts FindingCounts
	groupCounts := make(map[string]int)
	for _, f := range findings {
		switch f.Meta.Status {
		case "fixed":
			counts.Fixed++
		case "ledgered":
			counts.Ledgered++
		case "parked":
			counts.Parked++
		default:
			counts.Open++
			groupCounts[f.Group.Key]++
		}
	}
	var groups []string
	for k, n := range groupCounts {
		groups = append(groups, fmt.Sprintf("%s: %d open", k, n))
	}
	sort.Strings(groups)
	return counts, groups
}

func attemptSummary(cfg Config) map[string]int {
	out := map[string]int{"total": 0, "fixed": 0, "ledgered": 0, "failed": 0, "timeout": 0}
	files, _ := filepath.Glob(filepath.Join(cfg.StateDir, "attempts", "*.jsonl"))
	for _, path := range files {
		// attempts/ also holds <sig>.attempt<N>.stream.jsonl codex transcripts;
		// only <sig>.jsonl files are attempt records.
		if strings.Contains(filepath.Base(path), ".attempt") {
			continue
		}
		records, err := LoadAttemptRecords(path)
		if err != nil {
			continue
		}
		for _, rec := range records {
			out["total"]++
			switch rec.Outcome {
			case "fixed":
				out["fixed"]++
			case "ledgered":
				out["ledgered"]++
			case "timeout":
				out["timeout"]++
			default:
				out["failed"]++
			}
		}
	}
	return out
}

func confirmQueueDepth(cfg Config) int {
	total := 0
	for _, dir := range []string{"pending", "processing"} {
		entries, err := os.ReadDir(filepath.Join(cfg.StateDir, "e2e-queue", dir))
		if err == nil {
			total += len(entries)
		}
	}
	return total
}

func formatIntMap(m map[string]int64) string {
	if len(m) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func formatAnyCounters(m map[string]any) string {
	counters := make(map[string]int64)
	collectCounters("", m, counters)
	return formatIntMap(counters)
}

func collectCounters(prefix string, v any, out map[string]int64) {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if k == "updated_at" || k == "ts" {
				continue
			}
			name := k
			if prefix != "" {
				name = prefix + "." + k
			}
			collectCounters(name, child, out)
		}
	case float64:
		out[prefix] = int64(x)
	}
}

func lastEdgeDelta(cfg Config, current map[string]int64) map[string]int64 {
	out := map[string]int64{"go": 0, "mysql": 0, "mariadb": 0}
	rows := readEdgesCSV(cfg)
	if len(rows) == 0 {
		return out
	}
	prev := rows[len(rows)-1]
	for _, key := range []string{"go", "mysql", "mariadb"} {
		out[key] = current[key] - prev[key]
	}
	return out
}

func readEdgesCSV(cfg Config) []map[string]int64 {
	path := filepath.Join(cfg.StateDir, "coverage", "history", "edges.csv")
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil || len(records) <= 1 {
		return nil
	}
	var rows []map[string]int64
	for _, rec := range records[1:] {
		if len(rec) < 4 {
			continue
		}
		row := make(map[string]int64)
		row["go"], _ = strconv.ParseInt(rec[1], 10, 64)
		row["mysql"], _ = strconv.ParseInt(rec[2], 10, 64)
		row["mariadb"], _ = strconv.ParseInt(rec[3], 10, 64)
		rows = append(rows, row)
	}
	return rows
}

func renderCoverageHistory(cfg Config) []string {
	path := filepath.Join(cfg.StateDir, "coverage", "history", "edges.csv")
	f, err := os.Open(path)
	if err != nil {
		return []string{"history unavailable"}
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if len(lines) > 13 {
		lines = lines[len(lines)-13:]
	}
	if len(lines) == 0 {
		return []string{"history unavailable"}
	}
	return lines
}

func plateauStatus(cfg Config) string {
	rows := readEdgesCSV(cfg)
	if len(rows) < 2 {
		return "climbing"
	}
	first := rows[0]
	last := rows[len(rows)-1]
	if last["mysql"]-first["mysql"] == 0 && last["mariadb"]-first["mariadb"] == 0 {
		return "plateaued"
	}
	return "climbing"
}

func parkedEscalationsStatus(cfg Config) string {
	parked, _ := LoadParkedList(cfg)
	for sig := range parked {
		if _, err := os.Stat(filepath.Join(cfg.StateDir, "escalations", sig+".md")); err != nil {
			return "work-remaining"
		}
	}
	return "clean"
}

func escalationIndex(cfg Config) []string {
	entries, err := os.ReadDir(filepath.Join(cfg.StateDir, "escalations"))
	if err != nil {
		return nil
	}
	var out []string
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".md") {
			continue
		}
		data, _ := os.ReadFile(filepath.Join(cfg.StateDir, "escalations", ent.Name()))
		reason := ""
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "reason:") {
				reason = strings.TrimSpace(strings.TrimPrefix(line, "reason:"))
				break
			}
		}
		if reason == "" {
			reason = "see file"
		}
		out = append(out, fmt.Sprintf("%s -> %s", strings.TrimSuffix(ent.Name(), ".md"), reason))
	}
	sort.Strings(out)
	return out
}
