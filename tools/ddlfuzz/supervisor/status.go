package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type StatusSnapshot struct {
	Now      time.Time       `json:"now"`
	Run      RunStatus       `json:"run"`
	Merge    MergeSlotStatus `json:"merge"`
	Fast     FastLaneStatus  `json:"fast"`
	E2E      E2ELaneStatus   `json:"e2e"`
	Findings FindingsStatus  `json:"findings"`
	FixAgent FixAgentStatus  `json:"fix_agent"`
	Events   []EventLine     `json:"events"`
}

type RunStatus struct {
	PID             int         `json:"pid"`
	Alive           bool        `json:"alive"`
	StartedAt       time.Time   `json:"started_at"`
	Deadline        time.Time   `json:"deadline"`
	RunHours        float64     `json:"run_hours"`
	FixModel        string      `json:"fix_model"`
	Blocked         bool        `json:"blocked"`
	BlockedReason   string      `json:"blocked_reason,omitempty"`
	LastSeenAge     string      `json:"last_seen_age,omitempty"`
	Spend           SpendRecord `json:"spend"`
	DiskFreeBytes   int64       `json:"disk_free_bytes"`
	RestartRequired bool        `json:"restart_required"`
	RestartHead     string      `json:"restart_head,omitempty"`
}

type MergeSlotStatus struct {
	State string `json:"state"`
	Line  string `json:"line"`
}

type FastLaneStatus struct {
	Stats         FastStats        `json:"stats"`
	StatsAge      string           `json:"stats_age"`
	StatsStale    bool             `json:"stats_stale"`
	ExecsRate1m   RateValue        `json:"execs_rate_1m"`
	ExecsRate15m  RateValue        `json:"execs_rate_15m"`
	SuppRateNow   RateValue        `json:"suppressed_rate_now"`
	SuppRate1m    RateValue        `json:"suppressed_rate_1m"`
	SuppRate15m   RateValue        `json:"suppressed_rate_15m"`
	EdgesDelta1m  map[string]int64 `json:"edges_delta_1m"`
	EdgesDelta15m map[string]int64 `json:"edges_delta_15m"`
	ExecSparkline string           `json:"exec_sparkline"`
}

type RateValue struct {
	Value      float64 `json:"value"`
	CoveredSec float64 `json:"covered_sec"`
	OK         bool    `json:"ok"`
}

type E2ELaneStatus struct {
	UpdatedAt      time.Time        `json:"updated_at"`
	Age            string           `json:"age"`
	Stale          bool             `json:"stale"`
	Cases          map[string]int64 `json:"cases"`
	ExecRejects    map[string]int64 `json:"exec_rejects"`
	CasesRateNow   RateValue        `json:"cases_rate_now"`
	CasesRate1m    RateValue        `json:"cases_rate_1m"`
	CasesRate15m   RateValue        `json:"cases_rate_15m"`
	RejectsRateNow RateValue        `json:"rejects_rate_now"`
	RejectsRate1m  RateValue        `json:"rejects_rate_1m"`
	RejectsRate15m RateValue        `json:"rejects_rate_15m"`
	Queue          SampleQueue      `json:"queue"`
	CasesSparkline string           `json:"cases_sparkline"`
}

type FindingsStatus struct {
	Counts     FindingCounts `json:"counts"`
	New15m     int           `json:"new_15m"`
	Groups     []GroupRow    `json:"groups"`
	MoreGroups int           `json:"more_groups"`
}

type GroupRow struct {
	ClassShape string `json:"class_shape"`
	GroupKey   string `json:"group_key"`
	Sigs       int    `json:"sigs"`
	Attempts   int    `json:"attempts"`
	OldestAge  string `json:"oldest_age"`
	MySQL      int    `json:"mysql"`
	MariaDB    int    `json:"mariadb"`
	Flags      string `json:"flags,omitempty"`
	Parked     bool   `json:"parked,omitempty"`
}

type FixAgentStatus struct {
	State       string          `json:"state"`
	Reason      string          `json:"reason,omitempty"`
	Attempt     *CurrentAttempt `json:"attempt,omitempty"`
	LastMessage string          `json:"last_message,omitempty"`
	Totals      map[string]int  `json:"totals"`
	Spend       SpendRecord     `json:"spend"`
	Elapsed     string          `json:"elapsed,omitempty"`
	Budget      string          `json:"budget,omitempty"`
	Stale       bool            `json:"stale"`
}

type CurrentAttempt struct {
	Sig             string    `json:"sig"`
	Attempt         int       `json:"attempt"`
	MaxAttempts     int       `json:"max_attempts"`
	GroupKey        string    `json:"group_key"`
	Class           string    `json:"class"`
	Shape           string    `json:"shape"`
	Engine          string    `json:"engine"`
	Phase           string    `json:"phase"`
	StartedAt       time.Time `json:"started_at"`
	AttemptDeadline time.Time `json:"attempt_deadline"`
	Transcript      string    `json:"transcript"`
}

type EventLine struct {
	Time    string `json:"time"`
	Message string `json:"message"`
}

func CollectStatus(cfg Config) StatusSnapshot {
	now := time.Now()
	samples := ReadSamplesTail(cfg, 1<<20)
	fast := readFastStats(cfg)
	e2eCounters := collectE2ECounters(readGenericJSON(filepath.Join(cfg.StateDir, "e2e-stats.json")))
	counts, _ := findingsSummary(cfg)
	spend, _ := LoadSpend(cfg)
	free, _ := FreeBytes(cfg.StateDir)
	run := collectRunStatus(cfg, now, spend, free)
	fastAge, fastStale := ageFromString(fast.TS, now, 5*time.Minute)
	e2eAge, e2eStale := ageFromTime(e2eCounters.UpdatedAt, now, 60*time.Second)
	groups, more := BuildGroupRows(cfg, now, 10)
	attempt := collectFixAgent(cfg, now, run, spend)
	return StatusSnapshot{
		Now: now,
		Run: run,
		Merge: MergeSlotStatus{
			State: mergeSlotState(cfg),
			Line:  NewMergeSlot(cfg).SnapshotLine(now),
		},
		Fast: FastLaneStatus{
			Stats: fast, StatsAge: fastAge, StatsStale: fastStale,
			ExecsRate1m:   rateValue(samples, func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, time.Minute, now),
			ExecsRate15m:  rateValue(samples, func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, 15*time.Minute, now),
			SuppRateNow:   rateValue(samples, func(s SampleRecord) int64 { return s.Fuzz.Suppressed }, 45*time.Second, now),
			SuppRate1m:    rateValue(samples, func(s SampleRecord) int64 { return s.Fuzz.Suppressed }, time.Minute, now),
			SuppRate15m:   rateValue(samples, func(s SampleRecord) int64 { return s.Fuzz.Suppressed }, 15*time.Minute, now),
			EdgesDelta1m:  sampleDeltas(samples, func(s SampleRecord) map[string]int64 { return s.Fuzz.Edges }, time.Minute, now),
			EdgesDelta15m: sampleDeltas(samples, func(s SampleRecord) map[string]int64 { return s.Fuzz.Edges }, 15*time.Minute, now),
			ExecSparkline: Sparkline(samples, func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, 30*time.Minute, 20),
		},
		E2E: E2ELaneStatus{
			UpdatedAt: e2eCounters.UpdatedAt, Age: e2eAge, Stale: e2eStale, Cases: e2eCounters.Cases, ExecRejects: e2eCounters.ExecRejects, Queue: queueDepths(cfg),
			CasesRateNow:   rateValue(samples, sampleE2ECasesTotal, 45*time.Second, now),
			CasesRate1m:    rateValue(samples, sampleE2ECasesTotal, time.Minute, now),
			CasesRate15m:   rateValue(samples, sampleE2ECasesTotal, 15*time.Minute, now),
			RejectsRateNow: rateValue(samples, sampleE2ERejectsTotal, 45*time.Second, now),
			RejectsRate1m:  rateValue(samples, sampleE2ERejectsTotal, time.Minute, now),
			RejectsRate15m: rateValue(samples, sampleE2ERejectsTotal, 15*time.Minute, now),
			CasesSparkline: Sparkline(samples, sampleE2ECasesTotal, 30*time.Minute, 20),
		},
		Findings: FindingsStatus{Counts: counts, New15m: findingsInWindow(cfg, now.Add(-15*time.Minute), now), Groups: groups, MoreGroups: more},
		FixAgent: attempt,
		Events:   readEvents(cfg),
	}
}

func collectRunStatus(cfg Config, now time.Time, spend SpendRecord, free int64) RunStatus {
	// StartedAt/Deadline deliberately stay zero when run.json is absent: cfg's
	// values are computed at *this* process's start, not the supervised run's.
	run := RunStatus{Spend: spend, DiskFreeBytes: free}
	var disk struct {
		PID       int       `json:"pid"`
		StartedAt time.Time `json:"started_at"`
		Deadline  time.Time `json:"deadline"`
		RunHours  float64   `json:"run_hours"`
		FixModel  string    `json:"fix_model"`
	}
	if data, err := os.ReadFile(filepath.Join(cfg.StateDir, "run.json")); err == nil && json.Unmarshal(data, &disk) == nil {
		run.PID = disk.PID
		run.StartedAt = disk.StartedAt
		run.Deadline = disk.Deadline
		run.RunHours = disk.RunHours
		run.FixModel = disk.FixModel
	}
	if run.PID == 0 {
		if data, err := os.ReadFile(filepath.Join(cfg.StateDir, "supervisor.pid")); err == nil {
			run.PID, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
	}
	run.Alive = pidAlive(run.PID)
	if data, err := os.ReadFile(filepath.Join(cfg.StateDir, "BLOCKED")); err == nil {
		run.Blocked = true
		run.BlockedReason = firstLine(string(data))
	}
	if marker, _, ok := resumeMarker(cfg); ok {
		run.RestartRequired = true
		run.RestartHead = marker.ValidatedHead
	}
	if !run.Alive {
		if info, err := os.Stat(filepath.Join(cfg.StateDir, "supervisor.log")); err == nil {
			run.LastSeenAge = fmtAgo(now.Sub(info.ModTime()))
		}
	}
	return run
}

func mergeSlotState(cfg Config) string {
	slot := NewMergeSlot(cfg)
	if req, err := slot.ReadRequest(); err == nil {
		return req.Phase
	}
	if _, err := slot.ReadClaimed(); err == nil {
		return "claimed"
	}
	if _, err := slot.ReadResult(); err == nil {
		return "result"
	}
	return "idle"
}

func WriteRunState(cfg Config) error {
	return atomicWriteJSON(filepath.Join(cfg.StateDir, "run.json"), map[string]any{
		"pid":        os.Getpid(),
		"started_at": cfg.StartedAt.UTC().Format(time.RFC3339),
		"deadline":   cfg.Deadline.UTC().Format(time.RFC3339),
		"run_hours":  cfg.RunHours,
		"fix_model":  cfg.FixModel,
	}, 0o644)
}

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

type e2eCounters struct {
	UpdatedAt   time.Time
	Cases       map[string]int64
	ExecRejects map[string]int64
}

func collectE2ECounters(raw map[string]any) e2eCounters {
	out := e2eCounters{Cases: map[string]int64{}, ExecRejects: map[string]int64{}}
	if len(raw) == 0 {
		return out
	}
	if n := int64FromAny(raw["updated_at"]); n > 0 {
		out.UpdatedAt = time.Unix(n, 0)
	}
	all := map[string]int64{}
	collectCounters("", raw, all)
	for k, v := range all {
		lk := strings.ToLower(k)
		switch {
		case strings.Contains(lk, "exec_reject"):
			out.ExecRejects[engineFromKey(lk)] += v
		case strings.Contains(lk, "case"):
			out.Cases[engineFromKey(lk)] += v
		}
	}
	return out
}

func engineFromKey(k string) string {
	if strings.Contains(k, "mariadb") || strings.Contains(k, "maria") {
		return "mariadb"
	}
	return "mysql"
}

func queueDepths(cfg Config) SampleQueue {
	return SampleQueue{Pending: int64(dirCount(filepath.Join(cfg.StateDir, "e2e-queue", "pending"))), Processing: int64(dirCount(filepath.Join(cfg.StateDir, "e2e-queue", "processing"))), Done: int64(dirCount(filepath.Join(cfg.StateDir, "e2e-queue", "done")))}
}

func dirCount(path string) int {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0
	}
	return len(entries)
}

func BuildGroupRows(cfg Config, now time.Time, capRows int) ([]GroupRow, int) {
	findings, _ := ScanFindings(cfg)
	groups, _ := LoadGroups(cfg)
	rowsByKey := map[string]*GroupRow{}
	oldest := map[string]time.Duration{}
	for _, f := range findings {
		if f.Meta.Status != "" && f.Meta.Status != "open" {
			continue
		}
		row := rowsByKey[f.Group.Key]
		if row == nil {
			row = &GroupRow{ClassShape: f.Group.Class + "|" + f.Group.Shape, GroupKey: f.Group.Key}
			rowsByKey[f.Group.Key] = row
		}
		row.Sigs++
		switch strings.ToLower(f.Meta.Engine) {
		case "mariadb", "maria":
			row.MariaDB++
		default:
			row.MySQL++
		}
		discovered := parseLooseTime(f.Meta.DiscoveredAt)
		if discovered.IsZero() {
			continue
		}
		if age := now.Sub(discovered); age > oldest[f.Group.Key] {
			oldest[f.Group.Key] = age
			row.OldestAge = fmtDur(age)
		}
	}
	for key, row := range rowsByKey {
		attemptSigs := map[string]bool{}
		for _, f := range findings {
			if f.Group.Key == key {
				attemptSigs[f.Sig] = true
			}
		}
		for sig := range attemptSigs {
			records, _ := LoadAttemptRecords(filepath.Join(cfg.StateDir, "attempts", sig+".jsonl"))
			row.Attempts += len(records)
		}
		if g := groups[key]; g != nil {
			var flags []string
			if g.Parked {
				row.Parked = true
				flags = append(flags, "flap-parked")
			}
			if g.FixCount > 0 {
				flags = append(flags, "fixes:"+strconv.Itoa(g.FixCount))
			}
			row.Flags = strings.Join(flags, ",")
		}
	}
	var rows []GroupRow
	for _, row := range rowsByKey {
		if row.OldestAge == "" {
			row.OldestAge = "n/a"
		}
		rows = append(rows, *row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Sigs == rows[j].Sigs {
			return rows[i].ClassShape < rows[j].ClassShape
		}
		return rows[i].Sigs > rows[j].Sigs
	})
	more := 0
	if capRows > 0 && len(rows) > capRows {
		more = len(rows) - capRows
		rows = rows[:capRows]
	}
	return rows, more
}

func collectFixAgent(cfg Config, now time.Time, run RunStatus, spend SpendRecord) FixAgentStatus {
	out := FixAgentStatus{State: "idle", Reason: idleReason(cfg, run.Deadline, now), Totals: attemptSummary(cfg), Spend: spend}
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "current-attempt.json"))
	if err != nil {
		return out
	}
	var cur CurrentAttempt
	if json.Unmarshal(data, &cur) != nil {
		return out
	}
	out.Attempt = &cur
	out.State = "running"
	out.Reason = ""
	out.Elapsed = fmtDur(now.Sub(cur.StartedAt))
	out.Budget = fmtDur(cur.AttemptDeadline.Sub(cur.StartedAt))
	if !run.Alive || now.After(cur.AttemptDeadline.Add(5*time.Minute)) {
		out.State = "stale"
		out.Stale = true
		out.Reason = "killed mid-attempt?"
	}
	if cur.Transcript != "" {
		out.LastMessage = lastAgentMessage(filepath.Join(cfg.StateDir, cur.Transcript))
	}
	return out
}

func writeCurrentAttempt(cfg Config, finding Finding, attemptN int, phase string, started time.Time) error {
	cur := CurrentAttempt{
		Sig: finding.Sig, Attempt: attemptN, MaxAttempts: 3, GroupKey: finding.Group.Key, Class: finding.Group.Class, Shape: finding.Group.Shape, Engine: finding.Meta.Engine,
		Phase: phase, StartedAt: started.UTC(), AttemptDeadline: started.Add(cfg.AttemptTO).UTC(),
		Transcript: filepath.ToSlash(filepath.Join("attempts", finding.Sig+".attempt"+strconv.Itoa(attemptN)+".stream.jsonl")),
	}
	return atomicWriteJSON(filepath.Join(cfg.StateDir, "current-attempt.json"), cur, 0o644)
}

func lastAgentMessage(path string) string {
	data := tailFile(path, 256<<10)
	if len(data) == 0 {
		return ""
	}
	tmp, err := os.CreateTemp("", "ddlfuzz-stream-*.jsonl")
	if err != nil {
		return ""
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return ""
	}
	_ = tmp.Close()
	parsed, err := ParseCodexJSONL(tmp.Name(), "")
	if err != nil {
		return ""
	}
	return oneLine(parsed.LastAgentText)
}

func readEvents(cfg Config) []EventLine {
	data := tailFile(filepath.Join(cfg.StateDir, "supervisor.log"), 64<<10)
	if len(data) == 0 {
		return nil
	}
	var events []EventLine
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := sc.Text()
		ts, msg, ok := splitLogLine(line)
		if !ok {
			continue
		}
		// Relayed child stdout; the supervisor logs lifecycle events (exits,
		// restarts, wedges) as its own unprefixed lines, so nothing is lost.
		if strings.HasPrefix(msg, "fuzzer: ") || strings.HasPrefix(msg, "e2e: ") {
			continue
		}
		events = append(events, EventLine{Time: ts.Local().Format("15:04:05"), Message: msg})
	}
	if len(events) > 8 {
		events = events[len(events)-8:]
	}
	return events
}

func splitLogLine(line string) (time.Time, string, bool) {
	fields := strings.SplitN(line, " ", 2)
	if len(fields) != 2 {
		return time.Time{}, "", false
	}
	ts := parseLooseTime(fields[0])
	if ts.IsZero() {
		return time.Time{}, "", false
	}
	return ts, fields[1], true
}

func tailFile(path string, max int64) []byte {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return nil
	}
	if max <= 0 || max > info.Size() {
		max = info.Size()
	}
	start := info.Size() - max
	dropLeadingPartial := false
	if start > 0 {
		var prev [1]byte
		if _, err := f.ReadAt(prev[:], start-1); err == nil && prev[0] != '\n' {
			dropLeadingPartial = true
		}
	}
	if _, err := f.Seek(start, 0); err != nil {
		return nil
	}
	data, _ := io.ReadAll(f)
	if dropLeadingPartial {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			return nil
		}
		data = data[i+1:]
	}
	return data
}

func rateValue(samples []SampleRecord, extract func(SampleRecord) int64, window time.Duration, now time.Time) RateValue {
	v, covered, ok := Rate(samples, extract, window, now)
	return RateValue{Value: v, CoveredSec: covered.Seconds(), OK: ok}
}

func sampleDeltas(samples []SampleRecord, extract func(SampleRecord) map[string]int64, window time.Duration, now time.Time) map[string]int64 {
	series := windowSamples(samples, window, now)
	if len(series) < 2 {
		return nil
	}
	out := map[string]int64{"go": 0, "mysql": 0, "mariadb": 0}
	prev := extract(series[0])
	for _, rec := range series[1:] {
		cur := extract(rec)
		for _, key := range []string{"go", "mysql", "mariadb"} {
			if cur[key] > prev[key] {
				out[key] += cur[key] - prev[key]
			}
		}
		prev = cur
	}
	return out
}

func sampleE2ECasesTotal(s SampleRecord) int64 {
	return s.E2E.Cases["mysql"] + s.E2E.Cases["mariadb"]
}

func sampleE2ERejectsTotal(s SampleRecord) int64 {
	return s.E2E.ExecRejects["mysql"] + s.E2E.ExecRejects["mariadb"]
}

func findingsInWindow(cfg Config, start, end time.Time) int {
	findings, _ := ScanFindings(cfg)
	n := 0
	for _, f := range findings {
		ts := parseLooseTime(f.Meta.DiscoveredAt)
		if !ts.IsZero() && !ts.Before(start) && !ts.After(end) {
			n++
		}
	}
	return n
}

func ageFromString(s string, now time.Time, stale time.Duration) (string, bool) {
	return ageFromTime(parseLooseTime(s), now, stale)
}

func ageFromTime(ts time.Time, now time.Time, stale time.Duration) (string, bool) {
	if ts.IsZero() {
		return "n/a", false
	}
	age := now.Sub(ts)
	return fmtAgo(age), age > stale
}

func idleReason(cfg Config, deadline, now time.Time) string {
	if !deadline.IsZero() && now.After(deadline.Add(-3*time.Hour)) {
		return "deadline window"
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, "escalations", "run-spend-cap.md")); err == nil {
		return "spend cap"
	}
	return "waiting for selection"
}

func firstLine(s string) string {
	sc := bufio.NewScanner(strings.NewReader(s))
	if sc.Scan() {
		return strings.TrimSpace(sc.Text())
	}
	return ""
}
