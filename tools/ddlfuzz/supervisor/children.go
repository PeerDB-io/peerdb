package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	childRegistryVersion = 1
	childStartTolerance  = 2 * time.Second
)

type ChildRecord struct {
	PID          int       `json:"pid"`
	PGID         int       `json:"pgid"`
	StartUnixUS  int64     `json:"start_unix_us"`
	Cmd          string    `json:"cmd"`
	RegisteredAt time.Time `json:"registered_at"`
}

type childRegistryFile struct {
	V             int           `json:"v"`
	SupervisorPID int           `json:"supervisor_pid"`
	Children      []ChildRecord `json:"children"`
}

type childTracker struct {
	mu       sync.Mutex
	path     string
	children map[int]ChildRecord
}

var procTracker *childTracker

func initChildTracking(cfg Config) {
	procTracker = &childTracker{
		path:     childRegistryPath(cfg),
		children: map[int]ChildRecord{},
	}
}

func childRegistryPath(cfg Config) string {
	return filepath.Join(cfg.StateDir, "children.json")
}

func (t *childTracker) register(cmd *exec.Cmd, pgid int) {
	if t == nil || cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	start, err := processStartTime(pid)
	if err != nil {
		start = 0
	}
	rec := ChildRecord{
		PID:          pid,
		PGID:         pgid,
		StartUnixUS:  start,
		Cmd:          childCmd(cmd),
		RegisteredAt: time.Now().UTC(),
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.children[pid] = rec
	if err := t.persistLocked(); err != nil {
		fmt.Fprintf(os.Stderr, "children registry write failed: %v\n", err)
	}
}

func (t *childTracker) deregister(pid int) {
	if t == nil || pid <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.children, pid)
	if err := t.persistLocked(); err != nil {
		fmt.Fprintf(os.Stderr, "children registry write failed: %v\n", err)
	}
}

func (t *childTracker) persistLocked() error {
	children := make([]ChildRecord, 0, len(t.children))
	for _, rec := range t.children {
		children = append(children, rec)
	}
	sort.Slice(children, func(i, j int) bool { return children[i].PID < children[j].PID })
	return writeChildRegistry(t.path, children)
}

func writeChildRegistry(path string, children []ChildRecord) error {
	return atomicWriteJSON(path, childRegistryFile{V: childRegistryVersion, SupervisorPID: os.Getpid(), Children: children}, 0o644)
}

func loadChildRegistry(path string) ([]ChildRecord, error) {
	var reg childRegistryFile
	if err := readJSONFile(path, &reg); err != nil {
		return nil, err
	}
	if reg.V != childRegistryVersion {
		return nil, fmt.Errorf("unsupported children registry version %d", reg.V)
	}
	return reg.Children, nil
}

func childCmd(cmd *exec.Cmd) string {
	text := strings.Join(cmd.Args, " ")
	if len(text) > 200 {
		return text[:200]
	}
	return text
}

type procProbe struct {
	groupAlive func(pgid int) (bool, error)
	startTime  func(pid int) (int64, error)
}

type childScanPlan struct {
	kill []ChildRecord
	keep []ChildRecord
	drop []ChildRecord
}

func defaultProcProbe() procProbe {
	return procProbe{
		groupAlive: func(pgid int) (bool, error) {
			err := syscall.Kill(-pgid, 0)
			if err == nil {
				return true, nil
			}
			if errors.Is(err, syscall.ESRCH) {
				return false, nil
			}
			return false, err
		},
		startTime: processStartTime,
	}
}

func planOrphanedChildren(records []ChildRecord, probe procProbe) (childScanPlan, error) {
	var plan childScanPlan
	for _, rec := range records {
		if rec.PID <= 0 || rec.PGID <= 0 {
			plan.keep = append(plan.keep, rec)
			return plan, fmt.Errorf("invalid child registry entry pid=%d pgid=%d", rec.PID, rec.PGID)
		}
		alive, err := probe.groupAlive(rec.PGID)
		if err != nil {
			plan.keep = append(plan.keep, rec)
			return plan, fmt.Errorf("probe child pgid %d: %w", rec.PGID, err)
		}
		if !alive {
			plan.drop = append(plan.drop, rec)
			continue
		}
		if rec.StartUnixUS != 0 {
			if start, err := probe.startTime(rec.PID); err == nil && start != 0 && absInt64(start-rec.StartUnixUS) > int64(childStartTolerance/time.Microsecond) {
				plan.keep = append(plan.keep, rec)
				return plan, fmt.Errorf("child identity anomaly pid=%d pgid=%d recorded_start_us=%d live_start_us=%d", rec.PID, rec.PGID, rec.StartUnixUS, start)
			}
		}
		plan.kill = append(plan.kill, rec)
	}
	return plan, nil
}

func killOrphanedChildren(cfg Config, logf func(string, ...any)) error {
	path := childRegistryPath(cfg)
	records, err := loadChildRegistry(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		_ = writeOrphanEscalation(cfg, fmt.Sprintf("children registry unreadable: %v", err), nil)
		return err
	}
	plan, err := planOrphanedChildren(records, defaultProcProbe())
	if err != nil {
		_ = writeOrphanEscalation(cfg, err.Error(), plan.keep)
		return err
	}
	// TERM every group before draining any, so no orphan keeps working
	// through an earlier group's grace window.
	for _, rec := range plan.kill {
		_ = syscall.Kill(-rec.PGID, syscall.SIGTERM)
	}
	for _, rec := range plan.kill {
		if err := killChildGroup(rec); err != nil {
			_ = writeOrphanEscalation(cfg, err.Error(), []ChildRecord{rec})
			return err
		}
		if logf != nil {
			logf("killed orphaned %s child pid=%d pgid=%d cmd=%q", childKind(rec.Cmd), rec.PID, rec.PGID, rec.Cmd)
		}
	}
	return writeChildRegistry(path, nil)
}

func killChildGroup(rec ChildRecord) error {
	_ = syscall.Kill(-rec.PGID, syscall.SIGTERM)
	if waitGroupGoneOrZombie(rec.PGID, 60*time.Second) {
		return nil
	}
	_ = syscall.Kill(-rec.PGID, syscall.SIGKILL)
	if waitGroupGoneOrZombie(rec.PGID, 30*time.Second) {
		return nil
	}
	return fmt.Errorf("orphan child group still alive pid=%d pgid=%d cmd=%q", rec.PID, rec.PGID, rec.Cmd)
}

func waitGroupGoneOrZombie(pgid int, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for {
		alive, err := defaultProcProbe().groupAlive(pgid)
		if err == nil && (!alive || groupAllZombies(pgid)) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Second)
	}
}

func groupAllZombies(pgid int) bool {
	out, err := exec.Command("ps", "-axo", "pid=,pgid=,stat=").Output()
	if err != nil {
		return false
	}
	lines := strings.Split(string(out), "\n")
	found := false
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[1] != fmt.Sprint(pgid) {
			continue
		}
		found = true
		if !strings.HasPrefix(fields[2], "Z") {
			return false
		}
	}
	return found
}

func writeOrphanEscalation(cfg Config, reason string, records []ChildRecord) error {
	var b strings.Builder
	fmt.Fprintf(&b, "reason: %s\n\n", reason)
	for _, rec := range records {
		fmt.Fprintf(&b, "- pid=%d pgid=%d cmd=%q registered_at=%s age=%s\n", rec.PID, rec.PGID, rec.Cmd, rec.RegisteredAt.Format(time.RFC3339), fmtAgo(time.Since(rec.RegisteredAt)))
		survivors := groupSurvivors(rec.PGID)
		for _, survivor := range survivors {
			fmt.Fprintf(&b, "  - survivor pid=%d stat=%s age=%s\n", survivor.PID, survivor.Stat, survivor.Age)
		}
	}
	return writeRunEscalation(cfg, "run-orphans.md", b.String())
}

type groupSurvivor struct {
	PID  int
	Stat string
	Age  string
}

func groupSurvivors(pgid int) []groupSurvivor {
	out, err := exec.Command("ps", "-axo", "pid=,pgid=,stat=,etime=").Output()
	if err != nil {
		return nil
	}
	var survivors []groupSurvivor
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[1] != fmt.Sprint(pgid) {
			continue
		}
		var pid int
		if _, err := fmt.Sscanf(fields[0], "%d", &pid); err != nil {
			continue
		}
		survivors = append(survivors, groupSurvivor{PID: pid, Stat: fields[2], Age: fields[3]})
	}
	return survivors
}

func childKind(cmd string) string {
	lower := strings.ToLower(cmd)
	switch {
	case strings.Contains(lower, "e2e"):
		return "e2e"
	case strings.Contains(lower, "fuzz"):
		return "fuzzer"
	case strings.Contains(lower, "codex"):
		return "codex"
	default:
		return "process"
	}
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
