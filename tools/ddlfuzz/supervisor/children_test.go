package main

import (
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestChildRegistryRoundTrip(t *testing.T) {
	cfg := testConfig(t)
	path := childRegistryPath(cfg)
	if _, err := loadChildRegistry(path); !os.IsNotExist(err) {
		t.Fatalf("missing registry err = %v, want not exist", err)
	}
	tracker := &childTracker{
		path: path,
		children: map[int]ChildRecord{
			123: {PID: 123, PGID: 123, StartUnixUS: 456, Cmd: "sleep 30", RegisteredAt: time.Now().UTC()},
		},
	}
	if err := tracker.persistLocked(); err != nil {
		t.Fatal(err)
	}
	records, err := loadChildRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].PID != 123 || records[0].PGID != 123 {
		t.Fatalf("records = %#v", records)
	}
	tracker.deregister(123)
	records, err = loadChildRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("records after deregister = %#v", records)
	}
	mustWrite(t, path, "{")
	if _, err := loadChildRegistry(path); err == nil {
		t.Fatal("corrupt registry load succeeded")
	}
}

func TestPlanOrphanedChildren(t *testing.T) {
	errEPERM := syscall.EPERM
	tests := []struct {
		name       string
		record     ChildRecord
		alive      bool
		aliveErr   error
		liveStart  int64
		wantKill   bool
		wantDrop   bool
		wantErr    bool
		errContain string
	}{
		{
			name:     "dead group drops",
			record:   ChildRecord{PID: 1, PGID: 1, StartUnixUS: 100},
			wantDrop: true,
		},
		{
			name:       "invalid pgid errors",
			record:     ChildRecord{PID: 1},
			wantErr:    true,
			errContain: "invalid child registry entry",
		},
		{
			name:      "live matching start kills",
			record:    ChildRecord{PID: 2, PGID: 2, StartUnixUS: 100_000_000},
			alive:     true,
			liveStart: 100_000_001,
			wantKill:  true,
		},
		{
			name:     "unknown start kills",
			record:   ChildRecord{PID: 3, PGID: 3},
			alive:    true,
			wantKill: true,
		},
		{
			name:       "mismatched start errors",
			record:     ChildRecord{PID: 4, PGID: 4, StartUnixUS: 100_000_000},
			alive:      true,
			liveStart:  103_000_001,
			wantErr:    true,
			errContain: "identity anomaly",
		},
		{
			name:       "probe error errors",
			record:     ChildRecord{PID: 5, PGID: 5},
			aliveErr:   errEPERM,
			wantErr:    true,
			errContain: "operation not permitted",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planOrphanedChildren([]ChildRecord{tt.record}, procProbe{
				groupAlive: func(int) (bool, error) { return tt.alive, tt.aliveErr },
				startTime:  func(int) (int64, error) { return tt.liveStart, nil },
			})
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tt.errContain != "" && !strings.Contains(err.Error(), tt.errContain) {
					t.Fatalf("error = %v, want %q", err, tt.errContain)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (len(plan.kill) == 1) != tt.wantKill {
				t.Fatalf("kill = %#v, wantKill=%v", plan.kill, tt.wantKill)
			}
			if (len(plan.drop) == 1) != tt.wantDrop {
				t.Fatalf("drop = %#v, wantDrop=%v", plan.drop, tt.wantDrop)
			}
		})
	}
}

func TestProcessStartTime(t *testing.T) {
	before := time.Now().Add(-time.Hour).UnixMicro()
	start, err := processStartTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now().Add(childStartTolerance).UnixMicro()
	if start < before || start > after {
		t.Fatalf("process start = %d, want between %d and %d", start, before, after)
	}
	if _, err := processStartTime(999999999); err == nil {
		t.Fatal("nonexistent pid start time succeeded")
	}
}

func TestKillOrphanedChildrenIntegration(t *testing.T) {
	cfg := testConfig(t)
	initChildTracking(cfg)
	defer func() { procTracker = nil }()
	p, err := Start("", nil, nil, "sleep", "30")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Kill()
	records, err := loadChildRegistry(childRegistryPath(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].PID != p.cmd.Process.Pid || records[0].PGID != records[0].PID {
		t.Fatalf("records = %#v, pid=%d", records, p.cmd.Process.Pid)
	}
	if err := killOrphanedChildren(cfg, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("child did not exit")
	}
	if aliveNonZombie(p.cmd.Process.Pid) {
		t.Fatalf("pid %d still alive", p.cmd.Process.Pid)
	}
	records, err = loadChildRegistry(childRegistryPath(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("records after cleanup = %#v", records)
	}
}

func TestDeregisterOnExit(t *testing.T) {
	cfg := testConfig(t)
	initChildTracking(cfg)
	defer func() { procTracker = nil }()
	p, err := Start("", nil, nil, "true")
	if err != nil {
		t.Fatal(err)
	}
	<-p.Done()
	records, err := loadChildRegistry(childRegistryPath(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("records after true exit = %#v", records)
	}
}

func TestChildTrackingDisabled(t *testing.T) {
	cfg := testConfig(t)
	procTracker = nil
	p, err := Start("", nil, nil, "true")
	if err != nil {
		t.Fatal(err)
	}
	<-p.Done()
	if _, err := os.Stat(childRegistryPath(cfg)); !os.IsNotExist(err) {
		t.Fatalf("registry stat err = %v, want not exist", err)
	}
}

func aliveNonZombie(pid int) bool {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return false
	}
	return true
}
