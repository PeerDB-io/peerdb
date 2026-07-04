package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMergeSlotStateMachine(t *testing.T) {
	cfg := testConfig(t)
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	slot := NewMergeSlot(cfg).withNow(func() time.Time { return now })
	req := MergeRequest{ID: "aaaaaaaaaaaa", PID: 1234, Phase: mergePhaseHold, Restart: true, CreatedAt: now}
	if err := slot.CreateRequest(req); err != nil {
		t.Fatal(err)
	}
	if err := slot.CreateRequest(MergeRequest{ID: "bbbbbbbbbbbb"}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second create err=%v, want EEXIST", err)
	}
	if err := slot.Cancel(); err != nil {
		t.Fatal(err)
	}
	if _, err := slot.ReadRequest(); !os.IsNotExist(err) {
		t.Fatalf("request after cancel err=%v", err)
	}

	if err := slot.CreateRequest(req); err != nil {
		t.Fatal(err)
	}
	claimed, err := slot.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != req.ID {
		t.Fatalf("claimed id=%s", claimed.ID)
	}
	if err := slot.Cancel(); !os.IsNotExist(err) {
		t.Fatalf("cancel after claim err=%v, want ENOENT", err)
	}
	if _, err := slot.ReadClaimed(); err != nil {
		t.Fatalf("claimed missing: %v", err)
	}
	if expired, err := slot.ExpireUnclaimed(6*time.Hour, func(int) bool { return false }); err != nil || expired {
		t.Fatalf("claimed expired=%v err=%v, want no expiry", expired, err)
	}
	if err := slot.Clear(); err != nil {
		t.Fatal(err)
	}

	if err := slot.CreateRequest(req); err != nil {
		t.Fatal(err)
	}
	if err := slot.Cancel(); err != nil {
		t.Fatal(err)
	}
	fresh := MergeRequest{ID: "cccccccccccc", PID: 1234, Phase: mergePhaseHold, Restart: true, CreatedAt: now}
	if err := slot.CreateRequest(fresh); err != nil {
		t.Fatal(err)
	}
	got, err := slot.ReadRequest()
	if err != nil || got.ID == req.ID || got.ID != fresh.ID {
		t.Fatalf("fresh hold got=%#v err=%v", got, err)
	}

	if err := slot.Clear(); err != nil {
		t.Fatal(err)
	}
	old := MergeRequest{ID: "dddddddddddd", PID: 9999, Phase: mergePhaseHold, CreatedAt: now.Add(-7 * time.Hour)}
	if err := slot.CreateRequest(old); err != nil {
		t.Fatal(err)
	}
	if expired, err := slot.ExpireUnclaimed(6*time.Hour, func(int) bool { return true }); err != nil || !expired {
		t.Fatalf("age expiry=%v err=%v, want expired", expired, err)
	}
	if err := slot.CreateRequest(MergeRequest{ID: "eeeeeeeeeeee", PID: 9999, Phase: mergePhaseHold, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if expired, err := slot.ExpireUnclaimed(6*time.Hour, func(int) bool { return false }); err != nil || !expired {
		t.Fatalf("dead pid expiry=%v err=%v, want expired", expired, err)
	}

	if err := slot.WriteAck(MergeAck{ID: "stale", AckedHead: "old"}); err != nil {
		t.Fatal(err)
	}
	ack, err := slot.ReadAck()
	if err != nil {
		t.Fatal(err)
	}
	if ack.ID == fresh.ID {
		t.Fatalf("stale ack matched fresh request")
	}
}

func TestOracleManifestHash(t *testing.T) {
	cfg := testConfig(t)
	mustWrite(t, filepath.Join(cfg.Root, "tools", "ddlfuzz", "oracle", "proto", "proto.hpp"), "proto\n")
	mustWrite(t, filepath.Join(cfg.Root, "tools", "ddlfuzz", "oracle", "mysql", "b.cc"), "b\n")
	mustWrite(t, filepath.Join(cfg.Root, "tools", "ddlfuzz", "oracle", "mysql", "a.cc"), "a\n")
	first, err := OracleSourceHash(cfg, "mysql")
	if err != nil {
		t.Fatal(err)
	}
	second, err := OracleSourceHash(cfg, "mysql")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("hash not deterministic: %s %s", first, second)
	}
	mustWrite(t, filepath.Join(cfg.Root, "tools", "ddlfuzz", "oracle", "mysql", "dirty.tmp"), "dirty\n")
	dirty, err := OracleSourceHash(cfg, "mysql")
	if err != nil {
		t.Fatal(err)
	}
	if dirty == first {
		t.Fatalf("dirty-tree file did not affect hash")
	}
	mustWrite(t, filepath.Join(cfg.Root, "tools", "ddlfuzz", "oracle", "mysql", "a.cc"), "changed\n")
	changed, err := OracleSourceHash(cfg, "mysql")
	if err != nil {
		t.Fatal(err)
	}
	if changed == dirty {
		t.Fatalf("content change did not affect hash")
	}
	out := filepath.Join(cfg.BuildDir, "oracle-mysql.manifest.json")
	if err := oracleManifestCommand(cfg, []string{"--engine", "mysql", "--out", out}); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadOracleManifest(out)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Engine != "mysql" || manifest.SourceHash != changed || manifest.ServerTag == "" || manifest.BuiltAt.IsZero() {
		t.Fatalf("manifest mismatch: %#v want hash %s", manifest, changed)
	}
}

func TestMergeServiceDecisionTable(t *testing.T) {
	base := func(t *testing.T) (*MergeService, MergeRequest, *int, *int) {
		t.Helper()
		cfg := testConfig(t)
		initTestRepo(t, cfg)
		cfg.Deadline = time.Now().Add(2 * time.Hour)
		slot := NewMergeSlot(cfg)
		_ = slot.WriteAck(MergeAck{ID: "req", AckedHead: "acked"})
		req := MergeRequest{ID: "req", PID: 1, Phase: mergePhaseSubmitted, Restart: true, CreatedAt: time.Now(), ExpectStagedHead: "staged"}
		resetCalls := 0
		rebuildCalls := 0
		svc := NewMergeService(cfg, nil, nil, nil)
		svc.deps = defaultMergeServiceDeps()
		svc.deps.head = func(context.Context, Config) (string, error) { return "newhead", nil }
		svc.deps.lastGood = func(Config) (string, error) { return "lastgood", nil }
		svc.deps.gitReady = func(context.Context, Config, string) error { return nil }
		svc.deps.stagedHead = func(context.Context, Config) (string, error) { return "staged", nil }
		svc.deps.contains = func(context.Context, Config, string, string) error { return nil }
		svc.deps.ffMerge = func(context.Context, Config) error { return nil }
		svc.deps.resetHard = func(context.Context, Config, string) error { resetCalls++; return nil }
		svc.deps.deleteNew = func(Config, pathSet, pathSet) error { return nil }
		svc.deps.touchedPaths = func(context.Context, Config, string) ([]string, error) {
			return []string{"flow/connectors/mysql/ddl_parser.go"}, nil
		}
		svc.deps.gate = func(context.Context, Config) error { return nil }
		svc.deps.golden = func(context.Context, Config) error { return nil }
		svc.deps.oracleHandoff = func(context.Context, []string) error { return nil }
		svc.deps.replayAll = func(context.Context, Config) error { return nil }
		svc.deps.rebuildFuzz = func(context.Context, Config, bool, *FuzzerManager) error { rebuildCalls++; return nil }
		svc.deps.rebuildE2E = func(context.Context, Config, *E2EManager) error { return nil }
		svc.deps.selfRestart = func(context.Context, Config, MergeRequest, string, []string, func(), bool) (string, string) {
			return mergeSupervisorRestartNone, ""
		}
		return svc, req, &resetCalls, &rebuildCalls
	}

	t.Run("deadline too close", func(t *testing.T) {
		svc, req, _, _ := base(t)
		svc.cfg.Deadline = time.Now().Add(30 * time.Minute)
		got := svc.serviceClaimed(context.Background(), req)
		if got.Result != mergeResultDeadlineTooClose {
			t.Fatalf("result=%#v", got)
		}
	})
	t.Run("git drift", func(t *testing.T) {
		svc, req, _, _ := base(t)
		svc.deps.gitReady = func(context.Context, Config, string) error { return errors.New("dirty") }
		got := svc.serviceClaimed(context.Background(), req)
		if got.Result != mergeResultGitDrift {
			t.Fatalf("result=%#v", got)
		}
	})
	t.Run("stale request", func(t *testing.T) {
		svc, req, _, _ := base(t)
		svc.deps.stagedHead = func(context.Context, Config) (string, error) { return "other", nil }
		got := svc.serviceClaimed(context.Background(), req)
		if got.Result != mergeResultStaleRequest {
			t.Fatalf("result=%#v", got)
		}
	})
	t.Run("not ff internal", func(t *testing.T) {
		svc, req, _, _ := base(t)
		svc.deps.ffMerge = func(context.Context, Config) error { return errors.New("not ff") }
		got := svc.serviceClaimed(context.Background(), req)
		if got.Result != mergeResultInternal || !strings.Contains(got.Detail, "not ff") {
			t.Fatalf("result=%#v", got)
		}
	})
	for _, tt := range []struct {
		name string
		want string
		hook func(*MergeService)
	}{
		{name: "gate rollback", want: mergeResultGateFailed, hook: func(s *MergeService) {
			s.deps.gate = func(context.Context, Config) error { return errors.New("gate") }
		}},
		{name: "golden rollback", want: mergeResultGoldenFailed, hook: func(s *MergeService) {
			s.deps.touchedPaths = func(context.Context, Config, string) ([]string, error) {
				return []string{"tools/ddlfuzz/oracle/mysql/driver.cc"}, nil
			}
			s.deps.golden = func(context.Context, Config) error { return errors.New("golden") }
		}},
		{name: "stale oracle rollback", want: mergeResultStaleOracleBinaries, hook: func(s *MergeService) {
			s.deps.touchedPaths = func(context.Context, Config, string) ([]string, error) {
				return []string{"tools/ddlfuzz/oracle/mysql/driver.cc"}, nil
			}
			s.deps.oracleHandoff = func(context.Context, []string) error { return errors.New("stale oracle") }
		}},
		{name: "replay rollback", want: mergeResultReplayRegressed, hook: func(s *MergeService) {
			s.deps.replayAll = func(context.Context, Config) error { return errors.New("regressed") }
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			svc, req, resetCalls, _ := base(t)
			tt.hook(svc)
			got := svc.serviceClaimed(context.Background(), req)
			if got.Result != tt.want {
				t.Fatalf("result=%#v want %s", got, tt.want)
			}
			if *resetCalls == 0 {
				t.Fatalf("rollback not called")
			}
			if _, err := os.Stat(filepath.Join(svc.cfg.StateDir, "merge-inflight.json")); !os.IsNotExist(err) {
				t.Fatalf("journal not cleared: %v", err)
			}
		})
	}
	t.Run("success lifecycle", func(t *testing.T) {
		svc, req, resetCalls, rebuildCalls := base(t)
		got := svc.serviceClaimed(context.Background(), req)
		if got.Result != mergeResultMerged || got.NewHead != "newhead" || *resetCalls != 0 || *rebuildCalls != 1 {
			t.Fatalf("result=%#v reset=%d rebuild=%d", got, *resetCalls, *rebuildCalls)
		}
		if data := string(mustRead(t, filepath.Join(svc.cfg.StateDir, "last_good_commit"))); strings.TrimSpace(data) != "newhead" {
			t.Fatalf("last_good=%q", data)
		}
		if records := LoadMergeRecords(svc.cfg, 10); len(records) != 1 || records[0].NewHead != "newhead" {
			t.Fatalf("merge records=%#v", records)
		}
		if _, err := os.Stat(filepath.Join(svc.cfg.StateDir, "merge-inflight.json")); !os.IsNotExist(err) {
			t.Fatalf("journal not cleared: %v", err)
		}
	})
}

func TestOccupySlotAdoption(t *testing.T) {
	cfg := testConfig(t)
	slot := NewMergeSlot(cfg)
	dead := MergeRequest{ID: "deaddeaddead", PID: 1 << 30, Phase: mergePhaseHold, CreatedAt: time.Now().UTC()}
	if err := slot.CreateRequest(dead); err != nil {
		t.Fatal(err)
	}
	mine := MergeRequest{ID: "aaaaaaaaaaaa", PID: os.Getpid(), Phase: mergePhaseHold, CreatedAt: time.Now().UTC()}
	if err := occupySlot(slot, mine); err != nil {
		t.Fatalf("adoption failed: %v", err)
	}
	got, err := slot.ReadRequest()
	if err != nil || got.ID != mine.ID {
		t.Fatalf("adopted request=%#v err=%v", got, err)
	}

	if err := occupySlot(slot, MergeRequest{ID: "bbbbbbbbbbbb", PID: os.Getpid(), Phase: mergePhaseHold, CreatedAt: time.Now().UTC()}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("live-owner occupy err=%v, want EEXIST", err)
	}

	if err := slot.Rewrite(MergeRequest{ID: mine.ID, PID: 1 << 30, Phase: mergePhaseSubmitted, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := occupySlot(slot, MergeRequest{ID: "cccccccccccc", PID: os.Getpid(), Phase: mergePhaseHold, CreatedAt: time.Now().UTC()}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("submitted-phase occupy err=%v, want EEXIST (never adopt submitted)", err)
	}
}

func TestInitRunStart(t *testing.T) {
	cfg := testConfig(t)
	stale := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mustWrite(t, filepath.Join(cfg.StateDir, "run-start"), stale.Format(time.RFC3339)+"\n")
	cfg.RunHours = 72
	if err := initRunStart(&cfg); err != nil {
		t.Fatal(err)
	}
	if got, err := loadRunStart(cfg); err != nil || !got.Equal(cfg.StartedAt) {
		t.Fatalf("fresh start kept stale run-start: got=%v err=%v want=%v", got, err, cfg.StartedAt)
	}

	mustWrite(t, filepath.Join(cfg.StateDir, "run-start"), stale.Format(time.RFC3339)+"\n")
	if err := writeRestartRequired(cfg, "abc123"); err != nil {
		t.Fatal(err)
	}
	resumed := cfg
	resumed.StartedAt = time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
	if err := initRunStart(&resumed); err != nil {
		t.Fatal(err)
	}
	if !resumed.StartedAt.Equal(stale) {
		t.Fatalf("resume did not adopt persisted run-start: %v", resumed.StartedAt)
	}
	if want := stale.Add(72 * time.Hour); !resumed.Deadline.Equal(want) {
		t.Fatalf("resume deadline=%v want=%v", resumed.Deadline, want)
	}
	if got, err := loadRunStart(resumed); err != nil || !got.Equal(stale) {
		t.Fatalf("resume rewrote run-start: got=%v err=%v", got, err)
	}
}

func TestHandleSelfRestartDegradesToMarker(t *testing.T) {
	cfg := testConfig(t)
	req := MergeRequest{ID: "aaaaaaaaaaaa", Restart: true}

	state, _ := handleSelfRestart(context.Background(), cfg, req, "head1", []string{"flow/connectors/mysql/ddl_parser.go"}, nil, false)
	if state != mergeSupervisorRestartNone {
		t.Fatalf("non-supervisor diff state=%s", state)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, "RESTART_REQUIRED")); !os.IsNotExist(err) {
		t.Fatalf("marker written for non-supervisor diff")
	}

	// cfg.DDLDir has no Go module, so the rebuild fails: the restart must
	// degrade to RESTART_REQUIRED without arming the exec path.
	state, detail := handleSelfRestart(context.Background(), cfg, req, "head1", []string{"tools/ddlfuzz/supervisor/main.go"}, nil, false)
	if state != mergeSupervisorRestartRequired || !strings.Contains(detail, "rebuild failed") {
		t.Fatalf("state=%s detail=%q", state, detail)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, "RESTART_REQUIRED")); err != nil {
		t.Fatalf("RESTART_REQUIRED missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, "selfrestart.json")); !os.IsNotExist(err) {
		t.Fatalf("selfrestart.json written on failed rebuild")
	}
	if execRestartPending.Load() {
		t.Fatalf("exec armed on failed rebuild")
	}
}
