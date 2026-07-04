package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type MergeService struct {
	cfg      Config
	slot     MergeSlot
	fuzzer   *FuzzerManager
	e2e      *E2EManager
	logf     func(string, ...any)
	deps     MergeServiceDeps
	noExec   bool
	shutdown func()
}

type MergeServiceDeps struct {
	now           func() time.Time
	head          func(context.Context, Config) (string, error)
	lastGood      func(Config) (string, error)
	gitReady      func(context.Context, Config, string) error
	stagedHead    func(context.Context, Config) (string, error)
	contains      func(context.Context, Config, string, string) error
	ffMerge       func(context.Context, Config) error
	resetHard     func(context.Context, Config, string) error
	untracked     func(context.Context, Config) (pathSet, error)
	deleteNew     func(Config, pathSet, pathSet) error
	touchedPaths  func(context.Context, Config, string) ([]string, error)
	gate          func(context.Context, Config) error
	golden        func(context.Context, Config) error
	oracleHandoff func(context.Context, []string) error
	replayAll     func(context.Context, Config) error
	rebuildFuzz   func(context.Context, Config, bool, *FuzzerManager) error
	rebuildE2E    func(context.Context, Config, *E2EManager) error
	selfRestart   func(context.Context, Config, MergeRequest, string, []string, func(), bool) (string, string)
}

func defaultMergeServiceDeps() MergeServiceDeps {
	return MergeServiceDeps{
		now:        time.Now,
		head:       GitHead,
		lastGood:   loadLastGoodCommit,
		gitReady:   ensureAttemptGitReady,
		stagedHead: gitStagedHead,
		contains:   gitContains,
		ffMerge:    gitMergeStagedFFOnly,
		resetHard:  GitResetHard,
		untracked:  GitUntrackedSet,
		deleteNew: func(cfg Config, before, after pathSet) error {
			_, err := DeleteNewUntracked(cfg, before, after)
			return err
		},
		touchedPaths: GitTouchedPaths,
		gate:         RunGate,
		golden:       RunGolden,
		replayAll:    preflightReplayAll,
		rebuildFuzz:  rebuildAndHotRestart,
		rebuildE2E:   rebuildAndHotRestartE2E,
		selfRestart:  handleSelfRestart,
	}
}

func NewMergeService(cfg Config, fuzzer *FuzzerManager, e2e *E2EManager, logf func(string, ...any)) *MergeService {
	return &MergeService{cfg: cfg, slot: NewMergeSlot(cfg), fuzzer: fuzzer, e2e: e2e, logf: logf, deps: defaultMergeServiceDeps()}
}

func (s *MergeService) ServiceClaimed(ctx context.Context, req MergeRequest) MergeResult {
	result := s.serviceClaimed(ctx, req)
	if err := s.slot.WriteResult(result); err != nil && s.logf != nil {
		s.logf("merge result write failed for %s: %v", req.ID, err)
	}
	return result
}

func (s *MergeService) serviceClaimed(ctx context.Context, req MergeRequest) MergeResult {
	if s.deps.now == nil {
		s.deps = defaultMergeServiceDeps()
	}
	if time.Until(s.cfg.Deadline) < time.Hour {
		return s.result(req.ID, mergeResultDeadlineTooClose, "", "deadline less than 1h away")
	}
	lastGood, err := s.deps.lastGood(s.cfg)
	if err != nil {
		return s.result(req.ID, mergeResultGitDrift, "", err.Error())
	}
	before, err := s.deps.untracked(ctx, s.cfg)
	if err != nil {
		return s.result(req.ID, mergeResultInternal, "", "untracked snapshot: "+err.Error())
	}
	if err := s.deps.gitReady(ctx, s.cfg, lastGood); err != nil {
		return s.result(req.ID, mergeResultGitDrift, "", err.Error())
	}
	stagedHead, err := s.deps.stagedHead(ctx, s.cfg)
	if err != nil {
		return s.result(req.ID, mergeResultStaleRequest, "", err.Error())
	}
	if stagedHead != req.ExpectStagedHead {
		return s.result(req.ID, mergeResultStaleRequest, "", fmt.Sprintf("staged head %s, expected %s", stagedHead, req.ExpectStagedHead))
	}
	acked := req.ExpectStagedHead
	if ack, err := s.slot.ReadAck(); err == nil && ack.ID == req.ID {
		acked = ack.AckedHead
	}
	if acked != "" {
		if err := s.deps.contains(ctx, s.cfg, req.ExpectStagedHead, acked); err != nil {
			return s.result(req.ID, mergeResultStaleRequest, "", err.Error())
		}
	}
	journal := MergeInflight{ID: req.ID, LastGood: lastGood, ExpectStagedHead: req.ExpectStagedHead}
	if err := atomicWriteJSON(filepath.Join(s.cfg.StateDir, "merge-inflight.json"), journal, 0o644); err != nil {
		return s.result(req.ID, mergeResultInternal, "", err.Error())
	}
	if err := s.deps.ffMerge(ctx, s.cfg); err != nil {
		_ = os.Remove(filepath.Join(s.cfg.StateDir, "merge-inflight.json"))
		return s.result(req.ID, mergeResultInternal, "", err.Error())
	}
	paths, err := s.deps.touchedPaths(ctx, s.cfg, lastGood)
	if err != nil {
		s.rollback(ctx, lastGood, before)
		return s.result(req.ID, mergeResultInternal, "", err.Error())
	}
	if err := s.deps.gate(ctx, s.cfg); err != nil {
		s.rollback(ctx, lastGood, before)
		return s.result(req.ID, mergeResultGateFailed, "", err.Error())
	}
	if engines := touchedOracleEngines(paths); len(engines) > 0 {
		oracleHandoff := s.copyStagedOracles
		if s.deps.oracleHandoff != nil {
			oracleHandoff = s.deps.oracleHandoff
		}
		if err := oracleHandoff(ctx, engines); err != nil {
			s.rollback(ctx, lastGood, before)
			return s.result(req.ID, mergeResultStaleOracleBinaries, "", err.Error())
		}
		if err := s.deps.golden(ctx, s.cfg); err != nil {
			s.rollback(ctx, lastGood, before)
			return s.result(req.ID, mergeResultGoldenFailed, "", err.Error())
		}
	}
	if err := s.deps.replayAll(ctx, s.cfg); err != nil {
		s.rollback(ctx, lastGood, before)
		return s.result(req.ID, mergeResultReplayRegressed, "", err.Error())
	}
	newHead, err := s.deps.head(ctx, s.cfg)
	if err != nil {
		s.rollback(ctx, lastGood, before)
		return s.result(req.ID, mergeResultInternal, "", err.Error())
	}
	if err := atomicWriteFile(filepath.Join(s.cfg.StateDir, "last_good_commit"), []byte(newHead+"\n"), 0o644); err != nil {
		s.rollback(ctx, lastGood, before)
		return s.result(req.ID, mergeResultInternal, "", err.Error())
	}
	// The merge is committed-good from this point: last_good_commit has
	// advanced, so crash recovery must no longer reset to the old head.
	_ = os.Remove(filepath.Join(s.cfg.StateDir, "merge-inflight.json"))
	if currentUntracked, err := s.deps.untracked(ctx, s.cfg); err == nil {
		_ = writeUntrackedBaseline(s.cfg, currentUntracked)
	} else if s.logf != nil {
		s.logf("untracked baseline refresh after merge %s failed: %v", req.ID, err)
	}
	_ = appendMergeRecord(s.cfg, MergeRecord{
		ID:           req.ID,
		PrevHead:     lastGood,
		NewHead:      newHead,
		TS:           s.deps.now().UTC(),
		Restart:      req.Restart,
		TouchedRoots: touchedRoots(paths),
	})
	if err := s.deps.rebuildFuzz(ctx, s.cfg, false, s.fuzzer); err != nil {
		return s.result(req.ID, mergeResultMerged, newHead, "merge validated; fuzzer rebuild failed: "+err.Error())
	}
	if touchesE2EBinary(paths) {
		if err := s.deps.rebuildE2E(ctx, s.cfg, s.e2e); err != nil && s.logf != nil {
			s.logf("e2e rebuild after merge %s failed: %v", req.ID, err)
		}
	}
	restartState, restartDetail := s.deps.selfRestart(ctx, s.cfg, req, newHead, paths, s.shutdown, s.noExec)
	result := s.result(req.ID, mergeResultMerged, newHead, restartDetail)
	result.SupervisorRestart = restartState
	return result
}

func (s *MergeService) rollback(ctx context.Context, lastGood string, before pathSet) {
	// Shutdown or Ctrl-C mid-validation cancels ctx while the tree is merged;
	// the rollback must still run, and the journal may only go once the reset
	// actually succeeded — otherwise startup crash recovery is the way back.
	rctx := context.WithoutCancel(ctx)
	if err := s.deps.resetHard(rctx, s.cfg, lastGood); err != nil {
		if s.logf != nil {
			s.logf("merge rollback reset failed; journal kept for crash recovery: %v", err)
		}
		return
	}
	after, err := s.deps.untracked(rctx, s.cfg)
	if err == nil && before != nil {
		_ = s.deps.deleteNew(s.cfg, before, after)
	}
	_ = os.Remove(filepath.Join(s.cfg.StateDir, "merge-inflight.json"))
}

func (s *MergeService) result(id, result, newHead, detail string) MergeResult {
	return MergeResult{ID: id, Result: result, NewHead: newHead, Detail: detail, SupervisorRestart: mergeSupervisorRestartNone}
}

func (s *MergeService) copyStagedOracles(ctx context.Context, engines []string) error {
	stagedBuild := filepath.Join(s.cfg.DDLDir, "staged", "tools", "ddlfuzz", "build")
	if _, err := os.Stat(stagedBuild); os.IsNotExist(err) {
		stagedBuild = filepath.Join(s.cfg.DDLDir, "staged", "build")
	}
	for _, engine := range engines {
		want, err := OracleSourceHash(s.cfg, engine)
		if err != nil {
			return err
		}
		manifestPath := filepath.Join(stagedBuild, "oracle-"+engine+".manifest.json")
		manifest, err := LoadOracleManifest(manifestPath)
		if err != nil {
			return fmt.Errorf("%s manifest missing/stale: %w", engine, err)
		}
		if manifest.Engine != engine || manifest.SourceHash != want {
			return fmt.Errorf("%s manifest hash mismatch", engine)
		}
		srcBin := filepath.Join(stagedBuild, "oracle-"+engine)
		dstBin := oracleBinaryPath(s.cfg, engine)
		if err := copyFileAtomic(srcBin, dstBin, 0o755); err != nil {
			return err
		}
		if err := copyFileAtomic(manifestPath, oracleManifestPath(s.cfg, engine), 0o644); err != nil {
			return err
		}
		if err := HelloSmoke(ctx, dstBin, engine); err != nil {
			return fmt.Errorf("%s copied oracle HELLO failed: %w", engine, err)
		}
	}
	return nil
}

func RecoverMergeInflight(ctx context.Context, cfg Config) (bool, error) {
	path := filepath.Join(cfg.StateDir, "merge-inflight.json")
	var journal MergeInflight
	if err := readJSONFile(path, &journal); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if journal.LastGood != "" {
		if err := GitResetHard(ctx, cfg, journal.LastGood); err != nil {
			return true, err
		}
	}
	slot := NewMergeSlot(cfg)
	_ = os.Remove(slot.claimedPath())
	_ = slot.WriteResult(MergeResult{ID: journal.ID, Result: mergeResultCrashRecovered, Detail: "reset to " + journal.LastGood, SupervisorRestart: mergeSupervisorRestartNone})
	_ = os.Remove(path)
	return true, nil
}

func appendMergeRecord(cfg Config, rec MergeRecord) error {
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(cfg.StateDir, "merges.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

type MergeRecord struct {
	ID           string    `json:"id"`
	PrevHead     string    `json:"prev_head"`
	NewHead      string    `json:"new_head"`
	TS           time.Time `json:"ts"`
	Restart      bool      `json:"restart"`
	TouchedRoots []string  `json:"touched_roots"`
}

func LoadMergeRecords(cfg Config, limit int) []MergeRecord {
	f, err := os.Open(filepath.Join(cfg.StateDir, "merges.jsonl"))
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	var records []MergeRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec MergeRecord
		if json.Unmarshal(sc.Bytes(), &rec) == nil {
			records = append(records, rec)
		}
	}
	if limit > 0 && len(records) > limit {
		records = records[len(records)-limit:]
	}
	return records
}

func gitStagedHead(ctx context.Context, cfg Config) (string, error) {
	res, err := runGit(ctx, cfg, 30*time.Second, "rev-parse", "ddlfuzz-staged")
	if err != nil {
		return "", fmt.Errorf("git rev-parse ddlfuzz-staged: %w: %s", err, resultOutputTail(res, 2000))
	}
	return strings.TrimSpace(res.Stdout), nil
}

func gitContains(ctx context.Context, cfg Config, commit, ancestor string) error {
	if strings.TrimSpace(commit) == "" || strings.TrimSpace(ancestor) == "" {
		return fmt.Errorf("empty merge-base input commit=%q ancestor=%q", commit, ancestor)
	}
	res, err := runGit(ctx, cfg, 30*time.Second, "merge-base", "--is-ancestor", ancestor, commit)
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("%s does not contain %s: %w", commit, ancestor, err)
	}
	return nil
}

func gitMergeStagedFFOnly(ctx context.Context, cfg Config) error {
	res, err := runGit(ctx, cfg, 2*time.Minute, "merge", "--ff-only", "ddlfuzz-staged")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("git merge --ff-only ddlfuzz-staged: %w: %s", err, resultOutputTail(res, 4000))
	}
	return nil
}

func touchedRoots(paths []string) []string {
	seen := map[string]bool{}
	for _, p := range paths {
		parts := strings.Split(filepath.ToSlash(p), "/")
		switch {
		case len(parts) >= 3 && parts[0] == "tools" && parts[1] == "ddlfuzz":
			seen[strings.Join(parts[:3], "/")] = true
		case len(parts) >= 2:
			seen[strings.Join(parts[:2], "/")] = true
		case p != "":
			seen[p] = true
		}
	}
	out := make([]string, 0, len(seen))
	for root := range seen {
		out = append(out, root)
	}
	sort.Strings(out)
	return out
}

func copyFileAtomic(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".tmp.")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, dst)
}
