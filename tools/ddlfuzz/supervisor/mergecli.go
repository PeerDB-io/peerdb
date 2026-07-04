package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func mergeStagedCommand(cfg Config, args []string) error {
	fs := flag.NewFlagSet("ddlsuper merge-staged", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var noRestart bool
	var ackTimeoutText, resultTimeoutText string
	fs.BoolVar(&noRestart, "no-restart", false, "do not exec a rebuilt supervisor")
	fs.StringVar(&ackTimeoutText, "ack-timeout", "2h", "ack timeout")
	fs.StringVar(&resultTimeoutText, "result-timeout", "2h", "result timeout")
	if err := fs.Parse(args); err != nil {
		return cliExitError{code: 1, err: err}
	}
	ackTimeout, err := time.ParseDuration(ackTimeoutText)
	if err != nil {
		return cliExitError{code: 1, err: err}
	}
	resultTimeout, err := time.ParseDuration(resultTimeoutText)
	if err != nil {
		return cliExitError{code: 1, err: err}
	}
	if err := mergeStagedSanity(context.Background(), cfg); err != nil {
		return cliExitError{code: 2, err: err}
	}
	if err := mergeTreeDryRun(context.Background(), cfg); err != nil {
		return cliExitError{code: 2, err: err}
	}
	lock, err := TrySupervisorLock(cfg)
	if err == nil {
		defer func() { _ = lock.Close() }()
		result := runInlineMerge(context.Background(), cfg, !noRestart)
		printMergeResult(result)
		return cliExitError{code: mergeExitCode(result), err: nil}
	}
	id, err := randomMergeID()
	if err != nil {
		return cliExitError{code: 1, err: err}
	}
	slot := NewMergeSlot(cfg)
	req := MergeRequest{ID: id, PID: os.Getpid(), Phase: mergePhaseHold, Restart: !noRestart, CreatedAt: time.Now().UTC()}
	if err := occupySlot(slot, req); err != nil {
		if errors.Is(err, os.ErrExist) {
			printSlotBusy(slot)
			return cliExitError{code: 6, err: nil}
		}
		return cliExitError{code: 1, err: err}
	}
	ack, err := waitForAck(context.Background(), cfg, slot, id, ackTimeout)
	if err != nil {
		_ = slot.Cancel()
		return cliExitError{code: 5, err: err}
	}
	if err := mergeAckedHeadIntoStaged(context.Background(), cfg, ack.AckedHead); err != nil {
		_ = abortStagedMerge(context.Background(), cfg)
		_ = slot.Cancel()
		return cliExitError{code: 2, err: err}
	}
	stagedHead, err := gitStagedWorktreeHead(context.Background(), cfg)
	if err != nil {
		_ = slot.Cancel()
		return cliExitError{code: 1, err: err}
	}
	req.ExpectStagedHead = stagedHead
	if err := slot.Submit(req, stagedHead); err != nil {
		return cliExitError{code: 5, err: err}
	}
	result, err := waitForResult(context.Background(), cfg, slot, id, resultTimeout)
	if err != nil {
		return cliExitError{code: 5, err: err}
	}
	printMergeResult(result)
	_ = slot.Clear()
	return cliExitError{code: mergeExitCode(result), err: nil}
}

func mergeCancelCommand(cfg Config, args []string) error {
	fs := flag.NewFlagSet("ddlsuper merge-cancel", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var keepHold bool
	fs.BoolVar(&keepHold, "keep-hold", false, "replace with a fresh hold")
	if err := fs.Parse(args); err != nil {
		return cliExitError{code: 1, err: err}
	}
	slot := NewMergeSlot(cfg)
	err := slot.Cancel()
	if os.IsNotExist(err) {
		if _, claimedErr := slot.ReadClaimed(); claimedErr == nil {
			fmt.Println("merge request already claimed; it will merge or roll back on its own (result lands in state/merge/result.json)")
			return cliExitError{code: 7, err: nil}
		}
		if result, resultErr := slot.ReadResult(); resultErr == nil {
			printMergeResult(result)
			return cliExitError{code: 7, err: nil}
		}
		fmt.Println("nothing to cancel")
		return cliExitError{code: 0, err: nil}
	}
	if err != nil {
		return cliExitError{code: 1, err: err}
	}
	if keepHold {
		id, err := randomMergeID()
		if err != nil {
			return cliExitError{code: 1, err: err}
		}
		if err := slot.CreateRequest(MergeRequest{ID: id, PID: os.Getpid(), Phase: mergePhaseHold, Restart: true, CreatedAt: time.Now().UTC()}); err != nil {
			return cliExitError{code: 6, err: err}
		}
		fmt.Printf("canceled previous request; new hold id=%s\n", id)
		return cliExitError{code: 7, err: nil}
	}
	fmt.Println("merge request canceled")
	return cliExitError{code: 7, err: nil}
}

type cliExitError struct {
	code int
	err  error
}

func (e cliExitError) Error() string {
	if e.err == nil {
		return ""
	}
	return e.err.Error()
}

func exitCodeForError(err error, fallback int) int {
	var cli cliExitError
	if errors.As(err, &cli) {
		return cli.code
	}
	return fallback
}

func mergeStagedSanity(ctx context.Context, cfg Config) error {
	staged := filepath.Join(cfg.DDLDir, "staged")
	if info, err := os.Stat(staged); err != nil || !info.IsDir() {
		return fmt.Errorf("staged worktree missing: %s", staged)
	}
	branch, err := gitStagedWorktree(ctx, cfg, "branch", "--show-current")
	if err != nil {
		return err
	}
	if strings.TrimSpace(branch) != "ddlfuzz-staged" {
		return fmt.Errorf("staged branch=%q, want ddlfuzz-staged", strings.TrimSpace(branch))
	}
	status, err := gitStagedWorktree(ctx, cfg, "status", "--porcelain")
	if err != nil {
		return err
	}
	if strings.TrimSpace(status) != "" {
		return fmt.Errorf("staged worktree dirty:\n%s", status)
	}
	branch, err = GitBranch(ctx, cfg)
	if err != nil {
		return err
	}
	if branch != "parser-wip" {
		return fmt.Errorf("campaign branch=%q, want parser-wip", branch)
	}
	return nil
}

func mergeTreeDryRun(ctx context.Context, cfg Config) error {
	campaignHead, err := GitHead(ctx, cfg)
	if err != nil {
		return err
	}
	res, err := RunTimeout(ctx, filepath.Join(cfg.DDLDir, "staged"), 2*time.Minute, nil, "git", "merge-tree", "--write-tree", campaignHead, "ddlfuzz-staged")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("dry-run merge-tree failed: %w\n%s", err, resultOutputTail(res, 8000))
	}
	return nil
}

func waitForAck(ctx context.Context, cfg Config, slot MergeSlot, id string, timeout time.Duration) (MergeAck, error) {
	deadline := time.Now().Add(timeout)
	for {
		ack, err := slot.ReadAck()
		if err == nil && ack.ID == id {
			return ack, nil
		}
		if time.Now().After(deadline) {
			return MergeAck{}, fmt.Errorf("timed out waiting for merge ack")
		}
		if !supervisorLockHeld(cfg) {
			return MergeAck{}, fmt.Errorf("supervisor died before ack")
		}
		if err := sleepPoll(ctx); err != nil {
			return MergeAck{}, err
		}
	}
}

func waitForResult(ctx context.Context, cfg Config, slot MergeSlot, id string, timeout time.Duration) (MergeResult, error) {
	deadline := time.Now().Add(timeout)
	for {
		result, err := slot.ReadResult()
		if err == nil && result.ID == id {
			return result, nil
		}
		if time.Now().After(deadline) {
			return MergeResult{}, fmt.Errorf("timed out waiting for merge result")
		}
		if !supervisorLockHeld(cfg) {
			return MergeResult{}, fmt.Errorf("supervisor died before result; rerun to take over inline")
		}
		if err := sleepPoll(ctx); err != nil {
			return MergeResult{}, err
		}
	}
}

// occupySlot creates the request, adopting an abandoned hold (dead owner,
// e.g. left by merge-cancel --keep-hold) in place of failing busy. Adoption
// rewrites request.json, which is claim-safe: the supervisor only claims
// submitted requests, and the previous hold-phase writer is dead.
func occupySlot(slot MergeSlot, req MergeRequest) error {
	err := slot.CreateRequest(req)
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	existing, readErr := slot.ReadRequest()
	if readErr != nil || existing.Phase != mergePhaseHold || pidAlive(existing.PID) {
		return err
	}
	return slot.Rewrite(req)
}

func sleepPoll(ctx context.Context) error {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func mergeAckedHeadIntoStaged(ctx context.Context, cfg Config, head string) error {
	_, err := gitStagedWorktree(ctx, cfg, "merge", head)
	return err
}

func abortStagedMerge(ctx context.Context, cfg Config) error {
	_, err := gitStagedWorktree(ctx, cfg, "merge", "--abort")
	return err
}

func gitStagedWorktreeHead(ctx context.Context, cfg Config) (string, error) {
	out, err := gitStagedWorktree(ctx, cfg, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func gitStagedWorktree(ctx context.Context, cfg Config, args ...string) (string, error) {
	dir := filepath.Join(cfg.DDLDir, "staged")
	res, err := RunTimeout(ctx, dir, 2*time.Minute, nil, "git", args...)
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return "", fmt.Errorf("git -C staged %s: %w\n%s", strings.Join(args, " "), err, resultOutputTail(res, 8000))
	}
	return res.Stdout, nil
}

func TrySupervisorLock(cfg Config) (*SupervisorLock, error) {
	return AcquireSupervisorLock(cfg)
}

func supervisorLockHeld(cfg Config) bool {
	lock, err := TrySupervisorLock(cfg)
	if err == nil {
		_ = lock.Close()
		return false
	}
	return true
}

func runInlineMerge(ctx context.Context, cfg Config, restart bool) MergeResult {
	fail := func(result, detail string) MergeResult {
		return MergeResult{ID: "inline", Result: result, Detail: detail, SupervisorRestart: mergeSupervisorRestartNone}
	}
	id, err := randomMergeID()
	if err != nil {
		return fail(mergeResultInternal, err.Error())
	}
	id = "inline-" + id
	slot := NewMergeSlot(cfg)
	req := MergeRequest{ID: id, PID: os.Getpid(), Phase: mergePhaseSubmitted, Restart: restart, CreatedAt: time.Now().UTC()}
	if err := occupySlot(slot, req); err != nil {
		if errors.Is(err, os.ErrExist) {
			printSlotBusy(slot)
			return fail(mergeResultInternal, "merge slot busy")
		}
		return fail(mergeResultInternal, err.Error())
	}
	defer func() { _ = slot.Clear() }()
	if _, err := RecoverMergeInflight(ctx, cfg); err != nil {
		return fail(mergeResultInternal, err.Error())
	}
	head, err := GitHead(ctx, cfg)
	if err != nil {
		return fail(mergeResultInternal, err.Error())
	}
	if stored, err := loadLastGoodCommit(cfg); err == nil && stored != head {
		fmt.Printf("note: HEAD %s != last_good_commit %s; merging onto HEAD\n", shortSHA(head), shortSHA(stored))
	}
	if err := mergeAckedHeadIntoStaged(ctx, cfg, head); err != nil {
		_ = abortStagedMerge(ctx, cfg)
		return fail(mergeResultStaleRequest, err.Error())
	}
	staged, err := gitStagedHead(ctx, cfg)
	if err != nil {
		return fail(mergeResultStaleRequest, err.Error())
	}
	req.ExpectStagedHead = staged
	_ = slot.WriteAck(MergeAck{ID: req.ID, AckedHead: head})
	svc := NewMergeService(cfg, nil, nil, nil)
	svc.noExec = true
	// Inline runs between campaigns: HEAD is the base even when a stale
	// last_good_commit disagrees, and only tracked cleanliness gates it.
	svc.deps.lastGood = func(Config) (string, error) { return head, nil }
	svc.deps.gitReady = func(ctx context.Context, cfg Config, _ string) error {
		clean, dirty, err := GitTrackedClean(ctx, cfg)
		if err != nil {
			return err
		}
		if !clean {
			return fmt.Errorf("tracked tree dirty:\n%s", dirty)
		}
		return nil
	}
	return svc.serviceClaimed(ctx, req)
}

func printSlotBusy(slot MergeSlot) {
	req, err := slot.ReadRequest()
	if err != nil {
		fmt.Println("merge slot busy")
		return
	}
	fmt.Printf("merge slot busy: id=%s pid=%d age=%s\n", req.ID, req.PID, fmtAgo(time.Since(req.CreatedAt)))
}

func printMergeResult(result MergeResult) {
	fmt.Printf("result=%s", result.Result)
	if result.NewHead != "" {
		fmt.Printf(" new_head=%s", result.NewHead)
	}
	if result.SupervisorRestart != "" {
		fmt.Printf(" supervisor_restart=%s", result.SupervisorRestart)
	}
	if result.Detail != "" {
		fmt.Printf("\ndetail: %s", result.Detail)
	}
	fmt.Println()
}

func mergeExitCode(result MergeResult) int {
	switch result.Result {
	case mergeResultMerged:
		return 0
	case mergeResultGateFailed, mergeResultGoldenFailed, mergeResultStaleOracleBinaries:
		return 3
	case mergeResultReplayRegressed:
		return 4
	case mergeResultDeadlineTooClose, mergeResultGitDrift, mergeResultStaleRequest, mergeResultInternal, mergeResultCrashRecovered:
		return 2
	case mergeResultCanceled, mergeResultShuttingDown:
		return 7
	default:
		return 1
	}
}
