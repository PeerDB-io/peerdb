package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type pathSet map[string]struct{}

type Commit struct {
	SHA     string
	Subject string
}

type UntrackedDeletionPlan struct {
	ToDelete []string
	Kept     []string
}

func runGit(ctx context.Context, cfg Config, timeout time.Duration, args ...string) (Result, error) {
	full := append([]string{"-C", cfg.Root}, args...)
	return RunTimeout(ctx, cfg.Root, timeout, nil, "git", full...)
}

func GitBranch(ctx context.Context, cfg Config) (string, error) {
	res, err := runGit(ctx, cfg, 15*time.Second, "branch", "--show-current")
	if err != nil {
		return "", fmt.Errorf("git branch --show-current: %w: %s", err, resultOutputTail(res, 2000))
	}
	return strings.TrimSpace(res.Stdout), nil
}

func GitHead(ctx context.Context, cfg Config) (string, error) {
	res, err := runGit(ctx, cfg, 15*time.Second, "rev-parse", "HEAD")
	if err != nil {
		return "", fmt.Errorf("git rev-parse HEAD: %w: %s", err, resultOutputTail(res, 2000))
	}
	return strings.TrimSpace(res.Stdout), nil
}

func GitTrackedClean(ctx context.Context, cfg Config) (bool, string, error) {
	res, err := runGit(ctx, cfg, 30*time.Second, "status", "--porcelain")
	if err != nil {
		return false, "", fmt.Errorf("git status --porcelain: %w: %s", err, resultOutputTail(res, 2000))
	}
	var dirty []string
	sc := bufio.NewScanner(strings.NewReader(res.Stdout))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "?? ") {
			continue
		}
		if strings.TrimSpace(line) != "" {
			dirty = append(dirty, line)
		}
	}
	if err := sc.Err(); err != nil {
		return false, "", err
	}
	return len(dirty) == 0, strings.Join(dirty, "\n"), nil
}

func GitUntrackedSet(ctx context.Context, cfg Config) (pathSet, error) {
	res, err := runGit(ctx, cfg, 30*time.Second, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return nil, fmt.Errorf("git status --porcelain --untracked-files=all: %w: %s", err, resultOutputTail(res, 2000))
	}
	out := make(pathSet)
	sc := bufio.NewScanner(strings.NewReader(res.Stdout))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "?? ") {
			p := filepath.ToSlash(strings.TrimSpace(strings.TrimPrefix(line, "?? ")))
			if p != "" {
				out[p] = struct{}{}
			}
		}
	}
	return out, sc.Err()
}

func writeUntrackedBaseline(cfg Config, set pathSet) error {
	lines := set.sorted()
	if len(lines) > 0 {
		return atomicWriteFile(filepath.Join(cfg.StateDir, "untracked.baseline"), []byte(strings.Join(lines, "\n")+"\n"), 0o644)
	}
	return atomicWriteFile(filepath.Join(cfg.StateDir, "untracked.baseline"), []byte{}, 0o644)
}

func loadUntrackedBaseline(cfg Config) (pathSet, error) {
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "untracked.baseline"))
	if err != nil {
		return nil, err
	}
	set := make(pathSet)
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		p := strings.TrimSpace(sc.Text())
		if p != "" {
			set[filepath.ToSlash(p)] = struct{}{}
		}
	}
	return set, sc.Err()
}

func (s pathSet) sorted() []string {
	out := make([]string, 0, len(s))
	for p := range s {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func GitResetHard(ctx context.Context, cfg Config, sha string) error {
	res, err := runGit(ctx, cfg, 2*time.Minute, "reset", "--hard", sha)
	if err != nil {
		return fmt.Errorf("git reset --hard %s: %w: %s", sha, err, resultOutputTail(res, 4000))
	}
	return nil
}

func GitCommitsSince(ctx context.Context, cfg Config, sha string) ([]Commit, error) {
	res, err := runGit(ctx, cfg, time.Minute, "log", "--reverse", "--format=%H%x00%s", sha+"..HEAD")
	if err != nil {
		return nil, fmt.Errorf("git log %s..HEAD: %w: %s", sha, err, resultOutputTail(res, 4000))
	}
	var commits []Commit
	for _, line := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\x00", 2)
		c := Commit{SHA: parts[0]}
		if len(parts) == 2 {
			c.Subject = parts[1]
		}
		commits = append(commits, c)
	}
	return commits, nil
}

func GitTouchedPaths(ctx context.Context, cfg Config, sha string) ([]string, error) {
	res, err := runGit(ctx, cfg, time.Minute, "diff", "--name-only", sha+"..HEAD")
	if err != nil {
		return nil, fmt.Errorf("git diff --name-only %s..HEAD: %w: %s", sha, err, resultOutputTail(res, 4000))
	}
	var paths []string
	sc := bufio.NewScanner(strings.NewReader(res.Stdout))
	for sc.Scan() {
		p := filepath.ToSlash(strings.TrimSpace(sc.Text()))
		if p != "" {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	return paths, sc.Err()
}

func GitNameStatus(ctx context.Context, cfg Config, sha string, diffFilter string, paths ...string) ([]string, error) {
	args := []string{"diff", "--name-status"}
	if diffFilter != "" {
		args = append(args, "--diff-filter="+diffFilter)
	}
	args = append(args, sha+"..HEAD", "--")
	args = append(args, paths...)
	res, err := runGit(ctx, cfg, time.Minute, args...)
	if err != nil {
		return nil, fmt.Errorf("git diff --name-status %s..HEAD: %w: %s", sha, err, resultOutputTail(res, 4000))
	}
	var rows []string
	sc := bufio.NewScanner(strings.NewReader(res.Stdout))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			rows = append(rows, line)
		}
	}
	return rows, sc.Err()
}

func GitSaveDiff(ctx context.Context, cfg Config, sha, outPath string) error {
	res, err := runGit(ctx, cfg, 2*time.Minute, "diff", sha+"..HEAD")
	if err != nil {
		return fmt.Errorf("git diff %s..HEAD: %w: %s", sha, err, resultOutputTail(res, 4000))
	}
	return atomicWriteFile(outPath, []byte(res.Stdout), 0o644)
}

func PlanUntrackedDeletion(before, after pathSet, allowedRoots, excludedPrefixes []string) UntrackedDeletionPlan {
	var plan UntrackedDeletionPlan
	for p := range after {
		if _, existed := before[p]; existed {
			continue
		}
		raw := filepath.ToSlash(p)
		if hasPathSegment(raw, "..") {
			plan.Kept = append(plan.Kept, p)
			continue
		}
		clean := filepath.ToSlash(filepath.Clean(p))
		if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, "../") || clean == ".." || strings.Contains(clean, "/../") || strings.HasPrefix(clean, ".git/") {
			plan.Kept = append(plan.Kept, p)
			continue
		}
		if hasAnyPrefix(clean, excludedPrefixes) || !hasAnyPrefix(clean, allowedRoots) {
			plan.Kept = append(plan.Kept, p)
			continue
		}
		plan.ToDelete = append(plan.ToDelete, clean)
	}
	sort.Strings(plan.ToDelete)
	sort.Strings(plan.Kept)
	return plan
}

func DeleteNewUntracked(cfg Config, before, after pathSet) (UntrackedDeletionPlan, error) {
	baseline, err := loadUntrackedBaseline(cfg)
	if err != nil {
		if !os.IsNotExist(err) {
			return UntrackedDeletionPlan{}, fmt.Errorf("untracked baseline unreadable, skipping deletion: %w", err)
		}
		baseline = nil
	}
	protected := make(pathSet, len(before)+len(baseline))
	for p := range before {
		protected[p] = struct{}{}
	}
	for p := range baseline {
		protected[p] = struct{}{}
	}
	plan := PlanUntrackedDeletion(protected, after,
		[]string{"flow/", "tools/ddlfuzz/"},
		[]string{"tools/ddlfuzz/state/", "tools/ddlfuzz/build/", "tools/ddlfuzz/worktrees/", "tools/ddlfuzz/staged/"},
	)
	var errs []string
	for _, rel := range plan.ToDelete {
		abs := filepath.Join(cfg.Root, filepath.FromSlash(rel))
		if err := cfg.ensureUnderRoot(abs); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if err := os.RemoveAll(abs); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", rel, err))
		}
	}
	if len(errs) > 0 {
		return plan, errors.New(strings.Join(errs, "; "))
	}
	return plan, nil
}

func hasAnyPrefix(p string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(p, filepath.ToSlash(prefix)) {
			return true
		}
	}
	return false
}

func hasPathSegment(p, segment string) bool {
	for _, part := range strings.Split(filepath.ToSlash(p), "/") {
		if part == segment {
			return true
		}
	}
	return false
}

func forbiddenTouchedPaths(paths []string, primarySig string) []string {
	var bad []string
	touchesFlow := false
	touchesOracle := false
	touchesCompare := false
	for _, p := range paths {
		switch {
		case p == "tools/ddlfuzz/state/parked.list":
			bad = append(bad, p)
		case strings.HasPrefix(p, ".github/"):
			bad = append(bad, p)
		case strings.HasPrefix(p, "flow/") && !strings.HasPrefix(p, "flow/connectors/mysql/"):
			bad = append(bad, p)
		case p == "docs/mysql-clickhouse-charset-e2e.md" || p == "docs/temporal-history.md" || p == "flow/e2e/clickhouse_mysql_charset_test.go":
			bad = append(bad, p)
		case strings.HasPrefix(p, "tools/ddlfuzz/state/findings/") && strings.HasSuffix(p, "/meta.json"):
			parts := strings.Split(p, "/")
			if len(parts) >= 5 && parts[4] != primarySig {
				bad = append(bad, p)
			}
		}
		if strings.HasPrefix(p, "flow/") {
			touchesFlow = true
		}
		if strings.HasPrefix(p, "tools/ddlfuzz/oracle/") {
			touchesOracle = true
		}
		if strings.HasPrefix(p, "tools/ddlfuzz/internal/compare/") {
			touchesCompare = true
		}
	}
	if touchesFlow && touchesOracle {
		bad = append(bad, "flow/ and tools/ddlfuzz/oracle/ touched in same attempt")
	}
	if touchesFlow && touchesCompare {
		bad = append(bad, "flow/ and tools/ddlfuzz/internal/compare/ touched in same attempt")
	}
	return bad
}
