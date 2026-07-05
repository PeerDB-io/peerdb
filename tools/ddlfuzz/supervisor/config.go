package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const defaultRepoRoot = "/Users/ilia/Code/peerdb"

type Config struct {
	Root            string
	DDLDir          string
	StateDir        string
	BuildDir        string
	PromptPath      string
	DDLfuzzBin      string
	E2EBin          string
	MySQLOracle     string
	MariaOracle     string
	MySQLBuild      string
	MariaBuild      string
	ComposeFile     string
	FixModel        string
	MaxTokens       int64
	AttemptTO       time.Duration
	RunHours        float64
	GoCache         string
	GoModCache      string
	LintCache       string
	StartedAt       time.Time
	Deadline        time.Time
	ReportEvery     time.Duration
	SampleEvery     time.Duration
	DiskEvery       time.Duration
	FindingEvery    time.Duration
	CoverageEvery   time.Duration
	CrossCheckEvery time.Duration
}

func LoadConfig() (Config, error) {
	root, err := discoverRoot()
	if err != nil {
		return Config{}, err
	}
	root = filepath.Clean(root)
	ddlDir := filepath.Join(root, "tools", "ddlfuzz")
	stateDir := filepath.Join(ddlDir, "state")
	buildDir := filepath.Join(ddlDir, "build")
	hours, err := envFloat("DDLFUZZ_HOURS", 72)
	if err != nil {
		return Config{}, err
	}
	attemptTO, err := envDuration("DDLFUZZ_ATTEMPT_TIMEOUT", 45*time.Minute)
	if err != nil {
		return Config{}, err
	}
	maxTokens, err := envInt64("DDLFUZZ_MAX_TOKENS", 0)
	if err != nil {
		return Config{}, err
	}
	model := os.Getenv("DDLFUZZ_FIX_MODEL")
	if model == "" {
		model = "gpt-5.5"
	}
	home, _ := os.UserHomeDir()
	now := time.Now()
	cfg := Config{
		Root:            root,
		DDLDir:          ddlDir,
		StateDir:        stateDir,
		BuildDir:        buildDir,
		PromptPath:      filepath.Join(ddlDir, "supervisor", "prompt.tmpl"),
		DDLfuzzBin:      filepath.Join(buildDir, "ddlfuzz"),
		E2EBin:          filepath.Join(buildDir, "ddlfuzz-e2e"),
		MySQLOracle:     filepath.Join(buildDir, "oracle-mysql"),
		MariaOracle:     filepath.Join(buildDir, "oracle-mariadb"),
		MySQLBuild:      filepath.Join(ddlDir, "oracle", "mysql", "build.sh"),
		MariaBuild:      filepath.Join(ddlDir, "oracle", "mariadb", "build.sh"),
		ComposeFile:     filepath.Join(ddlDir, "e2e", "compose.yml"),
		FixModel:        model,
		MaxTokens:       maxTokens,
		AttemptTO:       attemptTO,
		RunHours:        hours,
		LintCache:       filepath.Join(home, "Library", "Caches", "golangci-lint"),
		StartedAt:       now,
		Deadline:        now.Add(time.Duration(hours * float64(time.Hour))),
		ReportEvery:     time.Hour,
		SampleEvery:     5 * time.Second,
		DiskEvery:       10 * time.Minute,
		FindingEvery:    3 * time.Second,
		CoverageEvery:   time.Hour,
		CrossCheckEvery: 6 * time.Hour,
	}
	return cfg, nil
}

func discoverRoot() (string, error) {
	if v := os.Getenv("DDLFUZZ_ROOT"); v != "" {
		if !filepath.IsAbs(v) {
			return "", fmt.Errorf("DDLFUZZ_ROOT must be absolute: %s", v)
		}
		return v, nil
	}
	if wd, err := os.Getwd(); err == nil {
		if rel, err := filepath.Rel(defaultRepoRoot, wd); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return defaultRepoRoot, nil
		}
	}
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err == nil {
		root := strings.TrimSpace(string(out))
		if root != "" {
			return root, nil
		}
	}
	return defaultRepoRoot, nil
}

func envDuration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: %w", name, v, err)
	}
	return d, nil
}

func envFloat(name string, def float64) (float64, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		if err == nil {
			err = errors.New("must be positive")
		}
		return 0, fmt.Errorf("%s=%q: %w", name, v, err)
	}
	return f, nil
}

func envInt64(name string, def int64) (int64, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		if err == nil {
			err = errors.New("must be non-negative")
		}
		return 0, fmt.Errorf("%s=%q: %w", name, v, err)
	}
	return n, nil
}

func (c Config) ensureUnderRoot(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(c.Root, abs)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("refusing to write outside repo: %s", path)
	}
	return nil
}

func (c Config) ensureStateDirs() error {
	dirs := []string{
		c.StateDir,
		filepath.Join(c.StateDir, "attempts"),
		filepath.Join(c.StateDir, "escalations"),
		filepath.Join(c.StateDir, "findings"),
		filepath.Join(c.StateDir, "coverage", "history"),
		filepath.Join(c.StateDir, "e2e-queue", "pending"),
		filepath.Join(c.StateDir, "e2e-queue", "processing"),
		filepath.Join(c.StateDir, "e2e-queue", "done"),
		filepath.Join(c.StateDir, "merge"),
		c.BuildDir,
	}
	for _, dir := range dirs {
		if err := c.ensureUnderRoot(dir); err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp.")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
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
	return os.Rename(tmpName, path)
}

func atomicWriteJSON(path string, v any, perm os.FileMode) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data, perm)
}

func tailString(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[len(s)-max:]
}

func relTo(base, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(rel)
}

func shortSHA(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 12 {
		return s[:12]
	}
	if s == "" {
		return "n/a"
	}
	return s
}
