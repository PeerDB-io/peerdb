package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// CI joins these records with the JUnit durations to attribute e2e test time to waits
// (.github/scripts/summarize-e2e-timings.py). Without E2E_TIMINGS_DIR nothing is recorded.

type waitRecord struct {
	Test       string `json:"test"`
	Kind       string `json:"kind"`
	Reason     string `json:"reason"`
	StartMs    int64  `json:"start_ms"`
	DurationMs int64  `json:"duration_ms"`
	OK         bool   `json:"ok"`
}

var timings struct {
	enc  *json.Encoder
	mu   sync.Mutex
	once sync.Once
}

const (
	waitKindStatus = "status" // mirror creation until STATUS_RUNNING: setup flow and initial snapshot
	waitKindWait   = "wait"   // a test waiting for CDC or QRep results
	waitKindFinish = "finish" // waiting for the workflow to end, mostly teardown
)

func recordWait(t *testing.T, kind string, reason string, start time.Time, ok bool) {
	t.Helper()
	timings.once.Do(func() {
		dir := os.Getenv("E2E_TIMINGS_DIR")
		if dir == "" {
			return
		}
		// one file per test binary: go test runs packages as concurrent processes
		f, err := os.OpenFile( //nolint:gosec // G703: E2E_TIMINGS_DIR is set by CI, not by untrusted input
			filepath.Join(dir, fmt.Sprintf("%d.jsonl", os.Getpid())),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Logf("e2e timings disabled: %v", err)
			return
		}
		timings.enc = json.NewEncoder(f)
	})
	if timings.enc == nil {
		return
	}

	timings.mu.Lock()
	defer timings.mu.Unlock()
	if err := timings.enc.Encode(waitRecord{
		Test:       t.Name(),
		Kind:       kind,
		Reason:     reason,
		StartMs:    start.UnixMilli(),
		DurationMs: time.Since(start).Milliseconds(),
		OK:         ok,
	}); err != nil {
		t.Logf("failed to record e2e timing: %v", err)
	}
}
