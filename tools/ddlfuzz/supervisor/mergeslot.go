package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	mergePhaseHold      = "hold"
	mergePhaseSubmitted = "submitted"

	mergeResultMerged                = "merged"
	mergeResultCanceled              = "canceled"
	mergeResultDeadlineTooClose      = "deadline_too_close"
	mergeResultGitDrift              = "git_drift"
	mergeResultStaleRequest          = "stale_request"
	mergeResultInternal              = "internal"
	mergeResultGateFailed            = "gate_failed"
	mergeResultGoldenFailed          = "golden_failed"
	mergeResultStaleOracleBinaries   = "stale_oracle_binaries"
	mergeResultReplayRegressed       = "replay_regressed"
	mergeResultCrashRecovered        = "crash_recovered"
	mergeResultShuttingDown          = "shutting_down"
	mergeSupervisorRestartNone       = "none"
	mergeSupervisorRestartRequired   = "required"
	mergeSupervisorRestartRestarting = "restarting"
)

type MergeRequest struct {
	ID               string    `json:"id"`
	PID              int       `json:"pid"`
	Phase            string    `json:"phase"`
	Restart          bool      `json:"restart"`
	CreatedAt        time.Time `json:"created_at"`
	ExpectStagedHead string    `json:"expect_staged_head,omitempty"`
}

type MergeAck struct {
	ID        string `json:"id"`
	AckedHead string `json:"acked_head"`
}

type MergeResult struct {
	ID                string `json:"id"`
	Result            string `json:"result"`
	NewHead           string `json:"new_head,omitempty"`
	Detail            string `json:"detail,omitempty"`
	SupervisorRestart string `json:"supervisor_restart"`
}

type MergeInflight struct {
	ID               string `json:"id"`
	LastGood         string `json:"last_good"`
	ExpectStagedHead string `json:"expect_staged_head"`
}

type MergeSlot struct {
	dir string
	now func() time.Time
}

func NewMergeSlot(cfg Config) MergeSlot {
	return MergeSlot{dir: filepath.Join(cfg.StateDir, "merge"), now: time.Now}
}

func (s MergeSlot) withNow(now func() time.Time) MergeSlot {
	s.now = now
	return s
}

func randomMergeID() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s MergeSlot) requestPath() string { return filepath.Join(s.dir, "request.json") }
func (s MergeSlot) ackPath() string     { return filepath.Join(s.dir, "ack.json") }
func (s MergeSlot) claimedPath() string { return filepath.Join(s.dir, "claimed.json") }
func (s MergeSlot) resultPath() string  { return filepath.Join(s.dir, "result.json") }

func (s MergeSlot) CreateRequest(req MergeRequest) error {
	if req.ID == "" {
		return errors.New("merge request id is empty")
	}
	if req.PID == 0 {
		req.PID = os.Getpid()
	}
	if req.Phase == "" {
		req.Phase = mergePhaseHold
	}
	if req.CreatedAt.IsZero() {
		req.CreatedAt = s.now().UTC()
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	// Write the full content to a temp file first, then link() it into place:
	// link fails with EEXIST if a request already exists (the slot arbitration)
	// and never exposes a partially written request.json to concurrent readers.
	tmp, err := os.CreateTemp(s.dir, ".request.tmp.")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Link(tmpName, s.requestPath())
}

// Rewrite replaces request.json in place. Only valid for the hold phase with a
// dead prior owner (adoption) or the owner's own hold->submitted transition —
// the supervisor claims only submitted requests, so neither races the claim rename.
func (s MergeSlot) Rewrite(req MergeRequest) error {
	return atomicWriteJSON(s.requestPath(), req, 0o644)
}

func (s MergeSlot) Submit(req MergeRequest, expectHead string) error {
	if req.ID == "" {
		return errors.New("merge request id is empty")
	}
	req.Phase = mergePhaseSubmitted
	req.ExpectStagedHead = expectHead
	return s.Rewrite(req)
}

func (s MergeSlot) ReadRequest() (MergeRequest, error) {
	var req MergeRequest
	err := readJSONFile(s.requestPath(), &req)
	return req, err
}

func (s MergeSlot) ReadClaimed() (MergeRequest, error) {
	var req MergeRequest
	err := readJSONFile(s.claimedPath(), &req)
	return req, err
}

func (s MergeSlot) ReadAck() (MergeAck, error) {
	var ack MergeAck
	err := readJSONFile(s.ackPath(), &ack)
	return ack, err
}

func (s MergeSlot) WriteAck(ack MergeAck) error {
	return atomicWriteJSON(s.ackPath(), ack, 0o644)
}

func (s MergeSlot) ReadResult() (MergeResult, error) {
	var result MergeResult
	err := readJSONFile(s.resultPath(), &result)
	return result, err
}

func (s MergeSlot) WriteResult(result MergeResult) error {
	if result.SupervisorRestart == "" {
		result.SupervisorRestart = mergeSupervisorRestartNone
	}
	return atomicWriteJSON(s.resultPath(), result, 0o644)
}

func (s MergeSlot) Cancel() error {
	return os.Remove(s.requestPath())
}

func (s MergeSlot) Claim() (MergeRequest, error) {
	if err := os.Rename(s.requestPath(), s.claimedPath()); err != nil {
		return MergeRequest{}, err
	}
	return s.ReadClaimed()
}

func (s MergeSlot) Clear() error {
	var errs []string
	for _, path := range []string{s.requestPath(), s.ackPath(), s.claimedPath(), s.resultPath()} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (s MergeSlot) ExpireUnclaimed(maxAge time.Duration, alive func(int) bool) (bool, error) {
	req, err := s.ReadRequest()
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if req.PID > 0 && alive != nil && alive(req.PID) && s.now().Sub(req.CreatedAt) <= maxAge {
		return false, nil
	}
	return true, s.Clear()
}

func (s MergeSlot) CleanupResult(maxAge time.Duration, alive func(int) bool) (bool, error) {
	resultInfo, err := os.Stat(s.resultPath())
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	req, reqErr := s.ReadRequest()
	if reqErr == nil && req.PID > 0 && alive != nil && alive(req.PID) {
		return false, nil
	}
	if s.now().Sub(resultInfo.ModTime()) <= maxAge {
		return false, nil
	}
	return true, s.Clear()
}

func (s MergeSlot) HasHoldOrSubmit() bool {
	if req, err := s.ReadRequest(); err == nil && (req.Phase == mergePhaseHold || req.Phase == mergePhaseSubmitted) {
		return true
	}
	if req, err := s.ReadClaimed(); err == nil && req.Phase == mergePhaseSubmitted {
		return true
	}
	return false
}

func (s MergeSlot) SnapshotLine(now time.Time) string {
	if req, err := s.ReadRequest(); err == nil {
		return fmt.Sprintf("%s id=%s pid=%d age=%s", req.Phase, req.ID, req.PID, fmtAgo(now.Sub(req.CreatedAt)))
	}
	if req, err := s.ReadClaimed(); err == nil {
		return fmt.Sprintf("claimed id=%s pid=%d age=%s", req.ID, req.PID, fmtAgo(now.Sub(req.CreatedAt)))
	}
	if result, err := s.ReadResult(); err == nil {
		return fmt.Sprintf("result id=%s %s", result.ID, result.Result)
	}
	return "idle"
}

func readJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
