//go:build ddlfuzz

package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type sideChannels struct {
	stateDir string
	mu       sync.Mutex
}

type execRejectRecord struct {
	Engine      string `json:"engine"`
	Source      string `json:"source"`
	Statement   string `json:"statement"`
	SQLModeName string `json:"sql_mode_name"`
	Error       string `json:"error"`
}

type liveAcceptedRecord struct {
	Engine    string `json:"engine"`
	SQLMode   uint64 `json:"sql_mode"`
	Statement string `json:"statement"`
}

func newSideChannels(stateDir string) *sideChannels {
	return &sideChannels{stateDir: stateDir}
}

func (s *sideChannels) AppendExecReject(r execRejectRecord) error {
	return s.appendJSONL(filepath.Join(s.stateDir, "e2e-exec-rejects.jsonl"), r)
}

func (s *sideChannels) AppendLiveAccepted(r liveAcceptedRecord) error {
	return s.appendJSONL(filepath.Join(s.stateDir, "e2e-live-accepted.jsonl"), r)
}

func (s *sideChannels) appendJSONL(path string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(v); err != nil {
		return err
	}
	return nil
}
