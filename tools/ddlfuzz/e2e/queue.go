package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type queueItem struct {
	Sig         string `json:"sig"`
	Engine      string `json:"engine"`
	SQLMode     uint64 `json:"sql_mode"`
	SQLModeName string `json:"sql_mode_name"`
	Statement   string `json:"statement"`

	path string
}

type queueResult struct {
	Sig     string `json:"sig"`
	Result  string `json:"result"`
	Details string `json:"details,omitempty"`
}

func ensureStateLayout(stateDir string) error {
	for _, rel := range []string{
		"findings",
		"e2e-queue/pending",
		"e2e-queue/processing",
		"e2e-queue/done",
	} {
		if err := os.MkdirAll(filepath.Join(stateDir, rel), 0o755); err != nil {
			return err
		}
	}
	return nil
}

func requeueStaleProcessing(stateDir string, olderThan time.Duration) error {
	processing := filepath.Join(stateDir, "e2e-queue", "processing")
	pending := filepath.Join(stateDir, "e2e-queue", "pending")
	entries, err := os.ReadDir(processing)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	cutoff := time.Now().Add(-olderThan)
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		src := filepath.Join(processing, ent.Name())
		info, err := ent.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		_ = os.Rename(src, filepath.Join(pending, ent.Name()))
	}
	return nil
}

func pollQueue(ctx context.Context, stateDir string, engineChans map[string]chan<- queueItem, stats *Stats) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	_ = requeueStaleProcessing(stateDir, 10*time.Minute)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			items, err := claimQueueItems(stateDir)
			if err != nil {
				continue
			}
			for _, item := range items {
				ch := engineChans[item.Engine]
				if ch == nil {
					_ = completeQueueItem(stateDir, item, queueResult{Sig: item.Sig, Result: "exec-reject", Details: "unknown engine"})
					continue
				}
				select {
				case ch <- item:
				case <-ctx.Done():
					return
				default:
					_ = os.Rename(item.path, filepath.Join(stateDir, "e2e-queue", "pending", filepath.Base(item.path)))
				}
			}
			for engine := range engineChans {
				stats.SetPending(engine, stats.Snapshot()[engine].MatcherPending)
			}
		}
	}
}

func claimQueueItems(stateDir string) ([]queueItem, error) {
	pending := filepath.Join(stateDir, "e2e-queue", "pending")
	processing := filepath.Join(stateDir, "e2e-queue", "processing")
	entries, err := os.ReadDir(pending)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []queueItem
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		src := filepath.Join(pending, ent.Name())
		dst := filepath.Join(processing, ent.Name())
		if err := os.Rename(src, dst); err != nil {
			continue
		}
		data, err := os.ReadFile(dst)
		if err != nil {
			_ = os.Rename(dst, src)
			continue
		}
		var item queueItem
		if err := json.Unmarshal(data, &item); err != nil {
			_ = completeQueueItem(stateDir, queueItem{Sig: strings.TrimSuffix(ent.Name(), ".json"), path: dst}, queueResult{
				Sig:     strings.TrimSuffix(ent.Name(), ".json"),
				Result:  "exec-reject",
				Details: "invalid queue json: " + err.Error(),
			})
			continue
		}
		if item.Sig == "" {
			item.Sig = strings.TrimSuffix(ent.Name(), ".json")
		}
		item.Engine = strings.ToLower(item.Engine)
		item.path = dst
		out = append(out, item)
	}
	return out, nil
}

func completeQueueItem(stateDir string, item queueItem, result queueResult) error {
	if result.Sig == "" {
		result.Sig = item.Sig
	}
	if result.Sig == "" {
		return fmt.Errorf("queue item without sig")
	}
	done := filepath.Join(stateDir, "e2e-queue", "done", result.Sig+".json")
	if err := writeJSONAtomic(done, result); err != nil {
		return err
	}
	if item.path != "" {
		_ = os.Remove(item.path)
	}
	return nil
}
