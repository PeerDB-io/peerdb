package connbigquery

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

const bigQueryCDCCheckpointVersion = 1

type bigQueryCDCTableProgress struct {
	SyncedThrough time.Time `json:"synced_through"`
	Target        time.Time `json:"target"`
	Active        bool      `json:"active"`
}

type bigQueryCDCCheckpoint struct {
	Tables  map[string]bigQueryCDCTableProgress `json:"tables"`
	Version int                                 `json:"version"`
}

type CDCBatchTableProgress struct {
	Completed     int
	Total         int
	LaggingTables []string
}

func parseBigQueryCDCCheckpoint(raw string, sourceTables []string) (*bigQueryCDCCheckpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("BigQuery CDC checkpoint is empty")
	}

	if !strings.HasPrefix(raw, "{") {
		legacy, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse BigQuery CDC checkpoint %q: %w", raw, err)
		}
		tables := make(map[string]bigQueryCDCTableProgress, len(sourceTables))
		for _, table := range sourceTables {
			tables[table] = bigQueryCDCTableProgress{SyncedThrough: legacy, Target: legacy, Active: true}
		}
		return &bigQueryCDCCheckpoint{Version: bigQueryCDCCheckpointVersion, Tables: tables}, nil
	}

	var checkpoint bigQueryCDCCheckpoint
	if err := json.Unmarshal([]byte(raw), &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to parse BigQuery CDC checkpoint JSON: %w", err)
	}
	if checkpoint.Version != bigQueryCDCCheckpointVersion {
		return nil, fmt.Errorf("unsupported BigQuery CDC checkpoint version %d", checkpoint.Version)
	}
	if checkpoint.Tables == nil {
		return nil, fmt.Errorf("BigQuery CDC checkpoint has no tables")
	}
	for table, progress := range checkpoint.Tables {
		if progress.SyncedThrough.IsZero() || progress.Target.IsZero() {
			return nil, fmt.Errorf("BigQuery CDC checkpoint for table %s has an empty timestamp", table)
		}
		if progress.Target.Before(progress.SyncedThrough) {
			return nil, fmt.Errorf("BigQuery CDC checkpoint target precedes synced-through timestamp for table %s", table)
		}
	}

	if sourceTables != nil {
		checkpoint.retainAndInitializeTables(sourceTables)
	}
	return &checkpoint, nil
}

func (c *bigQueryCDCCheckpoint) retainAndInitializeTables(sourceTables []string) {
	var fallback time.Time
	for _, progress := range c.Tables {
		if fallback.IsZero() || progress.SyncedThrough.Before(fallback) {
			fallback = progress.SyncedThrough
		}
	}

	tables := make(map[string]bigQueryCDCTableProgress, len(c.Tables)+len(sourceTables))
	for table, progress := range c.Tables {
		progress.Active = false
		tables[table] = progress
	}
	for _, table := range sourceTables {
		if progress, ok := c.Tables[table]; ok {
			progress.Active = true
			tables[table] = progress
		} else if !fallback.IsZero() {
			tables[table] = bigQueryCDCTableProgress{SyncedThrough: fallback, Target: fallback, Active: true}
		}
	}
	c.Tables = tables
}

func (c *bigQueryCDCCheckpoint) SyncedThrough(table string) (time.Time, error) {
	progress, ok := c.Tables[table]
	if !ok {
		return time.Time{}, fmt.Errorf("BigQuery CDC checkpoint is missing table %s", table)
	}
	return progress.SyncedThrough, nil
}

func (c *bigQueryCDCCheckpoint) RecordSuccess(table string, target time.Time) {
	c.Tables[table] = bigQueryCDCTableProgress{SyncedThrough: target, Target: target, Active: true}
}

// RecordFailure keeps the last confirmed cursor and records the attempted
// target. It reports whether this failure starts a new lagging episode.
func (c *bigQueryCDCCheckpoint) RecordFailure(table string, target time.Time) bool {
	progress := c.Tables[table]
	wasLagging := progress.Target.After(progress.SyncedThrough)
	progress.Target = target
	progress.Active = true
	c.Tables[table] = progress
	return !wasLagging
}

func (c *bigQueryCDCCheckpoint) Marshal() (string, error) {
	encoded, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("failed to encode BigQuery CDC checkpoint: %w", err)
	}
	return string(encoded), nil
}

func BigQueryCDCBatchTableProgress(batchCheckpointText, latestCheckpointText string) (CDCBatchTableProgress, bool) {
	if !strings.HasPrefix(strings.TrimSpace(batchCheckpointText), "{") ||
		!strings.HasPrefix(strings.TrimSpace(latestCheckpointText), "{") {
		return CDCBatchTableProgress{}, false
	}

	batch, err := parseBigQueryCDCCheckpoint(batchCheckpointText, nil)
	if err != nil {
		return CDCBatchTableProgress{}, false
	}
	latest, err := parseBigQueryCDCCheckpoint(latestCheckpointText, nil)
	if err != nil {
		return CDCBatchTableProgress{}, false
	}

	progress := CDCBatchTableProgress{}
	for table, batchTable := range batch.Tables {
		if !batchTable.Active {
			continue
		}
		progress.Total++
		latestTable, ok := latest.Tables[table]
		if ok && !latestTable.SyncedThrough.Before(batchTable.Target) {
			progress.Completed++
		} else {
			progress.LaggingTables = append(progress.LaggingTables, table)
		}
	}
	slices.Sort(progress.LaggingTables)
	return progress, true
}
