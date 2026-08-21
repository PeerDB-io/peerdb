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
	// SyncedThrough is the upper bound of the latest source window that was
	// successfully scanned and durably confirmed. Successful zero-row scans
	// advance it too.
	SyncedThrough time.Time `json:"synced_through"`
	// Target is the upper bound of the most recent scan attempt. It equals
	// SyncedThrough after success and remains ahead of it while a failed window
	// is waiting to be retried.
	Target time.Time `json:"target"`
	// Active records whether the source table belonged to the mirror when this
	// checkpoint was written. Removed tables remain in the checkpoint as
	// inactive entries so their last cursor is retained.
	Active bool `json:"active"`
}

type bigQueryCDCCheckpoint struct {
	// Tables maps fully qualified BigQuery source table identifiers to their
	// independent poll progress.
	Tables map[string]bigQueryCDCTableProgress `json:"tables"`
	// Version identifies the JSON checkpoint format read by
	// parseBigQueryCDCCheckpoint.
	Version int `json:"version"`
}

// CDCBatchTableProgress summarizes current source scan coverage for the source
// targets stored with a historical CDC batch. It does not describe destination
// normalization.
type CDCBatchTableProgress struct {
	// LaggingTables contains active source table identifiers whose current
	// SyncedThrough timestamp has not reached the target stored with the batch.
	LaggingTables []string
	// Completed is the number of active source tables whose current cursor has
	// reached the target stored with the batch.
	Completed int
	// Total is the number of active source tables recorded in the batch
	// checkpoint.
	Total int
}

func newBigQueryCDCCheckpoint(syncedThrough time.Time, sourceTables []string) *bigQueryCDCCheckpoint {
	tables := make(map[string]bigQueryCDCTableProgress, len(sourceTables))
	for _, table := range sourceTables {
		tables[table] = bigQueryCDCTableProgress{
			SyncedThrough: syncedThrough,
			Target:        syncedThrough,
			Active:        true,
		}
	}
	return &bigQueryCDCCheckpoint{Version: bigQueryCDCCheckpointVersion, Tables: tables}
}

func parseBigQueryCDCCheckpoint(raw string, sourceTables []string) (*bigQueryCDCCheckpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("BigQuery CDC checkpoint is empty")
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
