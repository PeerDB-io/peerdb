package connmetadata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

const cdcTableReplicationStateTableName = "cdc_table_replication_state"

// TableReplicationState is one source table's durable progress in the
// isolated per-table CDC path (see connectors.TableCDCPullConnector).
//
//nolint:govet // logically grouped, fieldalignment confuses things
type TableReplicationState struct {
	// CursorText is the opaque cursor last returned by PullTableRecords for
	// this table, empty if this table has never been synced.
	CursorText string
	// LastAttemptAt is when the last poll attempt for this table started,
	// zero if never attempted.
	LastAttemptAt time.Time
	// LastSyncedAt is when this table last completed a poll successfully,
	// zero if never synced.
	LastSyncedAt time.Time
	// SyncedBatchID is the latest batch this table has staged (synced but not
	// necessarily normalized yet).
	SyncedBatchID int64
	// NormalizedBatchID is the latest batch this table has normalized into its
	// final destination table. SyncedBatchID-NormalizedBatchID is this table's
	// own sync/normalize lag, used for per-table backpressure.
	NormalizedBatchID int64
}

// GetTableReplicationState reads a table's durable progress, defaulting to an
// empty state for a table never seen before.
func (p *PostgresMetadata) GetTableReplicationState(
	ctx context.Context, jobName string, sourceTableIdentifier string,
) (TableReplicationState, error) {
	var state TableReplicationState
	var lastAttemptAt, lastSyncedAt *time.Time
	if err := p.pool.QueryRow(ctx,
		`SELECT cursor_text, last_attempt_at, last_synced_at, synced_batch_id, normalized_batch_id
		FROM `+cdcTableReplicationStateTableName+` WHERE flow_name = $1 AND source_table_identifier = $2`,
		jobName, sourceTableIdentifier,
	).Scan(&state.CursorText, &lastAttemptAt, &lastSyncedAt, &state.SyncedBatchID, &state.NormalizedBatchID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TableReplicationState{}, nil
		}
		return TableReplicationState{}, fmt.Errorf("failed to get table replication state for %s: %w", sourceTableIdentifier, err)
	}
	if lastAttemptAt != nil {
		state.LastAttemptAt = *lastAttemptAt
	}
	if lastSyncedAt != nil {
		state.LastSyncedAt = *lastSyncedAt
	}
	return state, nil
}

// RecordTableReplicationAttempt records that a poll attempt for this table
// started at attemptedAt, creating the row if this is the table's first poll.
func (p *PostgresMetadata) RecordTableReplicationAttempt(
	ctx context.Context, jobName string, sourceTableIdentifier string, attemptedAt time.Time,
) error {
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO `+cdcTableReplicationStateTableName+` (flow_name, source_table_identifier, last_attempt_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (flow_name, source_table_identifier)
		DO UPDATE SET last_attempt_at = excluded.last_attempt_at, updated_at = now()
	`, jobName, sourceTableIdentifier, attemptedAt); err != nil {
		p.logger.Error("failed to record table replication attempt", slog.String("table", sourceTableIdentifier), slog.Any("error", err))
		return fmt.Errorf("failed to record table replication attempt for %s: %w", sourceTableIdentifier, err)
	}
	return nil
}

// RecordTableReplicationSync persists a table's new cursor and, if newBatchID
// is non-zero, advances its synced_batch_id after a successful poll that
// produced records staged for normalize. newBatchID is zero for a poll that
// found nothing new; the cursor still advances but there's no batch to
// normalize.
func (p *PostgresMetadata) RecordTableReplicationSync(
	ctx context.Context, jobName string, sourceTableIdentifier string, cursor string, syncedAt time.Time, newBatchID int64,
) error {
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO `+cdcTableReplicationStateTableName+
		` (flow_name, source_table_identifier, cursor_text, last_attempt_at, last_synced_at, synced_batch_id)
		VALUES ($1, $2, $3, $4, $4, $5)
		ON CONFLICT (flow_name, source_table_identifier)
		DO UPDATE SET cursor_text = excluded.cursor_text, last_synced_at = excluded.last_synced_at,
			synced_batch_id = GREATEST(`+cdcTableReplicationStateTableName+`.synced_batch_id, excluded.synced_batch_id),
			updated_at = now()
	`, jobName, sourceTableIdentifier, cursor, syncedAt, newBatchID); err != nil {
		p.logger.Error("failed to record table replication sync", slog.String("table", sourceTableIdentifier), slog.Any("error", err))
		return fmt.Errorf("failed to record table replication sync for %s: %w", sourceTableIdentifier, err)
	}
	return nil
}

// RecordTableReplicationNormalize advances a table's normalized_batch_id after
// batches up through normalizedBatchID have been inserted into its final
// destination table.
func (p *PostgresMetadata) RecordTableReplicationNormalize(
	ctx context.Context, jobName string, sourceTableIdentifier string, normalizedBatchID int64,
) error {
	if _, err := p.pool.Exec(ctx, `
		UPDATE `+cdcTableReplicationStateTableName+`
		SET normalized_batch_id = GREATEST(normalized_batch_id, $3), updated_at = now()
		WHERE flow_name = $1 AND source_table_identifier = $2
	`, jobName, sourceTableIdentifier, normalizedBatchID); err != nil {
		p.logger.Error("failed to record table replication normalize", slog.String("table", sourceTableIdentifier), slog.Any("error", err))
		return fmt.Errorf("failed to record table replication normalize for %s: %w", sourceTableIdentifier, err)
	}
	return nil
}

// PruneTableReplicationState deletes rows for source tables no longer in
// activeSourceTables
func (p *PostgresMetadata) PruneTableReplicationState(
	ctx context.Context, jobName string, activeSourceTables []string,
) error {
	if _, err := p.pool.Exec(ctx,
		`DELETE FROM `+cdcTableReplicationStateTableName+` WHERE flow_name = $1 AND NOT (source_table_identifier = ANY($2))`,
		jobName, activeSourceTables,
	); err != nil {
		return fmt.Errorf("failed to prune table replication state: %w", err)
	}
	return nil
}

func deleteTableReplicationStateInTx(ctx context.Context, tx pgx.Tx, jobName string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM `+cdcTableReplicationStateTableName+` WHERE flow_name = $1`, jobName); err != nil {
		return err
	}
	return nil
}
