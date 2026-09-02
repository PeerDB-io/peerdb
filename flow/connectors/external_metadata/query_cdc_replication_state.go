package connmetadata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/model"
)

const (
	queryCDCReplicationStateTableName = "query_cdc_replication_state"
	queryCDCAvroStageTableName        = "query_cdc_avro_stage"
)

// QueryCDCReplicationState is one source table's durable progress in the
// query-based CDC path (see connectors.QueryCDCPullConnector).
//
//nolint:govet // logically grouped, fieldalignment confuses things
type QueryCDCReplicationState struct {
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
	// LastNormalizedAt is when this table last completed a normalize
	// successfully, zero if never normalized.
	LastNormalizedAt time.Time
	// InsertsCount, UpdatesCount, DeletesCount are this table's cumulative
	// row counts normalized into its final destination table.
	InsertsCount int64
	UpdatesCount int64
	DeletesCount int64
}

// GetQueryCDCReplicationState reads a table's durable progress, defaulting to an
// empty state for a table never seen before.
func (p *PostgresMetadata) GetQueryCDCReplicationState(
	ctx context.Context, jobName string, sourceTableIdentifier string,
) (QueryCDCReplicationState, error) {
	var state QueryCDCReplicationState
	var lastAttemptAt, lastSyncedAt, lastNormalizedAt *time.Time
	if err := p.pool.QueryRow(ctx,
		`SELECT cursor_text, last_attempt_at, last_synced_at, synced_batch_id, normalized_batch_id, last_normalized_at,
			inserts_count, updates_count, deletes_count
		FROM `+queryCDCReplicationStateTableName+` WHERE flow_name = $1 AND source_table_identifier = $2`,
		jobName, sourceTableIdentifier,
	).Scan(&state.CursorText, &lastAttemptAt, &lastSyncedAt, &state.SyncedBatchID, &state.NormalizedBatchID, &lastNormalizedAt,
		&state.InsertsCount, &state.UpdatesCount, &state.DeletesCount); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return QueryCDCReplicationState{}, nil
		}
		return QueryCDCReplicationState{}, fmt.Errorf("failed to get table replication state for %s: %w", sourceTableIdentifier, err)
	}
	if lastAttemptAt != nil {
		state.LastAttemptAt = *lastAttemptAt
	}
	if lastSyncedAt != nil {
		state.LastSyncedAt = *lastSyncedAt
	}
	if lastNormalizedAt != nil {
		state.LastNormalizedAt = *lastNormalizedAt
	}
	return state, nil
}

// InitializeQueryCDCReplicationState seeds a table's cursor from the checkpoint of the snapshot
// that just ran, before any per-table poll starts.
//
// The upsert is what makes this safe to call more than once for the same table, which happens
// whenever the activity that snapshots and seeds is retried. Existing progress always wins: a
// retry snapshots as of a later time, and keeping the earlier cursor makes CDC re-read the window
// between the two attempts rather than skip it. That also means a plain insert conflict is not an
// error worth failing the retry over.
func (p *PostgresMetadata) InitializeQueryCDCReplicationState(
	ctx context.Context, jobName string, sourceTableIdentifier string, cursor string,
) error {
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO `+queryCDCReplicationStateTableName+` (flow_name, source_table_identifier, cursor_text)
		VALUES ($1, $2, $3)
		ON CONFLICT (flow_name, source_table_identifier)
		DO UPDATE SET cursor_text = excluded.cursor_text, updated_at = now()
		WHERE `+queryCDCReplicationStateTableName+`.cursor_text = ''
			AND `+queryCDCReplicationStateTableName+`.synced_batch_id = 0
			AND `+queryCDCReplicationStateTableName+`.normalized_batch_id = 0
	`, jobName, sourceTableIdentifier, cursor); err != nil {
		return fmt.Errorf("failed to initialize table replication state for %s: %w", sourceTableIdentifier, err)
	}
	return nil
}

// RecordQueryCDCAttempt records that a poll attempt for this table started at attemptedAt.
//
// It creates the row when missing, which normally does not happen: setup seeds every table's
// state from the snapshot it just took, so the first poll finds a row with a cursor. The
// exception is a table added with the initial load skipped -- nothing snapshots it, so it has no
// checkpoint to inherit and starts from this first poll instead (see
// InitializeQueryCDCReplicationState).
func (p *PostgresMetadata) RecordQueryCDCAttempt(
	ctx context.Context, jobName string, sourceTableIdentifier string, attemptedAt time.Time,
) error {
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO `+queryCDCReplicationStateTableName+` (flow_name, source_table_identifier, last_attempt_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (flow_name, source_table_identifier)
		DO UPDATE SET last_attempt_at = excluded.last_attempt_at, updated_at = now()
	`, jobName, sourceTableIdentifier, attemptedAt); err != nil {
		p.logger.Error("failed to record table replication attempt", slog.String("table", sourceTableIdentifier), slog.Any("error", err))
		return fmt.Errorf("failed to record table replication attempt for %s: %w", sourceTableIdentifier, err)
	}
	return nil
}

// RecordQueryCDCSync persists a table's new cursor and, if newBatchID
// is non-zero, advances its synced_batch_id after a successful poll that
// produced records staged for normalize. newBatchID is zero for a poll that
// found nothing new; the cursor still advances but there's no batch to
// normalize. The state row always exists by now, RecordQueryCDCAttempt
// created it before the poll started.
func (p *PostgresMetadata) RecordQueryCDCSync(
	ctx context.Context, jobName string, sourceTableIdentifier string, cursor string, syncedAt time.Time, newBatchID int64,
) error {
	if _, err := p.pool.Exec(ctx, `
		UPDATE `+queryCDCReplicationStateTableName+`
		SET cursor_text = $3,
			last_synced_at = $4,
			synced_batch_id = GREATEST(synced_batch_id, $5),
			updated_at = now()
		WHERE flow_name = $1 AND source_table_identifier = $2
	`, jobName, sourceTableIdentifier, cursor, syncedAt, newBatchID); err != nil {
		p.logger.Error("failed to record table replication sync", slog.String("table", sourceTableIdentifier), slog.Any("error", err))
		return fmt.Errorf("failed to record table replication sync for %s: %w", sourceTableIdentifier, err)
	}
	return nil
}

// RecordQueryCDCNormalize advances a table's normalized_batch_id
// after batches up through normalizedBatchID have been inserted into its
// final destination table, records normalizedAt as the completion time, and
// adds rowCounts to the table's cumulative insert/update/delete counts. The
// count increment is skipped alongside last_normalized_at if normalizedBatchID
// was already applied, so a retry replaying the same range doesn't double count.
func (p *PostgresMetadata) RecordQueryCDCNormalize(
	ctx context.Context, jobName string, sourceTableIdentifier string, normalizedBatchID int64,
	rowCounts *model.RecordTypeCounts, normalizedAt time.Time,
) error {
	if _, err := p.pool.Exec(ctx, `
		UPDATE `+queryCDCReplicationStateTableName+`
		SET normalized_batch_id = GREATEST(normalized_batch_id, $3),
			last_normalized_at = CASE WHEN $3 > normalized_batch_id THEN $4 ELSE last_normalized_at END,
			inserts_count = CASE WHEN $3 > normalized_batch_id THEN inserts_count + $5 ELSE inserts_count END,
			updates_count = CASE WHEN $3 > normalized_batch_id THEN updates_count + $6 ELSE updates_count END,
			deletes_count = CASE WHEN $3 > normalized_batch_id THEN deletes_count + $7 ELSE deletes_count END,
			updated_at = now()
		WHERE flow_name = $1 AND source_table_identifier = $2
	`, jobName, sourceTableIdentifier, normalizedBatchID, normalizedAt,
		rowCounts.InsertCount.Load(), rowCounts.UpdateCount.Load(), rowCounts.DeleteCount.Load()); err != nil {
		p.logger.Error("failed to record table replication normalize", slog.String("table", sourceTableIdentifier), slog.Any("error", err))
		return fmt.Errorf("failed to record table replication normalize for %s: %w", sourceTableIdentifier, err)
	}
	return nil
}

// PruneQueryCDCReplicationState deletes rows for source tables no longer in
// activeSourceTables, along with any Avro batches they left staged, which will
// never be normalized now that the table is gone from the mirror.
func (p *PostgresMetadata) PruneQueryCDCReplicationState(
	ctx context.Context, jobName string, activeSourceTables []string,
) error {
	for _, table := range []string{queryCDCReplicationStateTableName, queryCDCAvroStageTableName} {
		if _, err := p.pool.Exec(ctx,
			`DELETE FROM `+table+` WHERE flow_name = $1 AND NOT (source_table_identifier = ANY($2))`,
			jobName, activeSourceTables,
		); err != nil {
			return fmt.Errorf("failed to prune %s: %w", table, err)
		}
	}
	return nil
}

// deleteQueryCDCReplicationStateInTx drops all query-based CDC state for a
// flow: per-table progress plus any Avro batches still staged for normalize.
func deleteQueryCDCReplicationStateInTx(ctx context.Context, tx pgx.Tx, jobName string) error {
	for _, table := range []string{queryCDCReplicationStateTableName, queryCDCAvroStageTableName} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+table+` WHERE flow_name = $1`, jobName); err != nil {
			return err
		}
	}
	return nil
}
