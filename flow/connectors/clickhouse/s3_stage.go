package connclickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
)

// ErrNoAvroStage is returned by GetTableAvroStage when no stage row exists for
// the requested batch.
var ErrNoAvroStage = errors.New("no avro stage found")

func SetAvroStage(
	ctx context.Context,
	flowJobName string,
	syncBatchID int64,
	avroFile utils.AvroFile,
) error {
	avroFileJSON, err := json.Marshal(avroFile)
	if err != nil {
		return fmt.Errorf("failed to marshal avro file: %w", err)
	}

	conn, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	if _, err := conn.Exec(ctx, `
		INSERT INTO ch_s3_stage (flow_job_name, sync_batch_id, avro_file)
		VALUES ($1, $2, $3)
		ON CONFLICT (flow_job_name, sync_batch_id)
		DO UPDATE SET avro_file = $3, created_at = CURRENT_TIMESTAMP`,
		flowJobName, syncBatchID, avroFileJSON,
	); err != nil {
		return fmt.Errorf("failed to set avro stage: %w", err)
	}

	return nil
}

func GetAvroStage(ctx context.Context, flowJobName string, syncBatchID int64) (utils.AvroFile, error) {
	conn, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return utils.AvroFile{}, fmt.Errorf("failed to get connection: %w", err)
	}

	var avroFileJSON []byte
	if err := conn.QueryRow(ctx, `
		SELECT avro_file FROM ch_s3_stage
		WHERE flow_job_name = $1 AND sync_batch_id = $2`,
		flowJobName, syncBatchID,
	).Scan(&avroFileJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return utils.AvroFile{}, fmt.Errorf("no avro stage found for flow job %s and sync batch %d", flowJobName, syncBatchID)
		}
		return utils.AvroFile{}, fmt.Errorf("failed to get avro stage: %w", err)
	}

	var avroFile utils.AvroFile
	if err := json.Unmarshal(avroFileJSON, &avroFile); err != nil {
		return utils.AvroFile{}, fmt.Errorf("failed to unmarshal avro file: %w", err)
	}

	return avroFile, nil
}

// SetTableAvroStage records a table's staged Avro file for the isolated
// per-table CDC path (see flow/activities/flowable_isolated_cdc.go). Unlike
// SetAvroStage/GetAvroStage, batchID here is a per-table sequence, not the
// flow-wide sync batch ID, one row per (flow, table, table's own batch).
// firstRowReceivedAt/firstRowCommitTime are the batch's first row event's
// received/commit timestamps (nil if the batch had no row events), read back
// at normalize time for per-table destination/e2e lag.
func SetTableAvroStage(
	ctx context.Context, flowJobName string, sourceTableIdentifier string, batchID int64, avroFile utils.AvroFile,
	rowCounts *model.RecordTypeCounts, firstRowReceivedAt *time.Time, firstRowCommitTime *time.Time,
) error {
	avroFileJSON, err := json.Marshal(avroFile)
	if err != nil {
		return fmt.Errorf("failed to marshal avro file: %w", err)
	}

	conn, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	if _, err := conn.Exec(ctx, `
		INSERT INTO cdc_table_avro_stage (flow_name, source_table_identifier, batch_id, avro_file, inserts_count, updates_count, deletes_count,
			first_row_received_at, first_row_commit_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (flow_name, source_table_identifier, batch_id)
		DO UPDATE SET avro_file = $4, inserts_count = $5, updates_count = $6, deletes_count = $7, created_at = CURRENT_TIMESTAMP,
			first_row_received_at = $8, first_row_commit_time = $9`,
		flowJobName, sourceTableIdentifier, batchID, avroFileJSON,
		rowCounts.InsertCount.Load(), rowCounts.UpdateCount.Load(), rowCounts.DeleteCount.Load(),
		firstRowReceivedAt, firstRowCommitTime,
	); err != nil {
		return fmt.Errorf("failed to set table avro stage: %w", err)
	}

	return nil
}

// GetTableAvroStage retrieves a table's staged Avro file for batchID, along
// with the insert/update/delete counts and first row received/commit times
// staged with it (the latter two nil if the batch had no row events).
func GetTableAvroStage(
	ctx context.Context, flowJobName string, sourceTableIdentifier string, batchID int64,
) (utils.AvroFile, *model.RecordTypeCounts, *time.Time, *time.Time, error) {
	conn, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return utils.AvroFile{}, nil, nil, nil, fmt.Errorf("failed to get connection: %w", err)
	}

	var avroFileJSON []byte
	var insertsCount, updatesCount, deletesCount int64
	var firstRowReceivedAt, firstRowCommitTime *time.Time
	if err := conn.QueryRow(ctx, `
		SELECT avro_file, inserts_count, updates_count, deletes_count, first_row_received_at, first_row_commit_time
		FROM cdc_table_avro_stage
		WHERE flow_name = $1 AND source_table_identifier = $2 AND batch_id = $3`,
		flowJobName, sourceTableIdentifier, batchID,
	).Scan(&avroFileJSON, &insertsCount, &updatesCount, &deletesCount, &firstRowReceivedAt, &firstRowCommitTime); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return utils.AvroFile{}, nil, nil, nil, fmt.Errorf(
				"%w for flow job %s, table %s, batch %d", ErrNoAvroStage, flowJobName, sourceTableIdentifier, batchID)
		}
		return utils.AvroFile{}, nil, nil, nil, fmt.Errorf("failed to get table avro stage: %w", err)
	}

	var avroFile utils.AvroFile
	if err := json.Unmarshal(avroFileJSON, &avroFile); err != nil {
		return utils.AvroFile{}, nil, nil, nil, fmt.Errorf("failed to unmarshal avro file: %w", err)
	}

	rowCounts := &model.RecordTypeCounts{}
	rowCounts.InsertCount.Store(int32(insertsCount))
	rowCounts.UpdateCount.Store(int32(updatesCount))
	rowCounts.DeleteCount.Store(int32(deletesCount))

	return avroFile, rowCounts, firstRowReceivedAt, firstRowCommitTime, nil
}

// DeleteTableAvroStage removes a table's staged Avro record for batchID once
// it's been normalized. The underlying S3/GCS object is not removed here.
func DeleteTableAvroStage(ctx context.Context, flowJobName string, sourceTableIdentifier string, batchID int64) error {
	conn, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	if _, err := conn.Exec(ctx,
		`DELETE FROM cdc_table_avro_stage WHERE flow_name = $1 AND source_table_identifier = $2 AND batch_id = $3`,
		flowJobName, sourceTableIdentifier, batchID,
	); err != nil {
		return fmt.Errorf("failed to delete table avro stage: %w", err)
	}

	return nil
}
