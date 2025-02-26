package connmetadata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	lastSyncStateTableName = "metadata_last_sync_state"
	qrepTableName          = "metadata_qrep_partitions"
)

type PostgresMetadata struct {
	pool   shared.CatalogPool
	logger log.Logger
}

func NewPostgresMetadata(ctx context.Context) (*PostgresMetadata, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog connection pool: %w", err)
	}

	return &PostgresMetadata{
		pool:   pool,
		logger: internal.LoggerFromCtx(ctx),
	}, nil
}

func NewPostgresMetadataFromCatalog(logger log.Logger, pool shared.CatalogPool) *PostgresMetadata {
	return &PostgresMetadata{
		pool:   pool,
		logger: logger,
	}
}

func (p *PostgresMetadata) Ping(ctx context.Context) error {
	if err := p.pool.Ping(ctx); err != nil {
		return fmt.Errorf("metadata db ping failed: %w", err)
	}

	return nil
}

func (p *PostgresMetadata) LogFlowInfo(ctx context.Context, flowName string, info string) error {
	_, err := p.pool.Exec(ctx,
		"INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, info, "info")
	return err
}

func (p *PostgresMetadata) NeedsSetupMetadataTables(_ context.Context) (bool, error) {
	return false, nil
}

func (p *PostgresMetadata) SetupMetadataTables(_ context.Context) error {
	return nil
}

func (p *PostgresMetadata) GetLastOffset(ctx context.Context, jobName string) (model.CdcCheckpoint, error) {
	var offset model.CdcCheckpoint
	if err := p.pool.QueryRow(ctx,
		`SELECT last_offset, last_text FROM `+lastSyncStateTableName+` WHERE job_name = $1`,
		jobName,
	).Scan(&offset.ID, &offset.Text); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return offset, nil
		}

		p.logger.Error("failed to get last offset", "error", err)
		return offset, err
	}

	p.logger.Info("got last offset for job", "offset", offset)

	return offset, nil
}

func (p *PostgresMetadata) GetLastSyncBatchID(ctx context.Context, jobName string) (int64, error) {
	var syncBatchID pgtype.Int8
	if err := p.pool.QueryRow(ctx,
		`SELECT sync_batch_id FROM `+lastSyncStateTableName+` WHERE job_name = $1`,
		jobName,
	).Scan(&syncBatchID); err != nil {
		// if the job doesn't exist, return 0
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}

		p.logger.Error("failed to get last sync batch id", "error", err)
		return 0, err
	}
	p.logger.Info("got last batch id for job", "batch id", syncBatchID.Int64)

	return syncBatchID.Int64, nil
}

func (p *PostgresMetadata) GetLastNormalizeBatchID(ctx context.Context, jobName string) (int64, error) {
	var normalizeBatchID pgtype.Int8
	if err := p.pool.QueryRow(ctx,
		`SELECT normalize_batch_id FROM `+lastSyncStateTableName+` WHERE job_name = $1`,
		jobName,
	).Scan(&normalizeBatchID); err != nil {
		// if the job doesn't exist, return 0
		if err.Error() == "no rows in result set" {
			return 0, nil
		}

		p.logger.Error("failed to get last normalize", "error", err)
		return 0, err
	}
	p.logger.Info("got last normalize batch normalize id for job", "batch id", normalizeBatchID.Int64)

	return normalizeBatchID.Int64, nil
}

func (p *PostgresMetadata) SetLastOffset(ctx context.Context, jobName string, offset model.CdcCheckpoint) error {
	p.logger.Debug("updating last offset", slog.String("offsetID", pglogrepl.LSN(offset.ID).String()), slog.String("offsetText", offset.Text))
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO `+lastSyncStateTableName+` (job_name, last_offset, last_text, sync_batch_id)
		VALUES ($1, $2, $3, 0)
		ON CONFLICT (job_name)
		DO UPDATE SET
			last_offset = GREATEST(`+lastSyncStateTableName+`.last_offset, excluded.last_offset),
			last_text = excluded.last_text,
			updated_at = NOW()
	`, jobName, offset.ID, offset.Text); err != nil {
		p.logger.Error("failed to update last offset", "error", err)
		return err
	}

	return nil
}

func (p *PostgresMetadata) FinishBatch(ctx context.Context, jobName string, syncBatchID int64, offset model.CdcCheckpoint) error {
	p.logger.Info("finishing batch", "SyncBatchID", syncBatchID, "offset", offset)
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO `+lastSyncStateTableName+` (job_name, last_offset, last_text, sync_batch_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (job_name)
		DO UPDATE SET
			last_offset = GREATEST(`+lastSyncStateTableName+`.last_offset, excluded.last_offset),
			last_text = excluded.last_text,
			sync_batch_id = GREATEST(`+lastSyncStateTableName+`.sync_batch_id, excluded.sync_batch_id),
			updated_at = NOW()
	`, jobName, offset.ID, offset.Text, syncBatchID); err != nil {
		p.logger.Error("failed to finish batch", slog.Any("error", err))
		return err
	}

	return nil
}

func (p *PostgresMetadata) UpdateNormalizeBatchID(ctx context.Context, jobName string, batchID int64) error {
	p.logger.Info("updating normalize batch id for job", slog.Int64("normalizeBatchID", batchID))
	if _, err := p.pool.Exec(ctx,
		`UPDATE `+lastSyncStateTableName+` SET normalize_batch_id=$2 WHERE job_name=$1`, jobName, batchID,
	); err != nil {
		p.logger.Error("failed to update normalize batch id", slog.Int64("batchID", batchID), slog.Any("error", err))
		return err
	}

	if err := monitoring.UpdateEndTimeForCDCBatch(ctx, p.pool, jobName, batchID); err != nil {
		p.logger.Error("failed to update end time for cdc batch", slog.Int64("batchID", batchID), slog.Any("error", err))
		return err
	}

	return nil
}

func (p *PostgresMetadata) FinishQRepPartition(
	ctx context.Context,
	partition *protos.QRepPartition,
	jobName string,
	startTime time.Time,
) error {
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return fmt.Errorf("failed to marshal partition to json: %w", err)
	}
	partitionJSON := string(pbytes)

	_, err = p.pool.Exec(ctx,
		`INSERT INTO `+qrepTableName+
			`(job_name, partition_id, sync_partition, sync_start_time) VALUES ($1, $2, $3, $4)
			ON CONFLICT (job_name, partition_id) DO UPDATE SET sync_partition = $3, sync_start_time = $4, sync_finish_time = NOW()`,
		jobName, partition.PartitionId, partitionJSON, startTime)
	return err
}

func (p *PostgresMetadata) IsQRepPartitionSynced(ctx context.Context, req *protos.IsQRepPartitionSyncedInput) (bool, error) {
	var exists bool
	err := p.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT * FROM `+qrepTableName+
			` WHERE job_name = $1 AND partition_id = $2)`,
		req.FlowJobName, req.PartitionId).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}
	return exists, nil
}

func (p *PostgresMetadata) SyncFlowCleanup(ctx context.Context, jobName string) error {
	if _, err := p.pool.Exec(ctx,
		`DELETE FROM `+lastSyncStateTableName+` WHERE job_name = $1`, jobName,
	); err != nil {
		return err
	}

	if _, err := p.pool.Exec(ctx, `DELETE FROM `+qrepTableName+` WHERE job_name = $1`, jobName); err != nil {
		return err
	}

	return nil
}
