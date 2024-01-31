package connmetadata

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	cc "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	lastSyncStateTableName = "last_sync_state"
)

type PostgresMetadataStore struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	schemaName string
	logger     slog.Logger
}

func NewPostgresMetadataStore(ctx context.Context, schemaName string) (*PostgresMetadataStore, error) {
	pool, err := cc.GetCatalogConnectionPoolFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog connection pool: %w", err)
	}

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return &PostgresMetadataStore{
		ctx:        ctx,
		pool:       pool,
		schemaName: schemaName,
		logger:     *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
	}, nil
}

func (p *PostgresMetadataStore) QualifyTable(table string) string {
	return connpostgres.QuoteIdentifier(p.schemaName) + "." + connpostgres.QuoteIdentifier(table)
}

func (p *PostgresMetadataStore) Ping() error {
	pingErr := p.pool.Ping(p.ctx)
	if pingErr != nil {
		return fmt.Errorf("metadata db ping failed: %w", pingErr)
	}

	return nil
}

func (p *PostgresMetadataStore) NeedsSetupMetadata() bool {
	// check if schema exists
	row := p.pool.QueryRow(p.ctx, "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = $1", p.schemaName)

	var exists pgtype.Int8
	err := row.Scan(&exists)
	if err != nil {
		p.logger.Error("failed to check if schema exists", slog.Any("error", err))
		return false
	}

	if exists.Int64 > 0 {
		return true
	}

	return true
}

func (p *PostgresMetadataStore) SetupMetadata() error {
	// create the schema
	_, err := p.pool.Exec(p.ctx, "CREATE SCHEMA IF NOT EXISTS "+p.schemaName)
	if err != nil && !utils.IsUniqueError(err) {
		p.logger.Error("failed to create schema", slog.Any("error", err))
		return err
	}

	// create the last sync state table
	_, err = p.pool.Exec(p.ctx, `
		CREATE TABLE IF NOT EXISTS `+p.QualifyTable(lastSyncStateTableName)+` (
			job_name TEXT PRIMARY KEY NOT NULL,
			last_offset BIGINT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
			sync_batch_id BIGINT NOT NULL,
			normalize_batch_id BIGINT
		)
	`)
	if err != nil && !utils.IsUniqueError(err) {
		p.logger.Error("failed to create last sync state table", slog.Any("error", err))
		return err
	}

	p.logger.Info(fmt.Sprintf("created external metadata table %s.%s", p.schemaName, lastSyncStateTableName))
	return nil
}

func (p *PostgresMetadataStore) FetchLastOffset(jobName string) (int64, error) {
	row := p.pool.QueryRow(p.ctx, `
		SELECT last_offset
		FROM `+p.QualifyTable(lastSyncStateTableName)+`
		WHERE job_name = $1
	`, jobName)
	var offset pgtype.Int8
	err := row.Scan(&offset)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}

		p.logger.Error("failed to get last offset", slog.Any("error", err))
		return 0, err
	}

	p.logger.Info("got last offset for job", slog.Int64("offset", offset.Int64))

	return offset.Int64, nil
}

func (p *PostgresMetadataStore) GetLastBatchID(jobName string) (int64, error) {
	row := p.pool.QueryRow(p.ctx, `
		SELECT sync_batch_id
		FROM `+p.QualifyTable(lastSyncStateTableName)+`
		WHERE job_name = $1
	`, jobName)

	var syncBatchID pgtype.Int8
	err := row.Scan(&syncBatchID)
	if err != nil {
		// if the job doesn't exist, return 0
		if err == pgx.ErrNoRows {
			return 0, nil
		}

		p.logger.Error("failed to get last sync batch id", slog.Any("error", err))
		return 0, err
	}
	p.logger.Info("got last batch id for job", slog.Int64("batch id", syncBatchID.Int64))

	return syncBatchID.Int64, nil
}

func (p *PostgresMetadataStore) GetLastNormalizeBatchID(jobName string) (int64, error) {
	rows := p.pool.QueryRow(p.ctx, `
		SELECT normalize_batch_id
		FROM `+p.schemaName+`.`+lastSyncStateTableName+`
		WHERE job_name = $1
	`, jobName)

	var normalizeBatchID pgtype.Int8
	err := rows.Scan(&normalizeBatchID)
	if err != nil {
		// if the job doesn't exist, return 0
		if err.Error() == "no rows in result set" {
			return 0, nil
		}

		p.logger.Error("failed to get last normalize", slog.Any("error", err))
		return 0, err
	}
	p.logger.Info("got last normalize batch normalize id for job", slog.Int64("batch id", normalizeBatchID.Int64))

	return normalizeBatchID.Int64, nil
}

// update offset for a job
func (p *PostgresMetadataStore) UpdateLastOffset(jobName string, offset int64) error {
	p.logger.Info("updating last offset", slog.Int64("offset", offset))
	_, err := p.pool.Exec(p.ctx, `
		INSERT INTO `+p.QualifyTable(lastSyncStateTableName)+` (job_name, last_offset, sync_batch_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (job_name)
		DO UPDATE SET last_offset = GREATEST(`+connpostgres.QuoteIdentifier(lastSyncStateTableName)+`.last_offset, excluded.last_offset),
			updated_at = NOW()
	`, jobName, offset, 0)
	if err != nil {
		p.logger.Error("failed to update last offset", slog.Any("error", err))
		return err
	}

	return nil
}

func (p *PostgresMetadataStore) FinishBatch(jobName string, syncBatchID int64, offset int64) error {
	p.logger.Info("finishing batch", slog.Int64("SyncBatchID", syncBatchID), slog.Int64("offset", offset))
	_, err := p.pool.Exec(p.ctx, `
		INSERT INTO `+p.QualifyTable(lastSyncStateTableName)+` (job_name, last_offset, sync_batch_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (job_name)
		DO UPDATE SET
			last_offset = GREATEST(`+connpostgres.QuoteIdentifier(lastSyncStateTableName)+`.last_offset, excluded.last_offset),
			sync_batch_id = GREATEST(`+connpostgres.QuoteIdentifier(lastSyncStateTableName)+`.sync_batch_id, excluded.sync_batch_id),
			updated_at = NOW()
	`, jobName, offset, syncBatchID)
	if err != nil {
		p.logger.Error("failed to finish batch", slog.Any("error", err))
		return err
	}

	return nil
}

func (p *PostgresMetadataStore) UpdateNormalizeBatchID(jobName string, batchID int64) error {
	p.logger.Info("updating normalize batch id for job")
	_, err := p.pool.Exec(p.ctx, `
		UPDATE `+p.schemaName+`.`+lastSyncStateTableName+`
		 SET normalize_batch_id=$2 WHERE job_name=$1
	`, jobName, batchID)
	if err != nil {
		p.logger.Error("failed to update normalize batch id", slog.Any("error", err))
		return err
	}

	return nil
}

func (p *PostgresMetadataStore) DropMetadata(jobName string) error {
	_, err := p.pool.Exec(p.ctx, `
		DELETE FROM `+p.QualifyTable(lastSyncStateTableName)+`
		WHERE job_name = $1
	`, jobName)
	return err
}
