package connmetadata

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	cc "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

const (
	lastSyncStateTableName = "last_sync_state"
)

type PostgresMetadataStore struct {
	ctx        context.Context
	config     *protos.PostgresConfig
	pool       *pgxpool.Pool
	schemaName string
}

func NewPostgresMetadataStore(ctx context.Context, pgConfig *protos.PostgresConfig,
	schemaName string) (*PostgresMetadataStore, error) {
	var storePool *pgxpool.Pool
	var poolErr error
	if pgConfig == nil {
		storePool, poolErr = cc.GetCatalogConnectionPoolFromEnv()
		if poolErr != nil {
			return nil, fmt.Errorf("failed to create catalog connection pool: %v", poolErr)
		}

		log.Info("obtained catalog connection pool for metadata store")
	} else {
		connectionString := utils.GetPGConnectionString(pgConfig)
		storePool, poolErr = pgxpool.New(ctx, connectionString)
		if poolErr != nil {
			log.Errorf("failed to create connection pool: %v", poolErr)
			return nil, poolErr
		}

		log.Info("created connection pool for metadata store")
	}

	return &PostgresMetadataStore{
		ctx:        ctx,
		config:     pgConfig,
		pool:       storePool,
		schemaName: schemaName,
	}, nil
}

func (p *PostgresMetadataStore) Ping() bool {
	err := p.pool.Ping(p.ctx)
	if err != nil {
		log.Errorf("failed to connect to metadata db: %v", err)
		return false
	}

	return true
}

func (p *PostgresMetadataStore) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}

	return nil
}

func (p *PostgresMetadataStore) Ping() error {
	if p.pool == nil {
		return fmt.Errorf("metadata db ping failed as pool does not exist")
	}
	pingErr := p.pool.Ping(p.ctx)
	if pingErr != nil {
		return fmt.Errorf("metadata db ping failed: %w", pingErr)
	}

	return nil
}

func (p *PostgresMetadataStore) NeedsSetupMetadata() bool {
	// check if schema exists
	rows := p.pool.QueryRow(p.ctx, "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = $1", p.schemaName)

	var exists int64
	err := rows.Scan(&exists)
	if err != nil {
		log.Errorf("failed to check if schema exists: %v", err)
		return false
	}

	if exists > 0 {
		return true
	}

	return true
}

func (p *PostgresMetadataStore) SetupMetadata() error {
	// start a transaction
	tx, err := p.pool.Begin(p.ctx)
	if err != nil {
		log.Errorf("failed to start transaction: %v", err)
		return err
	}

	// create the schema
	_, err = tx.Exec(p.ctx, "CREATE SCHEMA IF NOT EXISTS "+p.schemaName)
	if err != nil {
		log.Errorf("failed to create schema: %v", err)
		return err
	}

	// create the last sync state table
	_, err = tx.Exec(p.ctx, `
		CREATE TABLE IF NOT EXISTS `+p.schemaName+`.`+lastSyncStateTableName+` (
			job_name TEXT PRIMARY KEY NOT NULL,
			last_offset BIGINT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
			sync_batch_id BIGINT NOT NULL
		)
	`)
	if err != nil {
		log.Errorf("failed to create last sync state table: %v", err)
		return err
	}
	log.Infof("created external metadata table %s.%s", p.schemaName, lastSyncStateTableName)

	// commit the transaction
	err = tx.Commit(p.ctx)
	if err != nil {
		log.Errorf("failed to commit transaction: %v", err)
		return err
	}

	return nil
}

func (p *PostgresMetadataStore) FetchLastOffset(jobName string) (*protos.LastSyncState, error) {
	rows := p.pool.QueryRow(p.ctx, `
		SELECT last_offset
		FROM `+p.schemaName+`.`+lastSyncStateTableName+`
		WHERE job_name = $1
	`, jobName)
	var offset int64
	err := rows.Scan(&offset)
	if err != nil {
		// if the job doesn't exist, return 0
		if err.Error() == "no rows in result set" {
			return &protos.LastSyncState{
				Checkpoint: 0,
			}, nil
		}

		log.WithFields(log.Fields{
			"flowName": jobName,
		}).Errorf("failed to get last offset: %v", err)
		return nil, err
	}

	log.Infof("got last offset for job `%s`: %d", jobName, offset)

	return &protos.LastSyncState{
		Checkpoint: offset,
	}, nil
}

func (p *PostgresMetadataStore) GetLastBatchID(jobName string) (int64, error) {
	rows := p.pool.QueryRow(p.ctx, `
		SELECT sync_batch_id
		FROM `+p.schemaName+`.`+lastSyncStateTableName+`
		WHERE job_name = $1
	`, jobName)

	var syncBatchID int64
	err := rows.Scan(&syncBatchID)
	if err != nil {
		// if the job doesn't exist, return 0
		if err.Error() == "no rows in result set" {
			return 0, nil
		}

		log.WithFields(log.Fields{
			"flowName": jobName,
		}).Errorf("failed to get last offset: %v", err)
		return 0, err
	}

	log.Infof("got last sync batch ID for job `%s`: %d", jobName, syncBatchID)

	return syncBatchID, nil
}

// update offset for a job
func (p *PostgresMetadataStore) UpdateLastOffset(jobName string, offset int64) error {
	// start a transaction
	tx, err := p.pool.Begin(p.ctx)
	if err != nil {
		log.Errorf("failed to start transaction: %v", err)
		return err
	}

	// update the last offset
	log.WithFields(log.Fields{
		"flowName": jobName,
	}).Infof("updating last offset for job `%s` to `%d`", jobName, offset)
	_, err = tx.Exec(p.ctx, `
		INSERT INTO `+p.schemaName+`.`+lastSyncStateTableName+` (job_name, last_offset, sync_batch_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (job_name)
		DO UPDATE SET last_offset = $2, updated_at = NOW()
	`, jobName, offset, 0)

	if err != nil {
		log.WithFields(log.Fields{
			"flowName": jobName,
		}).Errorf("failed to update last offset: %v", err)
		return err
	}

	// commit the transaction
	err = tx.Commit(p.ctx)
	if err != nil {
		log.Errorf("failed to commit transaction: %v", err)
		return err
	}

	return nil
}

// update offset for a job
func (p *PostgresMetadataStore) IncrementID(jobName string) error {
	log.WithFields(log.Fields{
		"flowName": jobName,
	}).Infof("incrementing sync batch id for job `%s`", jobName)
	_, err := p.pool.Exec(p.ctx, `
		UPDATE `+p.schemaName+`.`+lastSyncStateTableName+`
		 SET sync_batch_id=sync_batch_id+1 WHERE job_name=$1
	`, jobName)

	if err != nil {
		log.WithFields(log.Fields{
			"flowName": jobName,
		}).Errorf("failed to increment sync batch id: %v", err)
		return err
	}

	return nil
}

func (p *PostgresMetadataStore) DropMetadata(jobName string) error {
	_, err := p.pool.Exec(p.ctx, `
		DELETE FROM `+p.schemaName+`.`+lastSyncStateTableName+`
		WHERE job_name = $1
	`, jobName)
	return err
}
