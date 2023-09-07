package conneventhub

import (
	"context"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

const (
	// schema for the peerdb metadata
	metadataSchema = "peerdb_eventhub_metadata"
	// The name of the table that stores the last sync state.
	lastSyncStateTableName = "last_sync_state"
)

type PostgresMetadataStore struct {
	config *protos.PostgresConfig
	pool   *pgxpool.Pool
}

func NewPostgresMetadataStore(ctx context.Context, pgConfig *protos.PostgresConfig) (*PostgresMetadataStore, error) {
	connectionString := utils.GetPGConnectionString(pgConfig)

	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		log.Errorf("failed to create connection pool: %v", err)
		return nil, err
	}
	log.Info("created connection pool for eventhub metadata store")

	return &PostgresMetadataStore{
		config: pgConfig,
		pool:   pool,
	}, nil
}

func (p *PostgresMetadataStore) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}

	return nil
}

func (c *EventHubConnector) NeedsSetupMetadataTables() bool {
	ms := c.pgMetadata

	// check if schema exists
	rows := ms.pool.QueryRow(c.ctx, "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = $1", metadataSchema)

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

func (c *EventHubConnector) SetupMetadataTables() error {
	ms := c.pgMetadata

	// start a transaction
	tx, err := ms.pool.Begin(c.ctx)
	if err != nil {
		log.Errorf("failed to start transaction: %v", err)
		return err
	}

	// create the schema
	_, err = tx.Exec(c.ctx, "CREATE SCHEMA IF NOT EXISTS "+metadataSchema)
	if err != nil {
		log.Errorf("failed to create schema: %v", err)
		return err
	}

	// create the last sync state table
	_, err = tx.Exec(c.ctx, `
		CREATE TABLE IF NOT EXISTS `+metadataSchema+`.`+lastSyncStateTableName+` (
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

	// commit the transaction
	err = tx.Commit(c.ctx)
	if err != nil {
		log.Errorf("failed to commit transaction: %v", err)
		return err
	}

	return nil
}

func (c *EventHubConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	ms := c.pgMetadata

	rows := ms.pool.QueryRow(c.ctx, `
		SELECT last_offset
		FROM `+metadataSchema+`.`+lastSyncStateTableName+`
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

func (c *EventHubConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	ms := c.pgMetadata

	rows := ms.pool.QueryRow(c.ctx, `
		SELECT sync_batch_id
		FROM `+metadataSchema+`.`+lastSyncStateTableName+`
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
func (c *EventHubConnector) updateLastOffset(jobName string, offset int64) error {
	ms := c.pgMetadata

	// start a transaction
	tx, err := ms.pool.Begin(c.ctx)
	if err != nil {
		log.Errorf("failed to start transaction: %v", err)
		return err
	}

	// update the last offset
	log.WithFields(log.Fields{
		"flowName": jobName,
	}).Infof("updating last offset for job `%s` to `%d`", jobName, offset)
	_, err = tx.Exec(c.ctx, `
		INSERT INTO `+metadataSchema+`.`+lastSyncStateTableName+` (job_name, last_offset, sync_batch_id)
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
	err = tx.Commit(c.ctx)
	if err != nil {
		log.Errorf("failed to commit transaction: %v", err)
		return err
	}

	return nil
}

// update offset for a job
func (c *EventHubConnector) incrementSyncBatchID(jobName string) error {
	ms := c.pgMetadata

	log.WithFields(log.Fields{
		"flowName": jobName,
	}).Infof("incrementing sync batch id for job `%s`", jobName)
	_, err := ms.pool.Exec(c.ctx, `
		UPDATE `+metadataSchema+`.`+lastSyncStateTableName+`
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
