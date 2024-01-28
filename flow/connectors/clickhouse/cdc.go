package connclickhouse

import (
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	checkIfTableExistsSQL     = `SELECT exists(SELECT 1 FROM system.tables WHERE database = ? AND name = ?) AS table_exists;`
	mirrorJobsTableIdentifier = "PEERDB_MIRROR_JOBS"
)

// getRawTableName returns the raw table name for the given table identifier.
func (c *ClickhouseConnector) getRawTableName(flowJobName string) string {
	// replace all non-alphanumeric characters with _
	flowJobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(flowJobName, "_")
	return fmt.Sprintf("_peerdb_raw_%s", flowJobName)
}

func (c *ClickhouseConnector) checkIfTableExists(databaseName string, tableIdentifier string) (bool, error) {
	var result sql.NullInt32
	err := c.database.QueryRowContext(c.ctx, checkIfTableExistsSQL, databaseName, tableIdentifier).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}

	if !result.Valid {
		return false, fmt.Errorf("[clickhouse] checkIfTableExists: result is not valid")
	}

	return result.Int32 == 1, nil
}

type MirrorJobRow struct {
	MirrorJobName    string
	Offset           int
	SyncBatchID      int
	NormalizeBatchID int
}

// func (c *ClickhouseConnector) getMirrorRowByJobNAme(jobName string) (*MirrorJobRow, error) {
// 	getLastOffsetSQL := "SELECT mirror_job_name, offset, sync_batch_id, normalize_batch_id FROM %s WHERE MIRROR_JOB_NAME=? Limit 1"

// 	row := c.database.QueryRowContext(c.ctx, fmt.Sprintf(getLastOffsetSQL, mirrorJobsTableIdentifier), jobName)

// 	var result MirrorJobRow

// 	err := row.Scan(
// 		&result.MirrorJobName,
// 		&result.Offset,
// 		&result.SyncBatchID,
// 		&result.NormalizeBatchID,
// 	)

// 	if err != nil {
// 		return nil, err
// 	}

// 	return &result, nil
// }

func (c *ClickhouseConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	// createRawTableTx, err := c.database.BeginTx(c.ctx, nil)
	// if err != nil {
	// 	return nil, fmt.Errorf("unable to begin transaction for creation of raw table: %w", err)
	// }

	createRawTableSQL := `CREATE TABLE IF NOT EXISTS %s (
		_peerdb_uid String NOT NULL,
		_peerdb_timestamp Int64 NOT NULL,
		_peerdb_destination_table_name String NOT NULL,
		_peerdb_data String NOT NULL,
		_peerdb_record_type Int NOT NULL,
		_peerdb_match_data String,
		_peerdb_batch_id Int,
		_peerdb_unchanged_toast_columns String
	) ENGINE = ReplacingMergeTree ORDER BY _peerdb_uid;`

	_, err := c.database.ExecContext(c.ctx,
		fmt.Sprintf(createRawTableSQL, rawTableName))
	if err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}
	// err = createRawTableTx.Commit()
	// if err != nil {
	// 	return nil, fmt.Errorf("unable to commit transaction for creation of raw table: %w", err)
	// }

	// stage := c.getStageNameForJob(req.FlowJobName)
	// err = c.createStage(stage, &protos.QRepConfig{})
	// if err != nil {
	// 	return nil, err
	// }

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

func (c *ClickhouseConnector) syncRecordsViaAvro(
	req *model.SyncRecordsRequest,
	rawTableIdentifier string,
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := make(map[string]uint32)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, syncBatchID)
	streamRes, err := utils.RecordsToRawTableStream(streamReq)
	//x := *&streamRes.Stream
	//y := (*x).Records
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	qrepConfig := &protos.QRepConfig{
		StagingPath:                c.config.S3Integration,
		FlowJobName:                req.FlowJobName,
		DestinationTableIdentifier: strings.ToLower(fmt.Sprintf("%s", rawTableIdentifier)),
	}
	avroSyncer := NewClickhouseAvroSyncMethod(qrepConfig, c)
	destinationTableSchema, err := c.getTableSchema(qrepConfig.DestinationTableIdentifier)
	if err != nil {
		return nil, err
	}

	numRecords, err := avroSyncer.SyncRecords(destinationTableSchema, streamRes.Stream, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	//tableSchemaDeltas := req.Records.WaitForSchemaDeltas(req.TableMappings)
	err = c.ReplayTableSchemaDeltas(req.FlowJobName, req.Records.SchemaDeltas)
	if err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       int64(numRecords),
		CurrentSyncBatchID:     syncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
		//RelationMessageMapping: <-req.Records.RelationMessageMapping,
	}, nil
}

func (c *ClickhouseConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	//c.config.S3Integration = "s3://avro-clickhouse"
	rawTableName := c.getRawTableName(req.FlowJobName)
	c.logger.Info(fmt.Sprintf("pushing records to Clickhouse table %s", rawTableName))

	// syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to get previous syncBatchID: %w", err)
	// }
	// syncBatchID += 1

	res, err := c.syncRecordsViaAvro(req, rawTableName, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	// transaction for SyncRecords
	// syncRecordsTx, err := c.database.BeginTx(c.ctx, nil)
	// if err != nil {
	// 	return nil, err
	// }
	// in case we return after error, ensure transaction is rolled back
	// defer func() {
	// 	deferErr := syncRecordsTx.Rollback()
	// 	if deferErr != sql.ErrTxDone && deferErr != nil {
	// 		c.logger.Error("error while rolling back transaction for SyncRecords: %v",
	// 			slog.Any("error", deferErr), slog.Int64("syncBatchID", syncBatchID))
	// 	}
	// }()

	// updating metadata with new offset and syncBatchID
	// err = c.updateSyncMetadata(req.FlowJobName, res.LastSyncedCheckPointID, syncBatchID, syncRecordsTx)
	// if err != nil {
	// 	return nil, err
	// }
	// transaction commits
	// err = syncRecordsTx.Commit()
	// if err != nil {
	// 	return nil, err
	// }

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get last checkpoint: %w", err)
	}

	err = c.SetLastOffset(req.FlowJobName, lastCheckpoint)
	if err != nil {
		c.logger.Error("failed to update last offset for s3 cdc", slog.Any("error", err))
		return nil, err
	}
	err = c.pgMetadata.IncrementID(req.FlowJobName)
	if err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	return res, nil
}

func (c *ClickhouseConnector) jobMetadataExistsTx(tx *sql.Tx, jobName string) (bool, error) {
	checkIfJobMetadataExistsSQL := "SELECT TO_BOOLEAN(COUNT(1)) FROM %s WHERE MIRROR_JOB_NAME=?"

	var result pgtype.Bool
	err := tx.QueryRowContext(c.ctx,
		fmt.Sprintf(checkIfJobMetadataExistsSQL, mirrorJobsTableIdentifier), jobName).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error reading result row: %w", err)
	}
	return result.Bool, nil
}

// func (c *ClickhouseConnector) updateSyncMetadata(flowJobName string, lastCP int64,
// 	syncBatchID int64, syncRecordsTx *sql.Tx,
// ) error {
// 	jobMetadataExists, err := c.jobMetadataExistsTx(syncRecordsTx, flowJobName)
// 	if err != nil {
// 		return fmt.Errorf("failed to get sync status for flow job: %w", err)
// 	}

// 	if !jobMetadataExists {
// 		_, err := syncRecordsTx.ExecContext(c.ctx,
// 			fmt.Sprintf(insertJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier),
// 			flowJobName, lastCP, syncBatchID, 0)
// 		if err != nil {
// 			return fmt.Errorf("failed to insert flow job status: %w", err)
// 		}
// 	} else {
// 		_, err := syncRecordsTx.ExecContext(c.ctx,
// 			fmt.Sprintf(updateMetadataForSyncRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier),
// 			lastCP, syncBatchID, flowJobName)
// 		if err != nil {
// 			return fmt.Errorf("failed to update flow job status: %w", err)
// 		}
// 	}

// 	return nil
// }

func (c *ClickhouseConnector) SyncFlowCleanup(jobName string) error {
	// syncFlowCleanupTx, err := c.database.BeginTx(c.ctx, nil)
	// if err != nil {
	// 	return fmt.Errorf("unable to begin transaction for sync flow cleanup: %w", err)
	// }
	// defer func() {
	// 	deferErr := syncFlowCleanupTx.Rollback()
	// 	if deferErr != sql.ErrTxDone && deferErr != nil {
	// 		c.logger.Error("error while rolling back transaction for flow cleanup", slog.Any("error", deferErr))
	// 	}
	// }()

	// row := syncFlowCleanupTx.QueryRowContext(c.ctx, checkSchemaExistsSQL, c.metadataSchema)
	// var schemaExists pgtype.Bool
	// err = row.Scan(&schemaExists)
	// if err != nil {
	// 	return fmt.Errorf("unable to check if internal schema exists: %w", err)
	// }

	// if schemaExists.Bool {
	// 	_, err = syncFlowCleanupTx.ExecContext(c.ctx, fmt.Sprintf(dropTableIfExistsSQL, c.metadataSchema,
	// 		getRawTableIdentifier(jobName)))
	// 	if err != nil {
	// 		return fmt.Errorf("unable to drop raw table: %w", err)
	// 	}
	// 	_, err = syncFlowCleanupTx.ExecContext(c.ctx,
	// 		fmt.Sprintf(deleteJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName)
	// 	if err != nil {
	// 		return fmt.Errorf("unable to delete job metadata: %w", err)
	// 	}
	// }

	// err = syncFlowCleanupTx.Commit()
	// if err != nil {
	// 	return fmt.Errorf("unable to commit transaction for sync flow cleanup: %w", err)
	// }

	// err = c.dropStage("", jobName)
	// if err != nil {
	// 	return err
	// }
	err := c.pgMetadata.DropMetadata(jobName)
	if err != nil {
		return err
	}
	return nil
}

// ReplayTableSchemaDeltas changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *ClickhouseConnector) ReplayTableSchemaDeltas(flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	return nil
}

// external
func (c *ClickhouseConnector) NeedsSetupMetadataTables() bool {
	return c.pgMetadata.NeedsSetupMetadata()
}

func (c *ClickhouseConnector) SetupMetadataTables() error {
	err := c.pgMetadata.SetupMetadata()
	if err != nil {
		c.logger.Error("failed to setup metadata tables", slog.Any("error", err))
		return err
	}

	return nil
}

// func (c *ClickhouseConnector) SetupNormalizedTables(
// 	req *protos.SetupNormalizedTableBatchInput,
// ) (*protos.SetupNormalizedTableBatchOutput, error) {
// 	return nil, nil
// }

func (c *ClickhouseConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	return c.pgMetadata.GetLastBatchID(jobName)
}

func (c *ClickhouseConnector) GetLastOffset(jobName string) (int64, error) {
	return c.pgMetadata.FetchLastOffset(jobName)
}

// update offset for a job
func (c *ClickhouseConnector) SetLastOffset(jobName string, offset int64) error {
	err := c.pgMetadata.UpdateLastOffset(jobName, offset)
	if err != nil {
		c.logger.Error("failed to update last offset: ", slog.Any("error", err))
		return err
	}

	return nil
}

// func (c *ClickhouseConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
// 	return &model.NormalizeResponse{
// 		Done:         true,
// 		StartBatchID: 1,
// 		EndBatchID:   1,
// 	}, nil
// }
