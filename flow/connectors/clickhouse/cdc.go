package connclickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	checkIfTableExistsSQL = `SELECT exists(SELECT 1 FROM system.tables WHERE database = ? AND name = ?) AS table_exists;`
	dropTableIfExistsSQL  = `DROP TABLE IF EXISTS %s;`
)

// getRawTableName returns the raw table name for the given table identifier.
func (c *ClickhouseConnector) getRawTableName(flowJobName string) string {
	return "_peerdb_raw_" + shared.ReplaceIllegalCharactersWithUnderscores(flowJobName)
}

func (c *ClickhouseConnector) checkIfTableExists(ctx context.Context, databaseName string, tableIdentifier string) (bool, error) {
	var result sql.NullInt32
	err := c.database.QueryRowContext(ctx, checkIfTableExistsSQL, databaseName, tableIdentifier).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}

	if !result.Valid {
		return false, errors.New("[clickhouse] checkIfTableExists: result is not valid")
	}

	return result.Int32 == 1, nil
}

func (c *ClickhouseConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

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

	_, err := c.execWithLogging(ctx,
		fmt.Sprintf(createRawTableSQL, rawTableName))
	if err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}
	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

func (c *ClickhouseConnector) avroSyncMethod(flowJobName string) *ClickhouseAvroSyncMethod {
	qrepConfig := &protos.QRepConfig{
		StagingPath:                c.credsProvider.BucketPath,
		FlowJobName:                flowJobName,
		DestinationTableIdentifier: c.getRawTableName(flowJobName),
	}
	return NewClickhouseAvroSyncMethod(qrepConfig, c)
}

func (c *ClickhouseConnector) syncRecordsViaAvro(
	ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, syncBatchID)
	stream, err := utils.RecordsToRawTableStream(streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	avroSyncer := c.avroSyncMethod(req.FlowJobName)
	numRecords, err := avroSyncer.SyncRecords(ctx, stream, req.FlowJobName, syncBatchID)
	if err != nil {
		return nil, err
	}

	err = c.ReplayTableSchemaDeltas(ctx, req.FlowJobName, req.Records.SchemaDeltas)
	if err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: req.Records.GetLastCheckpoint(),
		NumRecordsSynced:       int64(numRecords),
		CurrentSyncBatchID:     syncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func (c *ClickhouseConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	res, err := c.syncRecordsViaAvro(ctx, req, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	err = c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, res.LastSyncedCheckpointID)
	if err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	return res, nil
}

func (c *ClickhouseConnector) ReplayTableSchemaDeltas(ctx context.Context, flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	if len(schemaDeltas) == 0 {
		return nil
	}

	tableSchemaModifyTx, err := c.database.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction for schema modification: %w",
			err)
	}
	defer func() {
		deferErr := tableSchemaModifyTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Error("error rolling back transaction for table schema modification", "error", deferErr)
		}
	}()

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			clickhouseColType, err := qvalue.QValueKind(addedColumn.Type).ToDWHColumnType(protos.DBType_CLICKHOUSE)
			if err != nil {
				return fmt.Errorf("failed to convert column type %s to clickhouse type: %w",
					addedColumn.Type, err)
			}
			_, err = c.execWithLoggingTx(ctx,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS \"%s\" %s",
					schemaDelta.DstTableName, addedColumn.Name, clickhouseColType), tableSchemaModifyTx)
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.Name,
					schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s", addedColumn.Name,
				addedColumn.Type),
				"destination table name", schemaDelta.DstTableName,
				"source table name", schemaDelta.SrcTableName)
		}
	}

	err = tableSchemaModifyTx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction for table schema modification: %w",
			err)
	}

	return nil
}

func (c *ClickhouseConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	err := c.PostgresMetadata.SyncFlowCleanup(ctx, jobName)
	if err != nil {
		return fmt.Errorf("[clickhouse] unable to clear metadata for sync flow cleanup: %w", err)
	}

	// delete raw table if exists
	rawTableIdentifier := c.getRawTableName(jobName)
	_, err = c.execWithLogging(ctx, fmt.Sprintf(dropTableIfExistsSQL, rawTableIdentifier))
	if err != nil {
		return fmt.Errorf("[clickhouse] unable to drop raw table: %w", err)
	}

	return nil
}
