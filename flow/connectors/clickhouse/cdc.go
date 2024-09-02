package connclickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

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
	err := c.database.QueryRow(ctx, checkIfTableExistsSQL, databaseName, tableIdentifier).Scan(&result)
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

	err := c.execWithLogging(ctx,
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
			err = c.execWithLogging(ctx,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS \"%s\" %s",
					schemaDelta.DstTableName, addedColumn.Name, clickhouseColType))
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

	return nil
}

func (c *ClickhouseConnector) RenameTables(ctx context.Context, req *protos.RenameTablesInput) (*protos.RenameTablesOutput, error) {
	for _, renameRequest := range req.RenameTableOptions {
		resyncTableExists, err := c.checkIfTableExists(ctx, c.config.Database, renameRequest.CurrentName)
		if err != nil {
			return nil, fmt.Errorf("unable to check if resync table %s exists: %w", renameRequest.CurrentName, err)
		}

		if !resyncTableExists {
			c.logger.Info(fmt.Sprintf("table '%s' does not exist, skipping rename for it", renameRequest.CurrentName))
			continue
		}

		originalTableExists, err := c.checkIfTableExists(ctx, c.config.Database, renameRequest.NewName)
		if err != nil {
			return nil, fmt.Errorf("unable to check if table %s exists: %w", renameRequest.NewName, err)
		}

		if originalTableExists {
			columnNames := make([]string, 0, len(renameRequest.TableSchema.Columns))
			for _, col := range renameRequest.TableSchema.Columns {
				columnNames = append(columnNames, col.Name)
			}

			allCols := strings.Join(columnNames, ",")
			c.logger.Info(fmt.Sprintf("handling soft-deletes for table '%s'...", renameRequest.NewName))
			err = c.execWithLogging(ctx,
				fmt.Sprintf("INSERT INTO %s(%s,%s) SELECT %s,true FROM %s WHERE %s  = 1",
					renameRequest.CurrentName, allCols, signColName, allCols, renameRequest.NewName, signColName))
			if err != nil {
				return nil, fmt.Errorf("unable to handle soft-deletes for table %s: %w", renameRequest.NewName, err)
			}
		} else {
			c.logger.Info(fmt.Sprintf("table '%s' does not exist, skipping soft-deletes transfer for it", renameRequest.NewName))
		}

		// drop the dst table if exists
		err = c.execWithLogging(ctx, "DROP TABLE IF EXISTS "+renameRequest.NewName)
		if err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", renameRequest.NewName, err)
		}

		// rename the src table to dst
		err = c.execWithLogging(ctx, fmt.Sprintf("RENAME TABLE %s TO %s",
			renameRequest.CurrentName,
			renameRequest.NewName))
		if err != nil {
			return nil, fmt.Errorf("unable to rename table %s to %s: %w",
				renameRequest.CurrentName, renameRequest.NewName, err)
		}

		c.logger.Info(fmt.Sprintf("successfully renamed table '%s' to '%s'",
			renameRequest.CurrentName, renameRequest.NewName))
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *ClickhouseConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	err := c.PostgresMetadata.SyncFlowCleanup(ctx, jobName)
	if err != nil {
		return fmt.Errorf("[clickhouse] unable to clear metadata for sync flow cleanup: %w", err)
	}

	// delete raw table if exists
	rawTableIdentifier := c.getRawTableName(jobName)
	err = c.execWithLogging(ctx, fmt.Sprintf(dropTableIfExistsSQL, rawTableIdentifier))
	if err != nil {
		return fmt.Errorf("[clickhouse] unable to drop raw table: %w", err)
	}

	return nil
}

func (c *ClickhouseConnector) RemoveTableEntriesFromRawTable(
	ctx context.Context,
	req *protos.RemoveTablesFromRawTableInput,
) error {
	for _, tableName := range req.DestinationTableNames {
		// Better to use lightweight deletes here as the main goal is to
		// not have the rows in the table be visible by the NormalizeRecords'
		// INSERT INTO SELECT queries
		err := c.execWithLogging(ctx, fmt.Sprintf("DELETE FROM `%s` WHERE _peerdb_destination_table_name = '%s'"+
			" AND _peerdb_batch_id BETWEEN %d AND %d",
			c.getRawTableName(req.FlowJobName), tableName, req.NormalizeBatchId, req.SyncBatchId))
		if err != nil {
			return fmt.Errorf("unable to remove table %s from raw table: %w", tableName, err)
		}

		c.logger.Info(fmt.Sprintf("successfully removed entries for table '%s' from raw table", tableName))
	}

	return nil
}
