package connclickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	checkIfTableExistsSQL = `SELECT exists(SELECT 1 FROM system.tables WHERE database = ? AND name = ?) AS table_exists;`
	dropTableIfExistsSQL  = "DROP TABLE IF EXISTS `%s`;"
)

// getRawTableName returns the raw table name for the given table identifier.
func (c *ClickHouseConnector) getRawTableName(flowJobName string) string {
	return "_peerdb_raw_" + shared.ReplaceIllegalCharactersWithUnderscores(flowJobName)
}

func (c *ClickHouseConnector) checkIfTableExists(ctx context.Context, databaseName string, tableIdentifier string) (bool, error) {
	var result sql.NullInt32
	err := c.queryRow(ctx, checkIfTableExistsSQL, databaseName, tableIdentifier).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}

	if !result.Valid {
		return false, errors.New("[clickhouse] checkIfTableExists: result is not valid")
	}

	return result.Int32 == 1, nil
}

func (c *ClickHouseConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	createRawTableSQL := `CREATE TABLE IF NOT EXISTS %s (
		_peerdb_uid UUID,
		_peerdb_timestamp Int64,
		_peerdb_destination_table_name String,
		_peerdb_data String,
		_peerdb_record_type Int,
		_peerdb_match_data String,
		_peerdb_batch_id Int64,
		_peerdb_unchanged_toast_columns String
	) ENGINE = MergeTree() ORDER BY (_peerdb_batch_id, _peerdb_destination_table_name);`

	err := c.execWithLogging(ctx,
		fmt.Sprintf(createRawTableSQL, rawTableName))
	if err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}
	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

func (c *ClickHouseConnector) avroSyncMethod(flowJobName string) *ClickHouseAvroSyncMethod {
	qrepConfig := &protos.QRepConfig{
		StagingPath:                c.credsProvider.BucketPath,
		FlowJobName:                flowJobName,
		DestinationTableIdentifier: c.getRawTableName(flowJobName),
	}
	return NewClickHouseAvroSyncMethod(qrepConfig, c)
}

func (c *ClickHouseConnector) syncRecordsViaAvro(
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
	numRecords, err := avroSyncer.SyncRecords(ctx, req.Env, stream, req.FlowJobName, syncBatchID)
	if err != nil {
		return nil, err
	}

	if err := c.ReplayTableSchemaDeltas(ctx, req.Env, req.FlowJobName, req.Records.SchemaDeltas); err != nil {
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

func (c *ClickHouseConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	res, err := c.syncRecordsViaAvro(ctx, req, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	if err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, res.LastSyncedCheckpointID); err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	return res, nil
}

func (c *ClickHouseConnector) ReplayTableSchemaDeltas(
	ctx context.Context,
	env map[string]string,
	flowJobName string,
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
			clickHouseColType, err := qvalue.QValueKind(addedColumn.Type).ToDWHColumnType(ctx, env, protos.DBType_CLICKHOUSE, addedColumn)
			if err != nil {
				return fmt.Errorf("failed to convert column type %s to ClickHouse type: %w", addedColumn.Type, err)
			}
			err = c.execWithLogging(ctx,
				fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `%s` %s",
					schemaDelta.DstTableName, addedColumn.Name, clickHouseColType))
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.Name, schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s", addedColumn.Name,
				addedColumn.Type),
				"destination table name", schemaDelta.DstTableName,
				"source table name", schemaDelta.SrcTableName)
		}
	}

	return nil
}

func (c *ClickHouseConnector) RenameTables(
	ctx context.Context,
	req *protos.RenameTablesInput,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) (*protos.RenameTablesOutput, error) {
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
			tableSchema := tableNameSchemaMapping[renameRequest.CurrentName]
			columnNames := make([]string, 0, len(tableSchema.Columns))
			for _, col := range tableSchema.Columns {
				columnNames = append(columnNames, col.Name)
			}

			allCols := strings.Join(columnNames, ",")
			c.logger.Info("handling soft-deletes for table before rename", slog.String("NewName", renameRequest.NewName))
			if err := c.execWithLogging(ctx,
				fmt.Sprintf("INSERT INTO `%s`(%s,%s) SELECT %s,true FROM `%s` WHERE %s = 1",
					renameRequest.CurrentName, allCols, signColName, allCols, renameRequest.NewName, signColName),
			); err != nil {
				return nil, fmt.Errorf("unable to handle soft-deletes for table %s: %w", renameRequest.NewName, err)
			}

			// target table exists, so we can attempt to swap. In most cases, we will have Atomic engine,
			// which supports a special query to exchange two tables, allowing dependent (materialized) views and dictionaries on these tables
			c.logger.Info("attempting atomic exchange",
				slog.String("OldName", renameRequest.CurrentName), slog.String("NewName", renameRequest.NewName))
			if err = c.execWithLogging(ctx,
				fmt.Sprintf("EXCHANGE TABLES `%s` and `%s`", renameRequest.NewName, renameRequest.CurrentName),
			); err == nil {
				if err := c.execWithLogging(ctx, fmt.Sprintf(dropTableIfExistsSQL, renameRequest.CurrentName)); err != nil {
					return nil, fmt.Errorf("unable to drop exchanged table %s: %w", renameRequest.CurrentName, err)
				}
			} else if ex, ok := err.(*clickhouse.Exception); !ok || ex.Code != 48 {
				// code 48 == not implemented -> move on to the fallback code, in all other error codes / types
				// return, since we know/assume that the exchange would be the sensible action
				return nil, fmt.Errorf("unable to exchange tables %s and %s: %w", renameRequest.NewName, renameRequest.CurrentName, err)
			}
		}

		// either original table doesn't exist, in which case it is safe to just run rename,
		// or err is set (in which case err comes from EXCHANGE TABLES)
		if !originalTableExists || err != nil {
			if err := c.execWithLogging(ctx, fmt.Sprintf(dropTableIfExistsSQL, renameRequest.NewName)); err != nil {
				return nil, fmt.Errorf("unable to drop table %s: %w", renameRequest.NewName, err)
			}

			if err := c.execWithLogging(ctx,
				fmt.Sprintf("RENAME TABLE `%s` TO `%s`", renameRequest.CurrentName, renameRequest.NewName),
			); err != nil {
				return nil, fmt.Errorf("unable to rename table %s to %s: %w", renameRequest.CurrentName, renameRequest.NewName, err)
			}
		}

		c.logger.Info("successfully renamed table",
			slog.String("OldName", renameRequest.CurrentName), slog.String("NewName", renameRequest.NewName))
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *ClickHouseConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	if err := c.PostgresMetadata.SyncFlowCleanup(ctx, jobName); err != nil {
		return fmt.Errorf("[clickhouse] unable to clear metadata for sync flow cleanup: %w", err)
	}

	// delete raw table if exists
	rawTableIdentifier := c.getRawTableName(jobName)
	if err := c.execWithLogging(ctx, fmt.Sprintf(dropTableIfExistsSQL, rawTableIdentifier)); err != nil {
		return fmt.Errorf("[clickhouse] unable to drop raw table: %w", err)
	}

	return nil
}

func (c *ClickHouseConnector) RemoveTableEntriesFromRawTable(
	ctx context.Context,
	req *protos.RemoveTablesFromRawTableInput,
) error {
	for _, tableName := range req.DestinationTableNames {
		// Better to use lightweight deletes here as the main goal is to
		// not have the rows in the table be visible by the NormalizeRecords'
		// INSERT INTO SELECT queries
		err := c.execWithLogging(ctx, fmt.Sprintf("DELETE FROM `%s` WHERE _peerdb_destination_table_name = '%s'"+
			" AND _peerdb_batch_id > %d AND _peerdb_batch_id <= %d",
			c.getRawTableName(req.FlowJobName), tableName, req.NormalizeBatchId, req.SyncBatchId))
		if err != nil {
			return fmt.Errorf("unable to remove table %s from raw table: %w", tableName, err)
		}

		c.logger.Info(fmt.Sprintf("successfully removed entries for table '%s' from raw table", tableName))
	}

	return nil
}
