package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	checkIfTableExistsSQL = `SELECT exists(SELECT 1 FROM system.tables WHERE database = %s AND name = %s) AS table_exists`
	dropTableIfExistsSQL  = "DROP TABLE IF EXISTS %s%s"
)

// GetRawTableName returns the raw table name for the given table identifier.
func (c *ClickHouseConnector) GetRawTableName(flowJobName string) string {
	return "_peerdb_raw_" + shared.ReplaceIllegalCharactersWithUnderscores(flowJobName)
}

func (c *ClickHouseConnector) checkIfTableExists(ctx context.Context, databaseName string, tableIdentifier string) (bool, error) {
	var result uint8
	if err := c.queryRow(ctx,
		fmt.Sprintf(checkIfTableExistsSQL, peerdb_clickhouse.QuoteLiteral(databaseName), peerdb_clickhouse.QuoteLiteral(tableIdentifier)),
	).Scan(&result); err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}

	return result == 1, nil
}

func (c *ClickHouseConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableName := c.GetRawTableName(req.FlowJobName)
	onCluster := ""
	if c.config.Cluster != "" {
		onCluster = " ON CLUSTER " + peerdb_clickhouse.QuoteIdentifier(c.config.Cluster)
	}

	createRawTableSQL := `CREATE TABLE IF NOT EXISTS %s%s (
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
		fmt.Sprintf(createRawTableSQL, peerdb_clickhouse.QuoteIdentifier(rawTableName), onCluster))
	if err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}
	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

func (c *ClickHouseConnector) avroSyncMethod(flowJobName string, env map[string]string, version uint32) *ClickHouseAvroSyncMethod {
	qrepConfig := &protos.QRepConfig{
		StagingPath:                c.credsProvider.BucketPath,
		FlowJobName:                flowJobName,
		DestinationTableIdentifier: c.GetRawTableName(flowJobName),
		Env:                        env,
		Version:                    version,
	}
	return NewClickHouseAvroSyncMethod(qrepConfig, c)
}

func (c *ClickHouseConnector) syncRecordsViaAvro(
	ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	unboundedNumericAsString, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, req.Env)
	if err != nil {
		return nil, err
	}
	streamReq := model.NewRecordsToStreamRequest(
		req.Records.GetRecords(), tableNameRowsMapping, syncBatchID, unboundedNumericAsString,
		protos.DBType_CLICKHOUSE,
	)
	numericTruncator := model.NewStreamNumericTruncator(req.TableMappings, peerdb_clickhouse.NumericDestinationTypes)
	stream, err := utils.RecordsToRawTableStream(streamReq, numericTruncator)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	avroSyncer := c.avroSyncMethod(req.FlowJobName, req.Env, req.Version)
	numRecords, err := avroSyncer.SyncRecords(ctx, req.Env, stream, req.FlowJobName, syncBatchID)
	if err != nil {
		return nil, err
	}
	warnings := numericTruncator.Warnings()

	if err := c.ReplayTableSchemaDeltas(ctx, req.Env, req.FlowJobName, req.Records.SchemaDeltas); err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckpoint: req.Records.GetLastCheckpoint(),
		NumRecordsSynced:     numRecords,
		CurrentSyncBatchID:   syncBatchID,
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    req.Records.SchemaDeltas,
		Warnings:             warnings,
	}, nil
}

func (c *ClickHouseConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	res, err := c.syncRecordsViaAvro(ctx, req, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	if err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, res.LastSyncedCheckpoint); err != nil {
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

	onCluster := ""
	if c.config.Cluster != "" {
		onCluster = " ON CLUSTER " + peerdb_clickhouse.QuoteIdentifier(c.config.Cluster)
	}

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			qvKind := types.QValueKind(addedColumn.Type)
			clickHouseColType, err := qvalue.ToDWHColumnType(
				ctx, qvKind, env, protos.DBType_CLICKHOUSE, c.chVersion, addedColumn, schemaDelta.NullableEnabled,
			)
			if err != nil {
				return fmt.Errorf("failed to convert column type %s to ClickHouse type: %w", addedColumn.Type, err)
			}

			if c.config.Cluster != "" {
				if err := c.execWithLogging(ctx,
					fmt.Sprintf("ALTER TABLE %s%s ADD COLUMN IF NOT EXISTS %s %s",
						peerdb_clickhouse.QuoteIdentifier(schemaDelta.DstTableName+"_shard"), onCluster,
						peerdb_clickhouse.QuoteIdentifier(addedColumn.Name), clickHouseColType),
				); err != nil {
					return fmt.Errorf("failed to add column %s for table shards %s: %w", addedColumn.Name, schemaDelta.DstTableName, err)
				}
			}

			if err := c.execWithLogging(ctx,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
					peerdb_clickhouse.QuoteIdentifier(schemaDelta.DstTableName),
					peerdb_clickhouse.QuoteIdentifier(schemaDelta.DstTableName), onCluster,
					peerdb_clickhouse.QuoteIdentifier(addedColumn.Name), clickHouseColType),
			); err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.Name, schemaDelta.DstTableName, err)
			}
			c.logger.Info(
				"[schema delta replay] added column",
				slog.String("column", addedColumn.Name), slog.String("type", clickHouseColType),
				slog.String("destination table name", schemaDelta.DstTableName), slog.String("source table name", schemaDelta.SrcTableName),
			)
		}
	}

	return nil
}

func (c *ClickHouseConnector) RenameTables(
	ctx context.Context,
	req *protos.RenameTablesInput,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) (*protos.RenameTablesOutput, error) {
	onCluster := ""
	if c.config.Cluster != "" {
		onCluster = " ON CLUSTER " + peerdb_clickhouse.QuoteIdentifier(c.config.Cluster)
	}

	for _, renameRequest := range req.RenameTableOptions {
		if renameRequest.CurrentName == renameRequest.NewName {
			c.logger.Info("table rename is nop, probably Null table engine, skipping rename for it",
				slog.String("table", renameRequest.CurrentName))
			continue
		}

		resyncTableExists, err := c.checkIfTableExists(ctx, c.config.Database, renameRequest.CurrentName)
		if err != nil {
			return nil, fmt.Errorf("unable to check if resync table %s exists: %w", renameRequest.CurrentName, err)
		}

		if !resyncTableExists {
			c.logger.Info("table does not exist, skipping rename for it", slog.String("table", renameRequest.CurrentName))
			continue
		}

		originalTableExists, err := c.checkIfTableExists(ctx, c.config.Database, renameRequest.NewName)
		if err != nil {
			return nil, fmt.Errorf("unable to check if table %s exists: %w", renameRequest.NewName, err)
		}

		if originalTableExists {
			// target table exists, so we can attempt to swap. In most cases, we will have Atomic engine,
			// which supports a special query to exchange two tables, allowing dependent (materialized) views and dictionaries on these tables
			c.logger.Info("attempting atomic exchange",
				slog.String("OldName", renameRequest.CurrentName), slog.String("NewName", renameRequest.NewName))
			if err = c.execWithLogging(ctx,
				fmt.Sprintf("EXCHANGE TABLES %s and %s%s", peerdb_clickhouse.QuoteIdentifier(renameRequest.NewName),
					peerdb_clickhouse.QuoteIdentifier(renameRequest.CurrentName), onCluster),
			); err == nil {
				if err := c.execWithLogging(ctx,
					fmt.Sprintf(dropTableIfExistsSQL, peerdb_clickhouse.QuoteIdentifier(renameRequest.CurrentName), onCluster),
				); err != nil {
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
			if err := c.execWithLogging(ctx,
				fmt.Sprintf(dropTableIfExistsSQL, peerdb_clickhouse.QuoteIdentifier(renameRequest.NewName), onCluster),
			); err != nil {
				return nil, fmt.Errorf("unable to drop table %s: %w", renameRequest.NewName, err)
			}

			if err := c.execWithLogging(ctx, fmt.Sprintf("RENAME TABLE %s TO %s",
				peerdb_clickhouse.QuoteIdentifier(renameRequest.CurrentName),
				peerdb_clickhouse.QuoteIdentifier(renameRequest.NewName),
			)); err != nil {
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
	onCluster := ""
	if c.config.Cluster != "" {
		onCluster = " ON CLUSTER " + peerdb_clickhouse.QuoteIdentifier(c.config.Cluster)
	}

	// delete raw table if exists
	rawTableIdentifier := c.GetRawTableName(jobName)
	if err := c.execWithLogging(ctx,
		fmt.Sprintf(dropTableIfExistsSQL, peerdb_clickhouse.QuoteIdentifier(rawTableIdentifier), onCluster),
	); err != nil {
		return fmt.Errorf("[clickhouse] unable to drop raw table: %w", err)
	}
	c.logger.Info("successfully dropped raw table " + rawTableIdentifier)

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
		if err := c.execWithLogging(ctx, fmt.Sprintf("DELETE FROM `%s` WHERE _peerdb_destination_table_name = %s"+
			" AND _peerdb_batch_id > %d AND _peerdb_batch_id <= %d",
			c.GetRawTableName(req.FlowJobName), peerdb_clickhouse.QuoteLiteral(tableName), req.NormalizeBatchId, req.SyncBatchId),
		); err != nil {
			return fmt.Errorf("unable to remove table %s from raw table: %w", tableName, err)
		}

		c.logger.Info("successfully removed entries for table from raw table", slog.String("table", tableName))
	}

	return nil
}
