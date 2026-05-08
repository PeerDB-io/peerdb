package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	numeric "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
)

// NeedsSetupMetadataTables returns true if the metadata tables need to be set up.
func (c *PostgresConnector) NeedsSetupMetadataTables(ctx context.Context) (bool, error) {
	result, err := c.tableExists(ctx, &common.QualifiedTable{
		Namespace: c.metadataSchema,
		Table:     mirrorJobsTableIdentifier,
	})
	return !result, err
}

// SetupMetadataTables sets up the metadata tables.
func (c *PostgresConnector) SetupMetadataTables(ctx context.Context) error {
	if err := c.createMetadataSchema(ctx); err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx,
		fmt.Sprintf(createMirrorJobsTableSQL, c.metadataSchema, mirrorJobsTableIdentifier),
	); err != nil && !shared.IsSQLStateError(err, pgerrcode.UniqueViolation, pgerrcode.DuplicateObject) {
		return fmt.Errorf("error creating table %s: %w", mirrorJobsTableIdentifier, err)
	}

	return nil
}

// GetLastOffset returns the last synced offset for a job.
func (c *PostgresConnector) GetLastOffset(ctx context.Context, jobName string) (model.CdcCheckpoint, error) {
	var result model.CdcCheckpoint
	if err := c.conn.QueryRow(
		ctx, fmt.Sprintf(getLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName,
	).Scan(&result.ID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.logger.Info("No row found, returning nil")
			return result, nil
		}
		return result, fmt.Errorf("error while reading result row: %w", err)
	}

	if result.ID == 0 {
		c.logger.Warn("Assuming zero offset means no sync has happened")
	}
	return result, nil
}

// SetLastOffset updates the last synced offset for a job.
func (c *PostgresConnector) SetLastOffset(ctx context.Context, jobName string, lastOffset model.CdcCheckpoint) error {
	if _, err := c.conn.Exec(ctx,
		fmt.Sprintf(setLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		lastOffset.ID, jobName,
	); err != nil {
		return fmt.Errorf("error setting last offset for job %s: %w", jobName, err)
	}

	return nil
}

func (c *PostgresConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	return syncRecordsCore(ctx, c, req)
}

func (c *PostgresConnector) SyncPg(ctx context.Context, req *model.SyncRecordsRequest[model.PgItems]) (*model.SyncResponse, error) {
	return syncRecordsCore(ctx, c, req)
}

// syncRecordsCore pushes records to the destination.
func syncRecordsCore[Items model.Items](
	ctx context.Context,
	c *PostgresConnector,
	req *model.SyncRecordsRequest[Items],
) (*model.SyncResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	c.logger.Info(fmt.Sprintf("pushing records to Postgres table %s via COPY", rawTableIdentifier))

	numRecords := int64(0)
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	streamReadFunc := func() ([]any, error) {
		for record := range req.Records.GetRecords() {
			var row []any
			switch typedRecord := record.(type) {
			case *model.InsertRecord[Items]:
				itemsJSON, err := typedRecord.Items.ToJSONWithOptions(model.ToJSONOptions{
					UnnestColumns: nil,
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
				}

				row = []any{
					uuid.New(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					itemsJSON,
					0,
					"{}",
					req.SyncBatchID,
					"",
				}

			case *model.UpdateRecord[Items]:
				newItemsJSON, err := typedRecord.NewItems.ToJSONWithOptions(model.ToJSONOptions{
					UnnestColumns: nil,
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
				}
				oldItemsJSON, err := typedRecord.OldItems.ToJSONWithOptions(model.ToJSONOptions{
					UnnestColumns: nil,
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
				}

				row = []any{
					uuid.New(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					newItemsJSON,
					1,
					oldItemsJSON,
					req.SyncBatchID,
					utils.KeysToString(typedRecord.UnchangedToastColumns),
				}

			case *model.DeleteRecord[Items]:
				itemsJSON, err := typedRecord.Items.ToJSONWithOptions(model.ToJSONOptions{
					UnnestColumns: nil,
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
				}

				row = []any{
					uuid.New(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					itemsJSON,
					2,
					itemsJSON,
					req.SyncBatchID,
					"",
				}

			case *model.MessageRecord[Items]:
				continue

			default:
				return nil, fmt.Errorf("unsupported record type for Postgres flow connector: %T", typedRecord)
			}

			record.PopulateCountMap(tableNameRowsMapping)
			numRecords += 1
			return row, nil
		}

		return nil, nil
	}

	syncRecordsTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for syncing records: %w", err)
	}
	defer shared.RollbackTx(syncRecordsTx, c.logger)

	syncedRecordsCount, err := syncRecordsTx.CopyFrom(ctx, pgx.Identifier{c.metadataSchema, rawTableIdentifier},
		[]string{
			"_peerdb_uid", "_peerdb_timestamp", "_peerdb_destination_table_name", "_peerdb_data",
			"_peerdb_record_type", "_peerdb_match_data", "_peerdb_batch_id", "_peerdb_unchanged_toast_columns",
		},
		pgx.CopyFromFunc(streamReadFunc))
	if err != nil {
		return nil, fmt.Errorf("error syncing records: %w", err)
	}
	if syncedRecordsCount != numRecords {
		return nil, fmt.Errorf("error syncing records: expected %d records to be synced, but %d were synced",
			numRecords, syncedRecordsCount)
	}

	c.logger.Info(fmt.Sprintf("synced %d records to Postgres table %s via COPY",
		syncedRecordsCount, rawTableIdentifier))

	// updating metadata with new offset and syncBatchID
	lastCP := req.Records.GetLastCheckpoint()
	if err := c.updateSyncMetadata(ctx, req.FlowJobName, lastCP, req.SyncBatchID, syncRecordsTx); err != nil {
		return nil, err
	}
	// transaction commits
	if err := syncRecordsTx.Commit(ctx); err != nil {
		return nil, err
	}

	if err := c.ReplayTableSchemaDeltas(ctx, req.Env, req.FlowJobName, req.TableMappings, req.Records.SchemaDeltas, nil); err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckpoint: lastCP,
		NumRecordsSynced:     numRecords,
		CurrentSyncBatchID:   req.SyncBatchID,
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    req.Records.SchemaDeltas,
	}, nil
}

func (c *PostgresConnector) NormalizeRecords(
	ctx context.Context,
	req *model.NormalizeRecordsRequest,
) (model.NormalizeResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	jobMetadataExists, err := c.jobMetadataExists(ctx, req.FlowJobName)
	if err != nil {
		return model.NormalizeResponse{}, err
	}
	// no SyncFlow has run, chill until more records are loaded.
	if !jobMetadataExists {
		c.logger.Info("no metadata found for mirror")
		return model.NormalizeResponse{}, nil
	}

	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		return model.NormalizeResponse{}, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		c.logger.Info(fmt.Sprintf("no records to normalize: syncBatchID %d, normalizeBatchID %d",
			req.SyncBatchID, normBatchID))
		return model.NormalizeResponse{
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	pgversion, err := c.MajorVersion(ctx)
	if err != nil {
		return model.NormalizeResponse{}, err
	}

	normalizeStmtGen := normalizeStmtGenerator{
		Logger:             c.logger,
		rawTableName:       rawTableIdentifier,
		tableSchemaMapping: req.TableNameSchemaMapping,
		peerdbCols: &protos.PeerDBColumns{
			SoftDeleteColName: req.SoftDeleteColName,
			SyncedAtColName:   req.SyncedAtColName,
		},
		supportsMerge:  pgversion >= shared.POSTGRES_15,
		metadataSchema: c.metadataSchema,
	}

	totalRowsAffected := 0
	for batchID := normBatchID + 1; batchID <= req.SyncBatchID; batchID++ {
		unchangedToastColumnsMap, err := c.getTableNametoUnchangedCols(ctx, req.FlowJobName, batchID)
		if err != nil {
			return model.NormalizeResponse{}, err
		}
		normalizeStmtGen.unchangedToastColumnsMap = unchangedToastColumnsMap

		destinationTableNames, err := c.getDistinctTableNamesInBatch(
			ctx, req.FlowJobName, batchID, req.TableNameSchemaMapping)
		if err != nil {
			return model.NormalizeResponse{}, err
		}
		rowsAffected, err := c.normalizeBatch(ctx, batchID, req, destinationTableNames, &normalizeStmtGen)
		if err != nil {
			return model.NormalizeResponse{}, err
		}
		totalRowsAffected += rowsAffected
		c.logger.Info("normalize: committed batch to destination",
			slog.Int64("batchID", batchID),
			slog.Int64("syncBatchID", req.SyncBatchID),
			slog.Int("rowsAffected", rowsAffected),
		)
	}
	c.logger.Info(fmt.Sprintf("normalized %d records", totalRowsAffected))

	return model.NormalizeResponse{
		StartBatchID: normBatchID + 1,
		EndBatchID:   req.SyncBatchID,
	}, nil
}

type batchEntry struct {
	tableName string
	statement string
}

func (c *PostgresConnector) normalizeBatch(
	ctx context.Context,
	batchID int64,
	req *model.NormalizeRecordsRequest,
	destinationTableNames []string,
	normalizeStmtGen *normalizeStmtGenerator,
) (int, error) {
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("error starting transaction for normalizing records: %w", err)
	}
	defer shared.RollbackTx(tx, c.logger)

	for _, tableName := range destinationTableNames {
		if schema, ok := normalizeStmtGen.tableSchemaMapping[tableName]; ok && schema.System == protos.TypeSystem_PG {
			if _, err := tx.Exec(ctx, setSessionReplicaRoleSQL); err != nil {
				return 0, fmt.Errorf("failed to set session_replication_role to replica: %w", err)
			}
			c.logger.Info("set session_replication_role to replica for PG type system normalize")
			break
		}
	}

	batch := &pgx.Batch{}
	var entries []batchEntry
	for _, destinationTableName := range destinationTableNames {
		normalizeStatements := normalizeStmtGen.generateNormalizeStatements(destinationTableName)
		for _, stmt := range normalizeStatements {
			batch.Queue(stmt, batchID, destinationTableName)
			entries = append(entries, batchEntry{tableName: destinationTableName, statement: stmt})
		}
	}
	batch.Queue(
		fmt.Sprintf(updateMetadataForNormalizeRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		batchID, req.FlowJobName,
	)

	results := tx.SendBatch(ctx, batch)
	defer results.Close()

	totalRowsAffected := 0
	for _, entry := range entries {
		ct, err := results.Exec()
		if err != nil {
			c.logger.Error("error executing normalize statement",
				slog.String("statement", entry.statement),
				slog.Int64("batchID", batchID),
				slog.String("destinationTableName", entry.tableName),
				slog.Any("error", err),
			)
			return 0, fmt.Errorf("error executing normalize statement for table %s: %w", entry.tableName, err)
		}
		totalRowsAffected += int(ct.RowsAffected())
	}

	if _, err := results.Exec(); err != nil {
		return 0, fmt.Errorf("failed to update metadata for NormalizeTables: %w", err)
	}

	if err := results.Close(); err != nil {
		return 0, fmt.Errorf("failed to close batch results: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit normalize transaction: %w", err)
	}

	return totalRowsAffected, nil
}

// CreateRawTable creates a raw table, implementing the Connector interface.
func (c *PostgresConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	err := c.createMetadataSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating internal schema: %w", err)
	}

	createRawTableTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for creating raw table: %w", err)
	}
	defer shared.RollbackTx(createRawTableTx, c.logger)

	if _, err := createRawTableTx.Exec(ctx, fmt.Sprintf(createRawTableSQL, c.metadataSchema, rawTableIdentifier)); err != nil {
		return nil, fmt.Errorf("error creating raw table: %w", err)
	}
	if _, err := createRawTableTx.Exec(ctx,
		fmt.Sprintf(createRawTableBatchIDIndexSQL, rawTableIdentifier, c.metadataSchema, rawTableIdentifier),
	); err != nil {
		return nil, fmt.Errorf("error creating batch ID index on raw table: %w", err)
	}
	if _, err := createRawTableTx.Exec(ctx,
		fmt.Sprintf(createRawTableDstTableIndexSQL, rawTableIdentifier, c.metadataSchema, rawTableIdentifier),
	); err != nil {
		return nil, fmt.Errorf("error creating destination table index on raw table: %w", err)
	}

	if err := createRawTableTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction for creating raw table: %w", err)
	}

	return nil, nil
}

func (c *PostgresConnector) StartSetupNormalizedTables(ctx context.Context) (any, error) {
	// Postgres is cool and supports transactional DDL. So we use a transaction.
	return c.conn.Begin(ctx)
}

func (c *PostgresConnector) CleanupSetupNormalizedTables(ctx context.Context, tx any) {
	shared.RollbackTx(tx.(pgx.Tx), c.logger)
}

func (c *PostgresConnector) FinishSetupNormalizedTables(ctx context.Context, tx any) error {
	return tx.(pgx.Tx).Commit(ctx)
}

func (c *PostgresConnector) SetupNormalizedTable(
	ctx context.Context,
	tx any,
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
) (bool, error) {
	createNormalizedTablesTx := tx.(pgx.Tx)

	parsedNormalizedTable, err := common.ParseTableIdentifier(tableIdentifier)
	if err != nil {
		return false, fmt.Errorf("error while parsing table schema and name: %w", err)
	}
	tableAlreadyExists, err := c.tableExists(ctx, parsedNormalizedTable)
	if err != nil {
		return false, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
	}
	if tableAlreadyExists {
		c.logger.Info("[postgres] table already exists, skipping",
			slog.String("table", tableIdentifier))
		if !config.IsResync {
			return true, nil
		}

		err := c.ExecuteCommand(ctx, fmt.Sprintf(dropTableIfExistsSQL,
			common.QuoteIdentifier(parsedNormalizedTable.Namespace),
			common.QuoteIdentifier(parsedNormalizedTable.Table)))
		if err != nil {
			return false, fmt.Errorf("error while dropping _resync table: %w", err)
		}
		c.logger.Info("[postgres] dropped resync table for resync", slog.String("resyncTable", parsedNormalizedTable.String()))
	}

	// convert the column names and types to Postgres types
	normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(config, parsedNormalizedTable, tableSchema)
	_, err = c.execWithLoggingTx(ctx, normalizedTableCreateSQL, createNormalizedTablesTx)
	if err != nil {
		return false, fmt.Errorf("error while creating normalized table: %w", err)
	}

	return false, nil
}

// replayTableSchemaDeltaCore changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *PostgresConnector) ReplayTableSchemaDeltas(
	ctx context.Context,
	_ map[string]string,
	flowJobName string,
	_ []*protos.TableMapping,
	schemaDeltas []*protos.TableSchemaDelta,
	_ []string,
) error {
	if len(schemaDeltas) == 0 {
		return nil
	}

	// Postgres is cool and supports transactional DDL. So we use a transaction.
	tableSchemaModifyTx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction for schema modification: %w",
			err)
	}
	defer shared.RollbackTx(tableSchemaModifyTx, c.logger)

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			columnType := addedColumn.Type
			switch schemaDelta.System {
			case protos.TypeSystem_Q:
				columnType = qValueKindToPostgresType(columnType)
			case protos.TypeSystem_PG:
				// schema qualification handled after numeric typmod check
			default:
				return fmt.Errorf("unknown type system %d", schemaDelta.System)
			}

			if strings.EqualFold(columnType, "numeric") && addedColumn.TypeModifier != -1 {
				precision, scale := numeric.ParseNumericTypmod(addedColumn.TypeModifier)
				columnType = fmt.Sprintf("numeric(%d,%d)", precision, scale)
			} else if schemaDelta.System == protos.TypeSystem_PG && addedColumn.TypeSchemaName != "" {
				schemaQualifiedType := common.QualifiedTable{
					Namespace: addedColumn.TypeSchemaName,
					Table:     columnType,
				}
				columnType = schemaQualifiedType.String()
			}

			dstSchemaTable, err := common.ParseTableIdentifier(schemaDelta.DstTableName)
			if err != nil {
				return fmt.Errorf("error parsing schema and table for %s: %w", schemaDelta.DstTableName, err)
			}

			_, err = c.execWithLoggingTx(ctx, fmt.Sprintf(
				"ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s",
				common.QuoteIdentifier(dstSchemaTable.Namespace),
				common.QuoteIdentifier(dstSchemaTable.Table),
				common.QuoteIdentifier(addedColumn.Name), columnType), tableSchemaModifyTx)
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.Name,
					schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s",
				addedColumn.Name, addedColumn.Type),
				slog.String("srcTableName", schemaDelta.SrcTableName),
				slog.String("dstTableName", schemaDelta.DstTableName),
			)
		}
	}

	if err := tableSchemaModifyTx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction for table schema modification: %w", err)
	}
	return nil
}

func (c *PostgresConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	syncFlowCleanupTx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to begin transaction for sync flow cleanup: %w", err)
	}
	defer shared.RollbackTx(syncFlowCleanupTx, c.logger)

	if _, err := c.execWithLoggingTx(ctx,
		fmt.Sprintf(dropTableIfExistsSQL, c.metadataSchema, getRawTableIdentifier(jobName)), syncFlowCleanupTx,
	); err != nil {
		return fmt.Errorf("unable to drop raw table: %w", err)
	}

	mirrorJobsTableExists, err := c.tableExists(ctx, &common.QualifiedTable{
		Namespace: c.metadataSchema,
		Table:     mirrorJobsTableIdentifier,
	})
	if err != nil {
		return fmt.Errorf("unable to check if job metadata table exists: %w", err)
	}
	if mirrorJobsTableExists {
		if _, err := syncFlowCleanupTx.Exec(ctx,
			fmt.Sprintf(deleteJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName,
		); err != nil {
			return fmt.Errorf("unable to delete job metadata: %w", err)
		}
	}

	if err := syncFlowCleanupTx.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit transaction for sync flow cleanup: %w", err)
	}

	return nil
}

func (c *PostgresConnector) RenameTables(
	ctx context.Context,
	req *protos.RenameTablesInput,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) (*protos.RenameTablesOutput, error) {
	renameTablesTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction for rename tables: %w", err)
	}
	defer shared.RollbackTx(renameTablesTx, c.logger)

	for _, renameRequest := range req.RenameTableOptions {
		srcTable, err := common.ParseTableIdentifier(renameRequest.CurrentName)
		if err != nil {
			return nil, fmt.Errorf("unable to parse source %s: %w", renameRequest.CurrentName, err)
		}
		src := srcTable.String()

		resyncTableExists, err := c.checkIfTableExistsWithTx(ctx, srcTable.Namespace, srcTable.Table, renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to check if _resync table exists: %w", err)
		}

		if !resyncTableExists {
			c.logger.Info(fmt.Sprintf("table '%s' does not exist, skipping rename", src))
			continue
		}

		dstTable, err := common.ParseTableIdentifier(renameRequest.NewName)
		if err != nil {
			return nil, fmt.Errorf("unable to parse destination %s: %w", renameRequest.NewName, err)
		}
		dst := dstTable.String()

		// if original table does not exist, skip soft delete transfer
		originalTableExists, err := c.checkIfTableExistsWithTx(ctx, dstTable.Namespace, dstTable.Table, renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to check if source table exists: %w", err)
		}

		if originalTableExists {
			tableSchema := tableNameSchemaMapping[renameRequest.CurrentName]
			if req.SoftDeleteColName != "" {
				columnNames := make([]string, 0, len(tableSchema.Columns))
				for _, col := range tableSchema.Columns {
					columnNames = append(columnNames, common.QuoteIdentifier(col.Name))
				}

				var pkeyColCompare strings.Builder
				for _, col := range tableSchema.PrimaryKeyColumns {
					pkeyColCompare.WriteString("original_table.")
					pkeyColCompare.WriteString(common.QuoteIdentifier(col))
					pkeyColCompare.WriteString(" = resync_table.")
					pkeyColCompare.WriteString(common.QuoteIdentifier(col))
					pkeyColCompare.WriteString(" AND ")
				}
				pkeyColCompareStr := strings.TrimSuffix(pkeyColCompare.String(), " AND ")

				allCols := strings.Join(columnNames, ",")
				c.logger.Info(fmt.Sprintf("handling soft-deletes for table '%s'...", dst))
				_, err = c.execWithLoggingTx(ctx,
					fmt.Sprintf(
						"INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s original_table "+
							"WHERE NOT EXISTS (SELECT 1 FROM %s resync_table WHERE %s)",
						src, fmt.Sprintf("%s,%s", allCols, common.QuoteIdentifier(req.SoftDeleteColName)), allCols, req.SoftDeleteColName,
						dst, src, pkeyColCompareStr), renameTablesTx)
				if err != nil {
					return nil, fmt.Errorf("unable to handle soft-deletes for table %s: %w", dst, err)
				}
			}
		} else {
			c.logger.Info(fmt.Sprintf("table '%s' did not exist, skipped soft delete transfer", dst))
		}

		// renaming and dropping such that the _resync table is the new destination
		c.logger.Info(fmt.Sprintf("renaming table '%s' to '%s'...", src, dst))

		// drop the dst table if exists
		if _, err := c.execWithLoggingTx(ctx, "DROP TABLE IF EXISTS "+dst, renameTablesTx); err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dst, err)
		}

		// rename the src table to dst
		if _, err := c.execWithLoggingTx(ctx,
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", src, common.QuoteIdentifier(dstTable.Table)),
			renameTablesTx,
		); err != nil {
			return nil, fmt.Errorf("unable to rename table %s to %s: %w", src, dst, err)
		}

		c.logger.Info(fmt.Sprintf("successfully renamed table '%s' to '%s'", src, dst))
	}

	if err := renameTablesTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("unable to commit transaction for rename tables: %w", err)
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *PostgresConnector) RemoveTableEntriesFromRawTable(
	ctx context.Context,
	req *protos.RemoveTablesFromRawTableInput,
) error {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	for _, tableName := range req.DestinationTableNames {
		_, err := c.execWithLogging(ctx, fmt.Sprintf("DELETE FROM %s WHERE _peerdb_destination_table_name = %s"+
			" AND _peerdb_batch_id > %d AND _peerdb_batch_id <= %d",
			common.QuoteIdentifier(rawTableIdentifier), utils.QuoteLiteral(tableName), req.NormalizeBatchId, req.SyncBatchId))
		if err != nil {
			c.logger.Error("failed to remove entries from raw table", slog.Any("error", err))
		}

		c.logger.Info(fmt.Sprintf("successfully removed entries for table '%s' from raw table", tableName))
	}

	return nil
}
