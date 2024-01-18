package connclickhouse

import (
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"golang.org/x/sync/errgroup"
)

const (
	signColName    = "_peerdb_sign"
	signColType    = "Int8"
	versionColName = "_peerdb_version"
	versionColType = "Int8"
)

func (c *ClickhouseConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	tableExistsMapping := make(map[string]bool)
	for tableIdentifier, tableSchema := range req.TableNameSchemaMapping {
		normalizedSchemaTable, err := utils.ParseSchemaTable(tableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("error while parsing table schema and name: %w", err)
		}
		tableAlreadyExists, err := c.checkIfTableExists(normalizedSchemaTable.Schema, normalizedSchemaTable.Table)
		if err != nil {
			return nil, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
		}
		if tableAlreadyExists {
			tableExistsMapping[tableIdentifier] = true
			continue
		}

		normalizedTableCreateSQL, err := generateCreateTableSQLForNormalizedTable(
			normalizedSchemaTable,
			tableSchema,
			req.SoftDeleteColName,
			req.SyncedAtColName,
		)
		if err != nil {
			return nil, fmt.Errorf("error while generating create table sql for normalized table: %w", err)
		}

		_, err = c.database.ExecContext(c.ctx, normalizedTableCreateSQL)
		if err != nil {
			return nil, fmt.Errorf("[sf] error while creating normalized table: %w", err)
		}
		tableExistsMapping[tableIdentifier] = false
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func generateCreateTableSQLForNormalizedTable(
	normalizedSchemaTable *utils.SchemaTable,
	tableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
) (string, error) {
	var stmtBuilder strings.Builder
	stmtBuilder.WriteString(fmt.Sprintf("CREATE TABLE `%s`.`%s` (", normalizedSchemaTable.Schema, normalizedSchemaTable.Table))

	nc := len(tableSchema.ColumnNames)
	for i := 0; i < nc; i++ {
		colName := tableSchema.ColumnNames[i]
		colType := qvalue.QValueKind(tableSchema.ColumnTypes[i])
		clickhouseType, err := qValueKindToClickhouseType(colType)
		if err != nil {
			return "", fmt.Errorf("error while converting column type to clickhouse type: %w", err)
		}
		stmtBuilder.WriteString(fmt.Sprintf("`%s` %s, ", colName, clickhouseType))
	}

	// TODO support soft delete

	// synced at column will be added to all normalized tables
	if syncedAtColName != "" {
		stmtBuilder.WriteString(fmt.Sprintf("`%s` %s, ", syncedAtColName, "DateTime64(9)"))
	}

	// add sign and version columns
	stmtBuilder.WriteString(fmt.Sprintf("`%s` %s, ", signColName, signColType))
	stmtBuilder.WriteString(fmt.Sprintf("`%s` %s", versionColName, versionColType))

	stmtBuilder.WriteString(fmt.Sprintf(") ENGINE = ReplacingMergeTree(`%s`) ", versionColName))

	pkeys := tableSchema.PrimaryKeyColumns
	if len(pkeys) > 0 {
		pkeyStr := strings.Join(pkeys, ",")

		stmtBuilder.WriteString("PRIMARY KEY (")
		stmtBuilder.WriteString(pkeyStr)
		stmtBuilder.WriteString(") ")

		stmtBuilder.WriteString("ORDER BY (")
		stmtBuilder.WriteString(pkeyStr)
		stmtBuilder.WriteString(")")
	}

	return stmtBuilder.String(), nil
}

func (c *ClickhouseConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	batchIDs, err := c.GetLastSyncAndNormalizeBatchID(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// normalize has caught up with sync, chill until more records are loaded.
	if batchIDs.NormalizeBatchID >= batchIDs.SyncBatchID {
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: batchIDs.NormalizeBatchID,
			EndBatchID:   batchIDs.SyncBatchID,
		}, nil
	}

	jobMetadataExists, err := c.jobMetadataExists(req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// sync hasn't created job metadata yet, chill.
	if !jobMetadataExists {
		return &model.NormalizeResponse{
			Done: false,
		}, nil
	}
	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		req.FlowJobName,
		batchIDs.SyncBatchID,
		batchIDs.NormalizeBatchID,
	)
	if err != nil {
		return nil, err
	}

	tableNametoUnchangedToastCols, err := c.getTableNametoUnchangedCols(req.FlowJobName, batchIDs.SyncBatchID, batchIDs.NormalizeBatchID)
	if err != nil {
		return nil, fmt.Errorf("couldn't tablename to unchanged cols mapping: %w", err)
	}

	var totalRowsAffected int64 = 0
	g, gCtx := errgroup.WithContext(c.ctx)
	g.SetLimit(8) // limit parallel merges to 8

	for _, destinationTableName := range destinationTableNames {
		tableName := destinationTableName // local variable for the closure

		g.Go(func() error {
			mergeGen := &mergeStmtGenerator{
				rawTableName:          getRawTableIdentifier(req.FlowJobName),
				dstTableName:          tableName,
				syncBatchID:           batchIDs.SyncBatchID,
				normalizeBatchID:      batchIDs.NormalizeBatchID,
				normalizedTableSchema: req.TableNameSchemaMapping[tableName],
				unchangedToastColumns: tableNametoUnchangedToastCols[tableName],
				peerdbCols: &protos.PeerDBColumns{
					SoftDelete:        req.SoftDelete,
					SoftDeleteColName: req.SoftDeleteColName,
					SyncedAtColName:   req.SyncedAtColName,
				},
			}
			mergeStatement, err := mergeGen.generateMergeStmt()
			if err != nil {
				return err
			}

			startTime := time.Now()
			c.logger.Info("[merge] merging records...", slog.String("destTable", tableName))

			result, err := c.database.ExecContext(gCtx, mergeStatement, tableName)
			if err != nil {
				return fmt.Errorf("failed to merge records into %s (statement: %s): %w",
					tableName, mergeStatement, err)
			}

			endTime := time.Now()
			c.logger.Info(fmt.Sprintf("[merge] merged records into %s, took: %d seconds",
				tableName, endTime.Sub(startTime)/time.Second))
			if err != nil {
				c.logger.Error("[merge] error while normalizing records", slog.Any("error", err))
				return err
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected by merge statement for table %s: %w", tableName, err)
			}

			atomic.AddInt64(&totalRowsAffected, rowsAffected)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("error while normalizing records: %w", err)
	}

	// updating metadata with new normalizeBatchID
	err = c.updateNormalizeMetadata(req.FlowJobName, batchIDs.SyncBatchID)
	if err != nil {
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: batchIDs.NormalizeBatchID + 1,
		EndBatchID:   batchIDs.SyncBatchID,
	}, nil
}
