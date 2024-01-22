package connclickhouse

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
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
		c.logger.ErrorContext(c.ctx, "[clickhouse] error while getting last sync and normalize batch id", err)
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

	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		req.FlowJobName,
		batchIDs.SyncBatchID,
		batchIDs.NormalizeBatchID,
	)
	if err != nil {
		c.logger.ErrorContext(c.ctx, "[clickhouse] error while getting distinct table names in batch", err)
		return nil, err
	}

	// model the raw table data as inserts.

	endNormalizeBatchId := batchIDs.NormalizeBatchID + 1
	c.pgMetadata.UpdateNormalizeBatchID(req.FlowJobName, endNormalizeBatchId)
	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: endNormalizeBatchId,
		EndBatchID:   batchIDs.SyncBatchID,
	}, nil
}

func (c *ClickhouseConnector) getDistinctTableNamesInBatch(
	flowJobName string,
	syncBatchID int64,
	normalizeBatchID int64,
) ([]string, error) {
	rawTbl := c.getRawTableName(flowJobName)

	q := fmt.Sprintf(
		`SELECT DISTINCT _peerdb_destination_table_name FROM %s WHERE _peerdb_batch_id > %d AND _peerdb_batch_id <= %d`,
		rawTbl, normalizeBatchID, syncBatchID)

	rows, err := c.database.QueryContext(c.ctx, q)
	if err != nil {
		return nil, fmt.Errorf("error while querying raw table for distinct table names in batch: %w", err)
	}

	var tableNames []string
	for rows.Next() {
		var tableName sql.NullString
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("error while scanning table name: %w", err)
		}

		if !tableName.Valid {
			return nil, fmt.Errorf("table name is not valid")
		}

		tableNames = append(tableNames, tableName.String)
	}

	return tableNames, nil
}

func (c *ClickhouseConnector) GetLastSyncAndNormalizeBatchID(flowJobName string) (model.SyncAndNormalizeBatchID, error) {
	syncBatchID, err := c.pgMetadata.GetLastBatchID(flowJobName)
	if err != nil {
		return model.SyncAndNormalizeBatchID{}, fmt.Errorf("error while getting last sync batch id: %w", err)
	}

	normalizeBatchID, err := c.pgMetadata.GetLastNormalizeBatchID(flowJobName)
	if err != nil {
		return model.SyncAndNormalizeBatchID{}, fmt.Errorf("error while getting last normalize batch id: %w", err)
	}

	return model.SyncAndNormalizeBatchID{
		SyncBatchID:      syncBatchID,
		NormalizeBatchID: normalizeBatchID,
	}, nil
}
