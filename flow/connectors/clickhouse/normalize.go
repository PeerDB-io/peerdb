package connclickhouse

import (
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
		c.logger.ErrorContext(c.ctx, "[sf] error while getting last sync and normalize batch id", err)
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

	// This will never happen.
	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: batchIDs.NormalizeBatchID + 1,
		EndBatchID:   batchIDs.SyncBatchID,
	}, nil
}

func (c *ClickhouseConnector) GetLastSyncAndNormalizeBatchID(flowJobName string) (model.SyncAndNormalizeBatchID, error) {
	// return sync batch id as the normalize batch id as well as this is a no-op.
	batchId, err := c.pgMetadata.GetLastBatchID(flowJobName)
	if err != nil {
		return model.SyncAndNormalizeBatchID{}, err
	}

	return model.SyncAndNormalizeBatchID{
		SyncBatchID:      batchId,
		NormalizeBatchID: batchId,
	}, nil
}
