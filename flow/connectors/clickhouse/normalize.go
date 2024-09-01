package connclickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

const (
	isDeletedColName = "_peerdb_is_deleted"
	isDeletedColType = "UInt8"
	versionColName   = "_peerdb_version"
	versionColType   = "UInt64"
)

func (c *ClickhouseConnector) StartSetupNormalizedTables(_ context.Context) (interface{}, error) {
	return nil, nil
}

func (c *ClickhouseConnector) FinishSetupNormalizedTables(_ context.Context, _ interface{}) error {
	return nil
}

func (c *ClickhouseConnector) CleanupSetupNormalizedTables(_ context.Context, _ interface{}) {
}

func (c *ClickhouseConnector) SetupNormalizedTable(
	ctx context.Context,
	tx interface{},
	env map[string]string,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
) (bool, error) {
	tableAlreadyExists, err := c.checkIfTableExists(ctx, c.config.Database, tableIdentifier)
	if err != nil {
		return false, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
	}
	if tableAlreadyExists {
		return true, nil
	}

	normalizedTableCreateSQL, err := generateCreateTableSQLForNormalizedTable(
		tableIdentifier,
		tableSchema,
		softDeleteColName,
		syncedAtColName,
	)
	if err != nil {
		return false, fmt.Errorf("error while generating create table sql for normalized table: %w", err)
	}

	err = c.execWithLogging(ctx, normalizedTableCreateSQL)
	if err != nil {
		return false, fmt.Errorf("[ch] error while creating normalized table: %w", err)
	}
	return false, nil
}

func generateCreateTableSQLForNormalizedTable(
	normalizedTable string,
	tableSchema *protos.TableSchema,
	_ string, // softDeleteColName
	syncedAtColName string,
) (string, error) {
	var stmtBuilder strings.Builder
	stmtBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (", normalizedTable))

	for _, column := range tableSchema.Columns {
		colName := column.Name
		colType := qvalue.QValueKind(column.Type)
		clickhouseType, err := colType.ToDWHColumnType(protos.DBType_CLICKHOUSE)
		if err != nil {
			return "", fmt.Errorf("error while converting column type to clickhouse type: %w", err)
		}

		if colType == qvalue.QValueKindNumeric {
			precision, scale := datatypes.GetNumericTypeForWarehouse(column.TypeModifier, datatypes.ClickHouseNumericCompatibility{})
			if column.Nullable {
				stmtBuilder.WriteString(fmt.Sprintf("`%s` Nullable(DECIMAL(%d, %d)), ", colName, precision, scale))
			} else {
				stmtBuilder.WriteString(fmt.Sprintf("`%s` DECIMAL(%d, %d), ", colName, precision, scale))
			}
		} else if tableSchema.NullableEnabled && column.Nullable && !colType.IsArray() {
			stmtBuilder.WriteString(fmt.Sprintf("`%s` Nullable(%s), ", colName, clickhouseType))
		} else {
			stmtBuilder.WriteString(fmt.Sprintf("`%s` %s, ", colName, clickhouseType))
		}
	}
	// TODO support soft delete
	// synced at column will be added to all normalized tables
	if syncedAtColName != "" {
		colName := strings.ToLower(syncedAtColName)
		stmtBuilder.WriteString(fmt.Sprintf("`%s` %s, ", colName, "DateTime64(9) DEFAULT now64()"))
	}

	// add sign and version columns
	stmtBuilder.WriteString(fmt.Sprintf(
		"`%s` %s, `%s` %s) ENGINE = ReplacingMergeTree(`%s`, `%s`)",
		isDeletedColName, isDeletedColType, versionColName, versionColType, versionColName,
		isDeletedColName))

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

func (c *ClickhouseConnector) NormalizeRecords(ctx context.Context,
	req *model.NormalizeRecordsRequest,
) (*model.NormalizeResponse, error) {
	// fix for potential consistency issues
	time.Sleep(3 * time.Second)

	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		c.logger.Error("[clickhouse] error while getting last sync and normalize batch id", "error", err)
		return nil, err
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	err = c.copyAvroStagesToDestination(ctx, req.FlowJobName, normBatchID, req.SyncBatchID)
	if err != nil {
		return nil, fmt.Errorf("failed to copy avro stages to destination: %w", err)
	}

	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		ctx,
		req.FlowJobName,
		req.SyncBatchID,
		normBatchID,
	)
	if err != nil {
		c.logger.Error("[clickhouse] error while getting distinct table names in batch", "error", err)
		return nil, err
	}

	rawTbl := c.getRawTableName(req.FlowJobName)

	// model the raw table data as inserts.
	for _, tbl := range destinationTableNames {
		// SELECT projection FROM raw_table WHERE _peerdb_batch_id > normalize_batch_id AND _peerdb_batch_id <= sync_batch_id
		selectQuery := strings.Builder{}
		selectQuery.WriteString("SELECT ")

		colSelector := strings.Builder{}
		colSelector.WriteString("(")

		schema := req.TableNameSchemaMapping[tbl]

		projection := strings.Builder{}

		for _, column := range schema.Columns {
			cn := column.Name
			ct := column.Type

			colSelector.WriteString(fmt.Sprintf("`%s`,", cn))
			colType := qvalue.QValueKind(ct)
			clickhouseType, err := colType.ToDWHColumnType(protos.DBType_CLICKHOUSE)
			if err != nil {
				return nil, fmt.Errorf("error while converting column type to clickhouse type: %w", err)
			}

			switch clickhouseType {
			case "Date":
				projection.WriteString(fmt.Sprintf(
					"toDate(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, '%s'))) AS `%s`,",
					cn,
					cn,
				))
			case "DateTime64(6)":
				projection.WriteString(fmt.Sprintf(
					"parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, '%s')) AS `%s`,",
					cn,
					cn,
				))
			default:
				projection.WriteString(fmt.Sprintf("JSONExtract(_peerdb_data, '%s', '%s') AS `%s`,", cn, clickhouseType, cn))
			}
		}

		// add _peerdb_sign as _peerdb_record_type / 2
		projection.WriteString(fmt.Sprintf("intDiv(_peerdb_record_type, 2) AS `%s`,", isDeletedColName))
		colSelector.WriteString(fmt.Sprintf("`%s`,", isDeletedColName))

		// add _peerdb_timestamp as _peerdb_version
		projection.WriteString(fmt.Sprintf("_peerdb_timestamp AS `%s`", versionColName))
		colSelector.WriteString(versionColName)
		colSelector.WriteString(") ")

		selectQuery.WriteString(projection.String())
		selectQuery.WriteString(" FROM ")
		selectQuery.WriteString(rawTbl)
		selectQuery.WriteString(" WHERE _peerdb_batch_id > ")
		selectQuery.WriteString(strconv.FormatInt(normBatchID, 10))
		selectQuery.WriteString(" AND _peerdb_batch_id <= ")
		selectQuery.WriteString(strconv.FormatInt(req.SyncBatchID, 10))
		selectQuery.WriteString(" AND _peerdb_destination_table_name = '")
		selectQuery.WriteString(tbl)
		selectQuery.WriteString("'")

		selectQuery.WriteString(" ORDER BY _peerdb_timestamp")

		insertIntoSelectQuery := strings.Builder{}
		insertIntoSelectQuery.WriteString("INSERT INTO ")
		insertIntoSelectQuery.WriteString(tbl)
		insertIntoSelectQuery.WriteString(colSelector.String())
		insertIntoSelectQuery.WriteString(selectQuery.String())

		q := insertIntoSelectQuery.String()

		err = c.execWithLogging(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("error while inserting into normalized table: %w", err)
		}
	}

	endNormalizeBatchId := normBatchID + 1
	err = c.UpdateNormalizeBatchID(ctx, req.FlowJobName, endNormalizeBatchId)
	if err != nil {
		c.logger.Error("[clickhouse] error while updating normalize batch id", "error", err)
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: endNormalizeBatchId,
		EndBatchID:   req.SyncBatchID,
	}, nil
}

func (c *ClickhouseConnector) getDistinctTableNamesInBatch(
	ctx context.Context,
	flowJobName string,
	syncBatchID int64,
	normalizeBatchID int64,
) ([]string, error) {
	rawTbl := c.getRawTableName(flowJobName)

	q := fmt.Sprintf(
		`SELECT DISTINCT _peerdb_destination_table_name FROM %s WHERE _peerdb_batch_id > %d AND _peerdb_batch_id <= %d`,
		rawTbl, normalizeBatchID, syncBatchID)

	rows, err := c.database.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("error while querying raw table for distinct table names in batch: %w", err)
	}
	defer rows.Close()
	var tableNames []string
	for rows.Next() {
		var tableName sql.NullString
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("error while scanning table name: %w", err)
		}

		if !tableName.Valid {
			return nil, errors.New("table name is not valid")
		}

		tableNames = append(tableNames, tableName.String)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	return tableNames, nil
}

func (c *ClickhouseConnector) copyAvroStageToDestination(ctx context.Context, flowJobName string, syncBatchID int64) error {
	avroSynvMethod := c.avroSyncMethod(flowJobName)
	avroFile, err := c.s3Stage.GetAvroStage(ctx, flowJobName, syncBatchID)
	if err != nil {
		return fmt.Errorf("failed to get avro stage: %w", err)
	}
	defer avroFile.Cleanup()

	err = avroSynvMethod.CopyStageToDestination(ctx, avroFile)
	if err != nil {
		return fmt.Errorf("failed to copy stage to destination: %w", err)
	}
	return nil
}

func (c *ClickhouseConnector) copyAvroStagesToDestination(
	ctx context.Context, flowJobName string, normBatchID, syncBatchID int64,
) error {
	for s := normBatchID + 1; s <= syncBatchID; s++ {
		err := c.copyAvroStageToDestination(ctx, flowJobName, s)
		if err != nil {
			return fmt.Errorf("failed to copy avro stage to destination: %w", err)
		}
	}
	return nil
}
