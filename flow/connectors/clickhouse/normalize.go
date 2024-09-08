package connclickhouse

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

const (
	signColName    = "_peerdb_is_deleted"
	signColType    = "Int8"
	versionColName = "_peerdb_version"
	versionColType = "Int64"
)

var acceptableTableEngines = []string{"ReplacingMergeTree", "MergeTree"}

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
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
) (bool, error) {
	tableAlreadyExists, err := c.checkIfTableExists(ctx, c.config.Database, tableIdentifier)
	if err != nil {
		return false, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
	}
	if tableAlreadyExists && !config.IsResync {
		c.logger.Info("[ch] normalized table already exists, skipping", "table", tableIdentifier)
		return true, nil
	}

	normalizedTableCreateSQL, err := generateCreateTableSQLForNormalizedTable(
		config,
		tableIdentifier,
	)
	if err != nil {
		return false, fmt.Errorf("error while generating create table sql for normalized table: %w", err)
	}

	if err := c.execWithLogging(ctx, normalizedTableCreateSQL); err != nil {
		return false, fmt.Errorf("[ch] error while creating normalized table: %w", err)
	}
	return false, nil
}

func getColName(overrides map[string]string, name string) string {
	if newName, ok := overrides[name]; ok {
		return newName
	}
	return name
}

func generateCreateTableSQLForNormalizedTable(
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
) (string, error) {
	tableSchema := config.TableNameSchemaMapping[tableIdentifier]

	var tableMapping *protos.TableMapping
	for _, tm := range config.TableMappings {
		if tm.DestinationTableIdentifier == tableIdentifier {
			tableMapping = tm
			break
		}
	}

	var stmtBuilder strings.Builder
	stmtBuilder.WriteString("CREATE ")
	if config.IsResync {
		stmtBuilder.WriteString("OR REPLACE ")
	}
	stmtBuilder.WriteString("TABLE ")
	if !config.IsResync {
		stmtBuilder.WriteString("IF NOT EXISTS ")
	}
	stmtBuilder.WriteString("`")
	stmtBuilder.WriteString(tableIdentifier)
	stmtBuilder.WriteString("` (")

	colNameMap := make(map[string]string)
	for _, column := range tableSchema.Columns {
		colName := column.Name
		dstColName := colName
		colType := qvalue.QValueKind(column.Type)
		var clickhouseType string
		var columnSetting *protos.ColumnSetting
		if tableMapping != nil {
			for _, col := range tableMapping.Columns {
				if col.SourceName == colName {
					columnSetting = col
					if columnSetting.DestinationName != "" {
						dstColName = columnSetting.DestinationName
						colNameMap[colName] = dstColName
					}
					if columnSetting.DestinationType != "" {
						// TODO can we restrict this to avoid injection?
						clickhouseType = columnSetting.DestinationType
					}
					break
				}
			}
		}

		if clickhouseType == "" {
			var err error
			clickhouseType, err = colType.ToDWHColumnType(protos.DBType_CLICKHOUSE)
			if err != nil {
				return "", fmt.Errorf("error while converting column type to clickhouse type: %w", err)
			}
		}

		if colType == qvalue.QValueKindNumeric {
			precision, scale := datatypes.GetNumericTypeForWarehouse(column.TypeModifier, datatypes.ClickHouseNumericCompatibility{})
			if column.Nullable {
				stmtBuilder.WriteString(fmt.Sprintf("`%s` Nullable(DECIMAL(%d, %d)), ", dstColName, precision, scale))
			} else {
				stmtBuilder.WriteString(fmt.Sprintf("`%s` DECIMAL(%d, %d), ", dstColName, precision, scale))
			}
		} else if tableSchema.NullableEnabled && column.Nullable && !colType.IsArray() {
			stmtBuilder.WriteString(fmt.Sprintf("`%s` Nullable(%s), ", dstColName, clickhouseType))
		} else {
			stmtBuilder.WriteString(fmt.Sprintf("`%s` %s, ", dstColName, clickhouseType))
		}
	}
	// TODO support soft delete
	// synced at column will be added to all normalized tables
	if config.SyncedAtColName != "" {
		colName := strings.ToLower(config.SyncedAtColName)
		stmtBuilder.WriteString(fmt.Sprintf("`%s` DateTime64(9) DEFAULT now64(), ", colName))
	}

	var engine string
	if tableMapping == nil || tableMapping.Engine == protos.TableEngine_CH_ENGINE_MERGE_TREE {
		engine = "MergeTree()"
	} else {
		engine = fmt.Sprintf("ReplacingMergeTree(`%s`)", versionColName)
	}

	// add sign and version columns
	stmtBuilder.WriteString(fmt.Sprintf(
		"`%s` %s, `%s` %s) ENGINE = %s",
		signColName, signColType, versionColName, versionColType, engine))

	var pkeyStr string
	pkeys := tableSchema.PrimaryKeyColumns
	if len(pkeys) > 0 {
		if len(colNameMap) > 0 {
			pkeys = slices.Clone(pkeys)
			for idx, pk := range pkeys {
				pkeys[idx] = getColName(colNameMap, pk)
			}
		}
		pkeyStr = strings.Join(pkeys, ",")

		stmtBuilder.WriteString("PRIMARY KEY (")
		stmtBuilder.WriteString(pkeyStr)
		stmtBuilder.WriteString(") ")
	}

	orderby := make([]*protos.ColumnSetting, 0, len(tableMapping.Columns))
	for _, col := range tableMapping.Columns {
		if col.Ordering > 0 && !slices.Contains(pkeys, getColName(colNameMap, col.SourceName)) {
			orderby = append(orderby, col)
		}
	}
	slices.SortStableFunc(orderby, func(a *protos.ColumnSetting, b *protos.ColumnSetting) int {
		return cmp.Compare(a.Ordering, b.Ordering)
	})

	if pkeyStr != "" || len(orderby) > 0 {
		stmtBuilder.WriteString("ORDER BY (")
		stmtBuilder.WriteString(pkeyStr)
		if len(orderby) > 0 {
			orderbyColumns := make([]string, len(orderby))
			for idx, col := range orderby {
				orderbyColumns[idx] = getColName(colNameMap, col.SourceName)
			}

			if pkeyStr != "" {
				stmtBuilder.WriteRune(',')
			}
			stmtBuilder.WriteString(strings.Join(orderbyColumns, ","))
		}
		stmtBuilder.WriteRune(')')
	}

	return stmtBuilder.String(), nil
}

func (c *ClickhouseConnector) NormalizeRecords(
	ctx context.Context,
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

		var tableMapping *protos.TableMapping
		for _, tm := range req.TableMappings {
			if tm.DestinationTableIdentifier == tbl {
				tableMapping = tm
				break
			}
		}

		projection := strings.Builder{}

		for _, column := range schema.Columns {
			colName := column.Name
			dstColName := colName
			colType := qvalue.QValueKind(column.Type)

			var clickhouseType string
			if tableMapping != nil {
				for _, col := range tableMapping.Columns {
					if col.SourceName == colName {
						if col.DestinationName != "" {
							dstColName = col.DestinationName
						}
						if col.DestinationType != "" {
							// TODO can we restrict this to avoid injection?
							clickhouseType = col.DestinationType
						}
						break
					}
				}
			}

			colSelector.WriteString(fmt.Sprintf("`%s`,", dstColName))
			if clickhouseType == "" {
				var err error
				clickhouseType, err = colType.ToDWHColumnType(protos.DBType_CLICKHOUSE)
				if err != nil {
					return nil, fmt.Errorf("error while converting column type to clickhouse type: %w", err)
				}
			}

			switch clickhouseType {
			case "Date":
				projection.WriteString(fmt.Sprintf(
					"toDate(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, '%s'))) AS `%s`,",
					colName,
					dstColName,
				))
			case "DateTime64(6)":
				projection.WriteString(fmt.Sprintf(
					"parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, '%s')) AS `%s`,",
					colName,
					dstColName,
				))
			default:
				projection.WriteString(fmt.Sprintf("JSONExtract(_peerdb_data, '%s', '%s') AS `%s`,", colName, clickhouseType, dstColName))
			}
		}

		// add _peerdb_sign as _peerdb_record_type / 2
		projection.WriteString(fmt.Sprintf("intDiv(_peerdb_record_type, 2) AS `%s`,", signColName))
		colSelector.WriteString(fmt.Sprintf("`%s`,", signColName))

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

		if err := c.execWithLogging(ctx, q); err != nil {
			return nil, fmt.Errorf("error while inserting into normalized table: %w", err)
		}
	}

	err = c.UpdateNormalizeBatchID(ctx, req.FlowJobName, req.SyncBatchID)
	if err != nil {
		c.logger.Error("[clickhouse] error while updating normalize batch id", "error", err)
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normBatchID + 1,
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

	if rows.Err() != nil {
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
