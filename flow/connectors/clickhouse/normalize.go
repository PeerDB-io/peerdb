package connclickhouse

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

const (
	signColName    = "_peerdb_is_deleted"
	signColType    = "Int8"
	versionColName = "_peerdb_version"
	versionColType = "Int64"
)

func (c *ClickHouseConnector) StartSetupNormalizedTables(_ context.Context) (interface{}, error) {
	return nil, nil
}

func (c *ClickHouseConnector) FinishSetupNormalizedTables(_ context.Context, _ interface{}) error {
	return nil
}

func (c *ClickHouseConnector) CleanupSetupNormalizedTables(_ context.Context, _ interface{}) {
}

func (c *ClickHouseConnector) SetupNormalizedTable(
	ctx context.Context,
	tx interface{},
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
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
		ctx,
		config,
		tableIdentifier,
		tableSchema,
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
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
) (string, error) {
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
		var columnNullableEnabled bool
		var clickHouseType string
		if tableMapping != nil {
			for _, col := range tableMapping.Columns {
				if col.SourceName == colName {
					if col.DestinationName != "" {
						dstColName = col.DestinationName
						colNameMap[colName] = dstColName
					}
					if col.DestinationType != "" {
						clickHouseType = col.DestinationType
					}
					columnNullableEnabled = col.NullableEnabled
					break
				}
			}
		}

		if clickHouseType == "" {
			var err error
			clickHouseType, err = colType.ToDWHColumnType(ctx, config.Env, protos.DBType_CLICKHOUSE, column)
			if err != nil {
				return "", fmt.Errorf("error while converting column type to ClickHouse type: %w", err)
			}
		}
		if (tableSchema.NullableEnabled || columnNullableEnabled) && column.Nullable && !colType.IsArray() {
			clickHouseType = fmt.Sprintf("Nullable(%s)", clickHouseType)
		}

		stmtBuilder.WriteString(fmt.Sprintf("`%s` %s, ", dstColName, clickHouseType))
	}
	// TODO support soft delete
	// synced at column will be added to all normalized tables
	if config.SyncedAtColName != "" {
		colName := strings.ToLower(config.SyncedAtColName)
		stmtBuilder.WriteString(fmt.Sprintf("`%s` DateTime64(9) DEFAULT now64(), ", colName))
	}

	var engine string
	if tableMapping == nil {
		engine = fmt.Sprintf("ReplacingMergeTree(`%s`)", versionColName)
	} else if tableMapping.Engine == protos.TableEngine_CH_ENGINE_MERGE_TREE {
		engine = "MergeTree()"
	} else {
		engine = fmt.Sprintf("ReplacingMergeTree(`%s`)", versionColName)
	}

	// add sign and version columns
	stmtBuilder.WriteString(fmt.Sprintf(
		"`%s` %s, `%s` %s) ENGINE = %s",
		signColName, signColType, versionColName, versionColType, engine))

	orderByColumns := getOrderedOrderByColumns(tableMapping, tableSchema.PrimaryKeyColumns, colNameMap)

	if len(orderByColumns) > 0 {
		orderByStr := strings.Join(orderByColumns, ",")

		stmtBuilder.WriteString("PRIMARY KEY (")
		stmtBuilder.WriteString(orderByStr)
		stmtBuilder.WriteString(") ")

		stmtBuilder.WriteString("ORDER BY (")
		stmtBuilder.WriteString(orderByStr)
		stmtBuilder.WriteString(") ")
	}

	if nullable, err := peerdbenv.PeerDBNullable(ctx, config.Env); err != nil {
		return "", err
	} else if nullable {
		stmtBuilder.WriteString(" SETTINGS allow_nullable_key = 1")
	}

	return stmtBuilder.String(), nil
}

// Returns a list of order by columns ordered by their ordering, and puts the pkeys at the end.
// pkeys are excluded from the order by columns.
func getOrderedOrderByColumns(
	tableMapping *protos.TableMapping,
	sourcePkeys []string,
	colNameMap map[string]string,
) []string {
	pkeys := slices.Clone(sourcePkeys)
	if len(sourcePkeys) > 0 {
		if len(colNameMap) > 0 {
			for idx, pk := range sourcePkeys {
				pkeys[idx] = getColName(colNameMap, pk)
			}
		}
	}

	orderby := make([]*protos.ColumnSetting, 0)
	if tableMapping != nil {
		for _, col := range tableMapping.Columns {
			if col.Ordering > 0 {
				orderby = append(orderby, col)
			}
		}
	}

	if len(orderby) == 0 {
		return pkeys
	}

	slices.SortStableFunc(orderby, func(a *protos.ColumnSetting, b *protos.ColumnSetting) int {
		return cmp.Compare(a.Ordering, b.Ordering)
	})

	orderbyColumns := make([]string, len(orderby))
	for idx, col := range orderby {
		orderbyColumns[idx] = getColName(colNameMap, col.SourceName)
	}

	return orderbyColumns
}

func (c *ClickHouseConnector) NormalizeRecords(
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

	if err := c.copyAvroStagesToDestination(ctx, req.FlowJobName, normBatchID, req.SyncBatchID); err != nil {
		return nil, fmt.Errorf("failed to copy avro stages to destination: %w", err)
	}

	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		ctx,
		req.FlowJobName,
		req.SyncBatchID,
		normBatchID,
		req.TableNameSchemaMapping,
	)
	if err != nil {
		c.logger.Error("[clickhouse] error while getting distinct table names in batch", "error", err)
		return nil, err
	}

	enablePrimaryUpdate, err := peerdbenv.PeerDBEnableClickHousePrimaryUpdate(ctx, req.Env)
	if err != nil {
		return nil, err
	}

	parallelNormalize, err := peerdbenv.PeerDBClickHouseParallelNormalize(ctx, req.Env)
	if err != nil {
		return nil, err
	}
	parallelNormalize = min(max(parallelNormalize, 1), len(destinationTableNames))
	if parallelNormalize > 1 {
		c.logger.Info("normalizing in parallel", slog.Int("connections", parallelNormalize))
	}

	queries := make(chan string)
	rawTbl := c.getRawTableName(req.FlowJobName)

	group, errCtx := errgroup.WithContext(ctx)
	for i := range parallelNormalize {
		group.Go(func() error {
			var chConn clickhouse.Conn
			if i == 0 {
				chConn = c.database
			} else {
				var err error
				chConn, err = Connect(errCtx, req.Env, c.config)
				if err != nil {
					return err
				}
				defer chConn.Close()
			}

			for query := range queries {
				c.logger.Info("normalizing batch", slog.String("query", query))
				if err := chConn.Exec(errCtx, query); err != nil {
					return fmt.Errorf("error while inserting into normalized table: %w", err)
				}
			}
			return nil
		})
	}

	for _, tbl := range destinationTableNames {
		// SELECT projection FROM raw_table WHERE _peerdb_batch_id > normalize_batch_id AND _peerdb_batch_id <= sync_batch_id
		selectQuery := strings.Builder{}
		selectQuery.WriteString("SELECT ")

		colSelector := strings.Builder{}
		colSelector.WriteRune('(')

		schema := req.TableNameSchemaMapping[tbl]

		var tableMapping *protos.TableMapping
		for _, tm := range req.TableMappings {
			if tm.DestinationTableIdentifier == tbl {
				tableMapping = tm
				break
			}
		}

		projection := strings.Builder{}
		projectionUpdate := strings.Builder{}

		for _, column := range schema.Columns {
			colName := column.Name
			dstColName := colName
			colType := qvalue.QValueKind(column.Type)

			var clickHouseType string
			var columnNullableEnabled bool
			if tableMapping != nil {
				for _, col := range tableMapping.Columns {
					if col.SourceName == colName {
						if col.DestinationName != "" {
							dstColName = col.DestinationName
						}
						if col.DestinationType != "" {
							// TODO can we restrict this to avoid injection?
							clickHouseType = col.DestinationType
						}
						columnNullableEnabled = col.NullableEnabled
						break
					}
				}
			}

			colSelector.WriteString(fmt.Sprintf("`%s`,", dstColName))
			if clickHouseType == "" {
				var err error
				clickHouseType, err = colType.ToDWHColumnType(ctx, req.Env, protos.DBType_CLICKHOUSE, column)
				if err != nil {
					close(queries)
					return nil, fmt.Errorf("error while converting column type to clickhouse type: %w", err)
				}

				if (schema.NullableEnabled || columnNullableEnabled) && column.Nullable && !colType.IsArray() {
					clickHouseType = fmt.Sprintf("Nullable(%s)", clickHouseType)
				}
			}

			switch clickHouseType {
			case "Date32", "Nullable(Date32)":
				projection.WriteString(fmt.Sprintf(
					"toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, '%s'),6)) AS `%s`,",
					colName,
					dstColName,
				))
				if enablePrimaryUpdate {
					projectionUpdate.WriteString(fmt.Sprintf(
						"toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_match_data, '%s'),6)) AS `%s`,",
						colName,
						dstColName,
					))
				}
			case "DateTime64(6)", "Nullable(DateTime64(6))":
				projection.WriteString(fmt.Sprintf(
					"parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, '%s'),6) AS `%s`,",
					colName,
					dstColName,
				))
				if enablePrimaryUpdate {
					projectionUpdate.WriteString(fmt.Sprintf(
						"parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_match_data, '%s'),6) AS `%s`,",
						colName,
						dstColName,
					))
				}
			default:
				projection.WriteString(fmt.Sprintf("JSONExtract(_peerdb_data, '%s', '%s') AS `%s`,", colName, clickHouseType, dstColName))
				if enablePrimaryUpdate {
					projectionUpdate.WriteString(fmt.Sprintf(
						"JSONExtract(_peerdb_match_data, '%s', '%s') AS `%s`,",
						colName,
						clickHouseType,
						dstColName,
					))
				}
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

		if enablePrimaryUpdate {
			// projectionUpdate generates delete on previous record, so _peerdb_record_type is filled in as 2
			projectionUpdate.WriteString(fmt.Sprintf("1 AS `%s`,", signColName))
			// decrement timestamp by 1 so delete is ordered before latest data,
			// could be same if deletion records were only generated when ordering updated
			projectionUpdate.WriteString(fmt.Sprintf("_peerdb_timestamp - 1 AS `%s`", versionColName))

			selectQuery.WriteString("UNION ALL SELECT ")
			selectQuery.WriteString(projectionUpdate.String())
			selectQuery.WriteString(" FROM ")
			selectQuery.WriteString(rawTbl)
			selectQuery.WriteString(" WHERE _peerdb_batch_id > ")
			selectQuery.WriteString(strconv.FormatInt(normBatchID, 10))
			selectQuery.WriteString(" AND _peerdb_batch_id <= ")
			selectQuery.WriteString(strconv.FormatInt(req.SyncBatchID, 10))
			selectQuery.WriteString(" AND _peerdb_destination_table_name = '")
			selectQuery.WriteString(tbl)
			selectQuery.WriteString("' AND _peerdb_record_type = 1")
		}

		insertIntoSelectQuery := strings.Builder{}
		insertIntoSelectQuery.WriteString("INSERT INTO `")
		insertIntoSelectQuery.WriteString(tbl)
		insertIntoSelectQuery.WriteString("` ")
		insertIntoSelectQuery.WriteString(colSelector.String())
		insertIntoSelectQuery.WriteString(selectQuery.String())

		select {
		case queries <- insertIntoSelectQuery.String():
		case <-errCtx.Done():
			close(queries)
			c.logger.Error("[clickhouse] context canceled while normalizing",
				slog.Any("error", errCtx.Err()),
				slog.Any("cause", context.Cause(errCtx)))
			return nil, context.Cause(errCtx)
		}
	}
	close(queries)
	if err := group.Wait(); err != nil {
		return nil, err
	}

	if err := c.UpdateNormalizeBatchID(ctx, req.FlowJobName, req.SyncBatchID); err != nil {
		c.logger.Error("[clickhouse] error while updating normalize batch id", slog.Int64("BatchID", req.SyncBatchID), slog.Any("error", err))
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normBatchID + 1,
		EndBatchID:   req.SyncBatchID,
	}, nil
}

func (c *ClickHouseConnector) getDistinctTableNamesInBatch(
	ctx context.Context,
	flowJobName string,
	syncBatchID int64,
	normalizeBatchID int64,
	tableToSchema map[string]*protos.TableSchema,
) ([]string, error) {
	rawTbl := c.getRawTableName(flowJobName)

	q := fmt.Sprintf(
		`SELECT DISTINCT _peerdb_destination_table_name FROM %s WHERE _peerdb_batch_id>%d AND _peerdb_batch_id<=%d`,
		rawTbl, normalizeBatchID, syncBatchID)

	rows, err := c.query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("error while querying raw table for distinct table names in batch: %w", err)
	}
	defer rows.Close()
	var tableNames []string
	for rows.Next() {
		var tableName sql.NullString
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("error while scanning table name: %w", err)
		}

		if !tableName.Valid {
			return nil, errors.New("table name is not valid")
		}

		if _, ok := tableToSchema[tableName.String]; ok {
			tableNames = append(tableNames, tableName.String)
		} else {
			c.logger.Warn("table not found in table to schema mapping", "table", tableName.String)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	return tableNames, nil
}

func (c *ClickHouseConnector) copyAvroStageToDestination(ctx context.Context, flowJobName string, syncBatchID int64) error {
	avroSyncMethod := c.avroSyncMethod(flowJobName)
	avroFile, err := GetAvroStage(ctx, flowJobName, syncBatchID)
	if err != nil {
		return fmt.Errorf("failed to get avro stage: %w", err)
	}
	defer avroFile.Cleanup()

	err = avroSyncMethod.CopyStageToDestination(ctx, avroFile)
	if err != nil {
		return fmt.Errorf("failed to copy stage to destination: %w", err)
	}
	return nil
}

func (c *ClickHouseConnector) copyAvroStagesToDestination(
	ctx context.Context, flowJobName string, normBatchID, syncBatchID int64,
) error {
	for s := normBatchID + 1; s <= syncBatchID; s++ {
		if err := c.copyAvroStageToDestination(ctx, flowJobName, s); err != nil {
			return fmt.Errorf("failed to copy avro stage to destination: %w", err)
		}
	}
	return nil
}
