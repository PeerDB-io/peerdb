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

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

const (
	signColName    = "_peerdb_is_deleted"
	signColType    = "Int8"
	versionColName = "_peerdb_version"
	versionColType = "Int64"
)

func (c *ClickHouseConnector) StartSetupNormalizedTables(_ context.Context) (any, error) {
	return nil, nil
}

func (c *ClickHouseConnector) FinishSetupNormalizedTables(_ context.Context, _ any) error {
	return nil
}

func (c *ClickHouseConnector) CleanupSetupNormalizedTables(_ context.Context, _ any) {
}

func (c *ClickHouseConnector) SetupNormalizedTable(
	ctx context.Context,
	tx any,
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
			clickHouseType, err = colType.ToDWHColumnType(
				ctx, config.Env, protos.DBType_CLICKHOUSE, column, tableSchema.NullableEnabled || columnNullableEnabled,
			)
			if err != nil {
				return "", fmt.Errorf("error while converting column type to ClickHouse type: %w", err)
			}
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
	} else {
		stmtBuilder.WriteString("ORDER BY tuple()")
	}

	if nullable, err := internal.PeerDBNullable(ctx, config.Env); err != nil {
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

type TableNormalizeQuery struct {
	TableName string
	Query     string
	Part      uint64
}

func (c *ClickHouseConnector) NormalizeRecords(
	ctx context.Context,
	req *model.NormalizeRecordsRequest,
) (model.NormalizeResponse, error) {
	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		c.logger.Error("[clickhouse] error while getting last sync and normalize batch id", "error", err)
		return model.NormalizeResponse{}, err
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		return model.NormalizeResponse{
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	if err := c.copyAvroStagesToDestination(ctx, req.FlowJobName, req.SyncBatchID, req.Env); err != nil {
		return model.NormalizeResponse{}, fmt.Errorf("failed to copy avro stages to destination: %w", err)
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
		return model.NormalizeResponse{}, err
	}

	enablePrimaryUpdate, err := internal.PeerDBEnableClickHousePrimaryUpdate(ctx, req.Env)
	if err != nil {
		return model.NormalizeResponse{}, err
	}

	parallelNormalize, err := internal.PeerDBClickHouseParallelNormalize(ctx, req.Env)
	if err != nil {
		return model.NormalizeResponse{}, err
	}
	parallelNormalize = min(max(parallelNormalize, 1), len(destinationTableNames))
	c.logger.Info("[clickhouse] normalizing batch",
		slog.Int64("StartBatchID", normBatchID),
		slog.Int64("EndBatchID", req.SyncBatchID),
		slog.Int("connections", parallelNormalize))

	numParts, err := internal.PeerDBClickHouseNormalizationParts(ctx, req.Env)
	if err != nil {
		c.logger.Warn("failed to get chunking parts, proceeding without chunking", slog.Any("error", err))
		numParts = 1
	}
	numParts = max(numParts, 1)

	queries := make(chan TableNormalizeQuery)
	rawTbl := c.GetRawTableName(req.FlowJobName)

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
				c.logger.Info("executing normalize query",
					slog.Int64("syncBatchId", req.SyncBatchID),
					slog.Int64("normalizeBatchId", normBatchID),
					slog.String("query", query.Query),
					slog.String("table", query.TableName))

				if err := c.execWithConnection(ctx, chConn, query.Query); err != nil {
					return fmt.Errorf("error while inserting into normalized table: %w", err)
				}

				if query.Part == numParts-1 {
					err := c.SetLastNormalizedBatchIDForTable(ctx, req.FlowJobName, query.TableName, req.SyncBatchID)
					if err != nil {
						return fmt.Errorf("error while setting last synced batch id for table %s: %w", query.TableName, err)
					}
				}
			}
			return nil
		})
	}

	for _, tbl := range destinationTableNames {
		normalizeBatchIDForTable, err := c.GetLastNormalizedBatchIDForTable(ctx, req.FlowJobName, tbl)
		if err != nil {
			c.logger.Error("[clickhouse] error while getting last synced batch id for table", "table", tbl, "error", err)
			return model.NormalizeResponse{}, err
		}

		if normalizeBatchIDForTable >= req.SyncBatchID {
			c.logger.Info("[clickhouse] "+tbl+" already normalized, skipping",
				"table", tbl, "batchIDForTable", normalizeBatchIDForTable, "syncBatchID", req.SyncBatchID)
			continue
		}

		for numPart := range numParts {
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
					clickHouseType, err = colType.ToDWHColumnType(
						ctx, req.Env, protos.DBType_CLICKHOUSE, column, schema.NullableEnabled || columnNullableEnabled,
					)
					if err != nil {
						close(queries)
						return model.NormalizeResponse{}, fmt.Errorf("error while converting column type to clickhouse type: %w", err)
					}
				}

				switch clickHouseType {
				case "Date32", "Nullable(Date32)":
					projection.WriteString(fmt.Sprintf(
						"toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, '%s'),6)) AS `%s`,",
						colName, dstColName,
					))
					if enablePrimaryUpdate {
						projectionUpdate.WriteString(fmt.Sprintf(
							"toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_match_data, '%s'),6)) AS `%s`,",
							colName, dstColName,
						))
					}
				case "DateTime64(6)", "Nullable(DateTime64(6))":
					projection.WriteString(fmt.Sprintf(
						"parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, '%s'),6) AS `%s`,",
						colName, dstColName,
					))
					if enablePrimaryUpdate {
						projectionUpdate.WriteString(fmt.Sprintf(
							"parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_match_data, '%s'),6) AS `%s`,",
							colName, dstColName,
						))
					}
				default:
					projLen := projection.Len()
					if colType == qvalue.QValueKindBytes {
						format, err := internal.PeerDBBinaryFormat(ctx, req.Env)
						if err != nil {
							return model.NormalizeResponse{}, err
						}
						switch format {
						case internal.BinaryFormatRaw:
							projection.WriteString(fmt.Sprintf(
								"base64Decode(JSONExtractString(_peerdb_data, '%s')) AS `%s`,",
								colName, dstColName,
							))
							if enablePrimaryUpdate {
								projectionUpdate.WriteString(fmt.Sprintf(
									"base64Decode(JSONExtractString(_peerdb_match_data, '%s')) AS `%s`,",
									colName, dstColName,
								))
							}
						case internal.BinaryFormatHex:
							projection.WriteString(fmt.Sprintf("hex(base64Decode(JSONExtractString(_peerdb_data, '%s'))) AS `%s`,",
								colName, dstColName))
							if enablePrimaryUpdate {
								projectionUpdate.WriteString(fmt.Sprintf(
									"hex(base64Decode(JSONExtractString(_peerdb_match_data, '%s'))) AS `%s`,",
									colName, dstColName,
								))
							}
						}
					}

					// proceed with default logic if logic above didn't add any sql
					if projection.Len() == projLen {
						projection.WriteString(fmt.Sprintf(
							"JSONExtract(_peerdb_data, '%s', '%s') AS `%s`,",
							colName, clickHouseType, dstColName,
						))
						if enablePrimaryUpdate {
							projectionUpdate.WriteString(fmt.Sprintf(
								"JSONExtract(_peerdb_match_data, '%s', '%s') AS `%s`,",
								colName, clickHouseType, dstColName,
							))
						}
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
			if numParts > 1 {
				selectQuery.WriteString(fmt.Sprintf(" AND cityHash64(_peerdb_uid) %% %d = %d", numParts, numPart))
			}

			if enablePrimaryUpdate {
				// projectionUpdate generates delete on previous record, so _peerdb_record_type is filled in as 2
				projectionUpdate.WriteString(fmt.Sprintf("1 AS `%s`,", signColName))
				// decrement timestamp by 1 so delete is ordered before latest data,
				// could be same if deletion records were only generated when ordering updated
				projectionUpdate.WriteString(fmt.Sprintf("_peerdb_timestamp - 1 AS `%s`", versionColName))

				selectQuery.WriteString(" UNION ALL SELECT ")
				selectQuery.WriteString(projectionUpdate.String())
				selectQuery.WriteString(" FROM ")
				selectQuery.WriteString(rawTbl)
				selectQuery.WriteString(" WHERE _peerdb_match_data != '' AND _peerdb_batch_id > ")
				selectQuery.WriteString(strconv.FormatInt(normBatchID, 10))
				selectQuery.WriteString(" AND _peerdb_batch_id <= ")
				selectQuery.WriteString(strconv.FormatInt(req.SyncBatchID, 10))
				selectQuery.WriteString(" AND _peerdb_destination_table_name = '")
				selectQuery.WriteString(tbl)
				selectQuery.WriteString("' AND _peerdb_record_type = 1")
				if numParts > 1 {
					selectQuery.WriteString(fmt.Sprintf(" AND cityHash64(_peerdb_uid) %% %d = %d", numParts, numPart))
				}
			}

			insertIntoSelectQuery := strings.Builder{}
			insertIntoSelectQuery.WriteString("INSERT INTO `")
			insertIntoSelectQuery.WriteString(tbl)
			insertIntoSelectQuery.WriteString("` ")
			insertIntoSelectQuery.WriteString(colSelector.String())
			insertIntoSelectQuery.WriteString(selectQuery.String())

			select {
			case queries <- TableNormalizeQuery{
				TableName: tbl,
				Query:     insertIntoSelectQuery.String(),
				Part:      numPart,
			}:
			case <-errCtx.Done():
				close(queries)
				c.logger.Error("[clickhouse] context canceled while normalizing",
					slog.Any("error", errCtx.Err()),
					slog.Any("cause", context.Cause(errCtx)))
				return model.NormalizeResponse{}, context.Cause(errCtx)
			}
		}
	}
	close(queries)
	if err := group.Wait(); err != nil {
		return model.NormalizeResponse{}, err
	}

	if err := c.UpdateNormalizeBatchID(ctx, req.FlowJobName, req.SyncBatchID); err != nil {
		c.logger.Error("[clickhouse] error while updating normalize batch id", slog.Int64("BatchID", req.SyncBatchID), slog.Any("error", err))
		return model.NormalizeResponse{}, err
	}

	return model.NormalizeResponse{
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
	rawTbl := c.GetRawTableName(flowJobName)

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

func (c *ClickHouseConnector) copyAvroStageToDestination(
	ctx context.Context,
	flowJobName string,
	syncBatchID int64,
	env map[string]string,
) error {
	avroSyncMethod := c.avroSyncMethod(flowJobName, env)
	avroFile, err := GetAvroStage(ctx, flowJobName, syncBatchID)
	if err != nil {
		return fmt.Errorf("failed to get avro stage: %w", err)
	}
	defer avroFile.Cleanup()

	if err := avroSyncMethod.CopyStageToDestination(ctx, avroFile); err != nil {
		return fmt.Errorf("failed to copy stage to destination: %w", err)
	}
	return nil
}

func (c *ClickHouseConnector) copyAvroStagesToDestination(
	ctx context.Context, flowJobName string, syncBatchID int64, env map[string]string,
) error {
	lastSyncedBatchIdInRawTable, err := c.GetLastBatchIDInRawTable(ctx, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get last batch id in raw table: %w", err)
	}

	for s := lastSyncedBatchIdInRawTable + 1; s <= syncBatchID; s++ {
		if err := c.copyAvroStageToDestination(ctx, flowJobName, s, env); err != nil {
			return fmt.Errorf("failed to copy avro stage to destination: %w", err)
		}
		if err := c.SetLastBatchIDInRawTable(ctx, flowJobName, s); err != nil {
			return fmt.Errorf("failed to set last batch id in raw table: %w", err)
		}
	}
	return nil
}
