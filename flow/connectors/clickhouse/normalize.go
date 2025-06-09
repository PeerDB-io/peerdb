package connclickhouse

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	signColName         = "_peerdb_is_deleted"
	signColType         = "Int8"
	versionColName      = "_peerdb_version"
	versionColType      = "Int64"
	sourceSchemaColName = "_peerdb_source_schema"
	sourceSchemaColType = "LowCardinality(String)"
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
		return false, fmt.Errorf("error occurred while checking if destination ClickHouse table exists: %w", err)
	}
	if tableAlreadyExists && !config.IsResync {
		c.logger.Info("[ch] destination ClickHouse table already exists, skipping", "table", tableIdentifier)
		return true, nil
	}

	normalizedTableCreateSQL, err := generateCreateTableSQLForNormalizedTable(
		ctx,
		config,
		tableIdentifier,
		tableSchema,
	)
	if err != nil {
		return false, fmt.Errorf("error while generating create table sql for destination ClickHouse table: %w", err)
	}

	if err := c.execWithLogging(ctx, normalizedTableCreateSQL); err != nil {
		return false, fmt.Errorf("[ch] error while creating destination ClickHouse table: %w", err)
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
	fmt.Fprintf(&stmtBuilder, "%s (", peerdb_clickhouse.QuoteIdentifier(tableIdentifier))

	colNameMap := make(map[string]string)
	for _, column := range tableSchema.Columns {
		colName := column.Name
		dstColName := colName
		colType := types.QValueKind(column.Type)
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
			clickHouseType, err = qvalue.ToDWHColumnType(
				ctx, colType, config.Env, protos.DBType_CLICKHOUSE, column, tableSchema.NullableEnabled || columnNullableEnabled,
			)
			if err != nil {
				return "", fmt.Errorf("error while converting column type to ClickHouse type: %w", err)
			}
		}

		fmt.Fprintf(&stmtBuilder, "%s %s, ", peerdb_clickhouse.QuoteIdentifier(dstColName), clickHouseType)
	}
	// TODO support soft delete
	// synced at column will be added to all normalized tables
	if config.SyncedAtColName != "" {
		colName := strings.ToLower(config.SyncedAtColName)
		fmt.Fprintf(&stmtBuilder, "%s DateTime64(9) DEFAULT now64(), ", peerdb_clickhouse.QuoteIdentifier(colName))
	}

	// add _peerdb_source_schema_name column
	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, config.Env)
	if err != nil {
		return "", err
	}
	if sourceSchemaAsDestinationColumn {
		fmt.Fprintf(&stmtBuilder, "%s %s, ", peerdb_clickhouse.QuoteIdentifier(sourceSchemaColName), sourceSchemaColType)
	}

	var engine string
	tmEngine := protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE
	if tableMapping != nil {
		tmEngine = tableMapping.Engine
	}
	switch tmEngine {
	case protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE:
		engine = fmt.Sprintf("ReplacingMergeTree(%s)", peerdb_clickhouse.QuoteIdentifier(versionColName))
	case protos.TableEngine_CH_ENGINE_MERGE_TREE:
		engine = "MergeTree()"
	case protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE:
		engine = fmt.Sprintf(
			"ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/%s','{replica}',%s)",
			peerdb_clickhouse.EscapeStr(tableIdentifier),
			peerdb_clickhouse.QuoteIdentifier(versionColName),
		)
	case protos.TableEngine_CH_ENGINE_REPLICATED_MERGE_TREE:
		engine = fmt.Sprintf(
			"ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/%s','{replica}')",
			peerdb_clickhouse.EscapeStr(tableIdentifier),
		)
	case protos.TableEngine_CH_ENGINE_NULL:
		engine = "Null"
	}

	// add sign and version columns
	fmt.Fprintf(&stmtBuilder, "%s %s, %s %s) ENGINE = %s",
		peerdb_clickhouse.QuoteIdentifier(signColName), signColType, peerdb_clickhouse.QuoteIdentifier(versionColName), versionColType, engine)

	orderByColumns := getOrderedOrderByColumns(tableMapping, tableSchema.PrimaryKeyColumns, colNameMap)

	if sourceSchemaAsDestinationColumn {
		orderByColumns = append([]string{sourceSchemaColName}, orderByColumns...)
	}

	if tmEngine != protos.TableEngine_CH_ENGINE_NULL {
		if len(orderByColumns) > 0 {
			orderByStr := strings.Join(orderByColumns, ",")

			fmt.Fprintf(&stmtBuilder, " PRIMARY KEY (%[1]s) ORDER BY (%[1]s)", orderByStr)
		} else {
			stmtBuilder.WriteString(" ORDER BY tuple()")
		}

		if nullable, err := internal.PeerDBNullable(ctx, config.Env); err != nil {
			return "", err
		} else if nullable {
			stmtBuilder.WriteString(" SETTINGS allow_nullable_key = 1")
		}
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
	pkeys := make([]string, len(sourcePkeys))
	for idx, pk := range sourcePkeys {
		pkeys[idx] = peerdb_clickhouse.QuoteIdentifier(pk)
	}
	if len(sourcePkeys) > 0 {
		if len(colNameMap) > 0 {
			for idx, pk := range sourcePkeys {
				pkeys[idx] = peerdb_clickhouse.QuoteIdentifier(getColName(colNameMap, pk))
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
		orderbyColumns[idx] = peerdb_clickhouse.QuoteIdentifier(getColName(colNameMap, col.SourceName))
	}

	return orderbyColumns
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

	if err := c.copyAvroStagesToDestination(ctx, req.FlowJobName, normBatchID, req.SyncBatchID, req.Env); err != nil {
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

	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, req.Env)
	if err != nil {
		return model.NormalizeResponse{}, err
	}

	parallelNormalize, err := internal.PeerDBClickHouseParallelNormalize(ctx, req.Env)
	if err != nil {
		return model.NormalizeResponse{}, err
	}
	parallelNormalize = min(max(parallelNormalize, 1), len(destinationTableNames))
	c.logger.Info("[clickhouse-cdc] inserting batch...",
		slog.Int64("StartBatchID", normBatchID),
		slog.Int64("EndBatchID", req.SyncBatchID),
		slog.Int("connections", parallelNormalize))

	numParts, err := internal.PeerDBClickHouseNormalizationParts(ctx, req.Env)
	if err != nil {
		c.logger.Warn("failed to get chunking parts, proceeding without chunking", slog.Any("error", err))
		numParts = 1
	}

	// This is for cases where currently normalizing can take a looooong time
	// there is no other indication of progress, so we log every 5 minutes.
	periodicLogger := shared.Interval(ctx, 5*time.Minute, func() {
		c.logger.Info("[clickhouse-cdc] inserting batch...",
			slog.Int64("StartBatchID", normBatchID),
			slog.Int64("EndBatchID", req.SyncBatchID),
			slog.Int("connections", parallelNormalize))
	})
	defer periodicLogger()

	numParts = max(numParts, 1)

	queries := make(chan NormalizeQueryGenerator)
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

			for insertIntoSelectQuery := range queries {
				c.logger.Info("executing INSERT command to ClickHouse table",
					slog.Int64("syncBatchId", req.SyncBatchID),
					slog.Int64("normalizeBatchId", normBatchID),
					slog.String("destinationTable", insertIntoSelectQuery.TableName),
					slog.String("query", insertIntoSelectQuery.Query))

				if err := c.execWithConnection(errCtx, chConn, insertIntoSelectQuery.Query); err != nil {
					c.logger.Error("[clickhouse] error while inserting into target clickhouse table",
						slog.String("table", insertIntoSelectQuery.TableName),
						slog.Int64("syncBatchID", req.SyncBatchID),
						slog.Int64("normalizeBatchID", normBatchID),
						slog.Any("error", err))
					return fmt.Errorf("error while inserting into target clickhouse table %s: %w", insertIntoSelectQuery.TableName, err)
				}

				if insertIntoSelectQuery.Part == numParts-1 {
					c.logger.Info("[clickhouse] set last normalized batch id for table",
						slog.String("table", insertIntoSelectQuery.TableName),
						slog.Int64("syncBatchID", req.SyncBatchID),
						slog.Int64("lastNormalizedBatchID", normBatchID))
					err := c.SetLastNormalizedBatchIDForTable(ctx, req.FlowJobName, insertIntoSelectQuery.TableName, req.SyncBatchID)
					if err != nil {
						return fmt.Errorf("error while setting last synced batch id for table %s: %w", insertIntoSelectQuery.TableName, err)
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

		c.logger.Info("[clickhouse] last normalized batch id for table",
			"table", tbl, "lastNormalizedBatchID", normalizeBatchIDForTable,
			"syncBatchID", req.SyncBatchID)
		batchIdToLoadForTable := max(normBatchID, normalizeBatchIDForTable)
		if batchIdToLoadForTable >= req.SyncBatchID {
			c.logger.Info("[clickhouse] "+tbl+" already synced to destination for this batch, skipping",
				"table", tbl, "batchIdToLoadForTable", batchIdToLoadForTable, "syncBatchID", req.SyncBatchID)
			continue
		}

		for numPart := range numParts {
			queryGenerator := NewNormalizeQueryGenerator(
				tbl,
				numPart,
				req.TableNameSchemaMapping,
				req.TableMappings,
				req.SyncBatchID,
				batchIdToLoadForTable,
				numParts,
				enablePrimaryUpdate,
				sourceSchemaAsDestinationColumn,
				req.Env,
				rawTbl,
			)
			insertIntoSelectQuery, err := queryGenerator.BuildQuery(ctx)
			if err != nil {
				close(queries)
				c.logger.Error("[clickhouse] error while building insert into select query",
					slog.String("table", tbl),
					slog.Int64("syncBatchID", req.SyncBatchID),
					slog.Int64("normalizeBatchID", normBatchID),
					slog.Any("error", err))
				return model.NormalizeResponse{}, fmt.Errorf("error while building insert into select query for table %s: %w", tbl, err)
			}

			select {
			case queries <- NormalizeQueryGenerator{
				TableName: tbl,
				Query:     insertIntoSelectQuery,
				Part:      numPart,
			}:
			case <-errCtx.Done():
				close(queries)
				c.logger.Error("[clickhouse] context canceled while inserting data to ClickHouse",
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
		"SELECT DISTINCT _peerdb_destination_table_name FROM %s WHERE _peerdb_batch_id>%d AND _peerdb_batch_id<=%d",
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
	ctx context.Context, flowJobName string, normBatchID int64, syncBatchID int64, env map[string]string,
) error {
	lastSyncedBatchIdInRawTable, err := c.GetLastBatchIDInRawTable(ctx, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get last batch id in raw table: %w", err)
	}

	batchIdToLoad := max(lastSyncedBatchIdInRawTable, normBatchID)
	c.logger.Info("[clickhouse] pushing s3 data to raw table",
		slog.Int64("BatchID", batchIdToLoad),
		slog.String("flowJobName", flowJobName),
		slog.Int64("syncBatchID", syncBatchID))

	for s := batchIdToLoad + 1; s <= syncBatchID; s++ {
		if err := c.copyAvroStageToDestination(ctx, flowJobName, s, env); err != nil {
			return fmt.Errorf("failed to copy avro stage to destination: %w", err)
		}
		c.logger.Info("[clickhouse] setting last batch id in raw table",
			slog.Int64("BatchID", s),
			slog.String("flowJobName", flowJobName))
		if err := c.SetLastBatchIDInRawTable(ctx, flowJobName, s); err != nil {
			c.logger.Error("[clickhouse] error while setting last batch id in raw table",
				slog.Int64("BatchID", s), slog.Any("error", err))
			return fmt.Errorf("failed to set last batch id in raw table: %w", err)
		}
	}
	return nil
}
