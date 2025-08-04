package connclickhouse

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
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
	destinationTableIdentifier string,
	sourceTableSchema *protos.TableSchema,
) (bool, error) {
	tableAlreadyExists, err := c.checkIfTableExists(ctx, c.config.Database, destinationTableIdentifier)
	if err != nil {
		return false, fmt.Errorf("error occurred while checking if destination ClickHouse table exists: %w", err)
	}
	if tableAlreadyExists && !config.IsResync {
		c.logger.Info("[clickhouse] destination ClickHouse table already exists, skipping", "table", destinationTableIdentifier)
		return true, nil
	}

	normalizedTableCreateSQL, err := c.generateCreateTableSQLForNormalizedTable(
		ctx,
		config,
		destinationTableIdentifier,
		sourceTableSchema,
		c.chVersion,
	)
	if err != nil {
		return false, fmt.Errorf("error while generating create table sql for destination ClickHouse table: %w", err)
	}

	for _, sql := range normalizedTableCreateSQL {
		if err := c.execWithLogging(ctx, sql); err != nil {
			return false, fmt.Errorf("[clickhouse] error while creating destination ClickHouse table: %w", err)
		}
	}
	return false, nil
}

func (c *ClickHouseConnector) generateCreateTableSQLForNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
	chVersion *chproto.Version,
) ([]string, error) {
	var engine string
	tmEngine := protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE

	var tableMapping *protos.TableMapping

	cfg, err := internal.FetchConfigFromDB(config.FlowName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch config from DB: %w", err)
	}
	tableMappings := cfg.TableMappings
	for _, tm := range tableMappings {
		if tm.DestinationTableIdentifier == tableIdentifier {
			tmEngine = tm.Engine
			tableMapping = tm
			break
		}
	}

	switch tmEngine {
	case protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE, protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE:
		if c.config.Replicated {
			engine = fmt.Sprintf(
				"ReplicatedReplacingMergeTree('%s%s','{replica}',%s)",
				zooPathPrefix,
				peerdb_clickhouse.EscapeStr(tableIdentifier),
				peerdb_clickhouse.QuoteIdentifier(versionColName),
			)
		} else {
			engine = fmt.Sprintf("ReplacingMergeTree(%s)", peerdb_clickhouse.QuoteIdentifier(versionColName))
		}
	case protos.TableEngine_CH_ENGINE_MERGE_TREE, protos.TableEngine_CH_ENGINE_REPLICATED_MERGE_TREE:
		if c.config.Replicated {
			engine = fmt.Sprintf(
				"ReplicatedMergeTree('%s%s','{replica}')",
				zooPathPrefix,
				peerdb_clickhouse.EscapeStr(tableIdentifier),
			)
		} else {
			engine = "MergeTree()"
		}
	case protos.TableEngine_CH_ENGINE_COALESCING_MERGE_TREE:
		if c.config.Replicated {
			engine = fmt.Sprintf(
				"ReplicatedCoalescingMergeTree('%s%s','{replica}')",
				zooPathPrefix,
				peerdb_clickhouse.EscapeStr(tableIdentifier),
			)
		} else {
			engine = "CoalescingMergeTree()"
		}
	case protos.TableEngine_CH_ENGINE_NULL:
		engine = "Null"
	}

	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, config.Env)
	if err != nil {
		return nil, err
	}

	var stmtBuilder strings.Builder
	var stmtBuilderDistributed strings.Builder
	var builders []*strings.Builder
	if c.config.Cluster != "" && tmEngine != protos.TableEngine_CH_ENGINE_NULL {
		builders = []*strings.Builder{&stmtBuilder, &stmtBuilderDistributed}
	} else {
		builders = []*strings.Builder{&stmtBuilder}
	}

	colNameMap := make(map[string]string)
	for idx, builder := range builders {
		if config.IsResync {
			builder.WriteString("CREATE OR REPLACE TABLE ")
		} else {
			builder.WriteString("CREATE TABLE IF NOT EXISTS ")
		}
		if c.config.Cluster != "" && tmEngine != protos.TableEngine_CH_ENGINE_NULL && idx == 0 {
			// distributed table gets destination name, avoid naming conflict
			builder.WriteString(peerdb_clickhouse.QuoteIdentifier(tableIdentifier + "_shard"))
		} else {
			builder.WriteString(peerdb_clickhouse.QuoteIdentifier(tableIdentifier))
		}
		if c.config.Cluster != "" {
			fmt.Fprintf(builder, " ON CLUSTER %s", peerdb_clickhouse.QuoteIdentifier(c.config.Cluster))
		}
		builder.WriteString(" (")

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
					ctx, colType, config.Env, protos.DBType_CLICKHOUSE, chVersion, column, tableSchema.NullableEnabled || columnNullableEnabled,
				)
				if err != nil {
					return nil, fmt.Errorf("error while converting column type to ClickHouse type: %w", err)
				}
			} else if (tableSchema.NullableEnabled || columnNullableEnabled) && column.Nullable && !colType.IsArray() {
				clickHouseType = fmt.Sprintf("Nullable(%s)", clickHouseType)
			}

			fmt.Fprintf(builder, "%s %s, ", peerdb_clickhouse.QuoteIdentifier(dstColName), clickHouseType)
		}
		// TODO support hard delete
		// synced at column will be added to all normalized tables
		if config.SyncedAtColName != "" {
			colName := strings.ToLower(config.SyncedAtColName)
			fmt.Fprintf(builder, "%s DateTime64(9) DEFAULT now64(), ", peerdb_clickhouse.QuoteIdentifier(colName))
		}

		// add _peerdb_source_schema_name column
		if sourceSchemaAsDestinationColumn {
			fmt.Fprintf(builder, "%s %s, ", peerdb_clickhouse.QuoteIdentifier(sourceSchemaColName), sourceSchemaColType)
		}

		// add sign and version columns
		fmt.Fprintf(builder, "%s %s, %s %s)",
			peerdb_clickhouse.QuoteIdentifier(signColName), signColType, peerdb_clickhouse.QuoteIdentifier(versionColName), versionColType)
	}

	fmt.Fprintf(&stmtBuilder, " ENGINE = %s", engine)

	if tmEngine != protos.TableEngine_CH_ENGINE_NULL {
		hasNullableKeyFn := buildIsNullableKeyFn(tableMapping, tableSchema.Columns, tableSchema.NullableEnabled)
		orderByColumns, allowNullableKey := getOrderedOrderByColumns(
			tableMapping, colNameMap, tableSchema.PrimaryKeyColumns, hasNullableKeyFn)
		if sourceSchemaAsDestinationColumn {
			orderByColumns = append([]string{sourceSchemaColName}, orderByColumns...)
		}

		if len(orderByColumns) > 0 {
			orderByStr := strings.Join(orderByColumns, ",")
			fmt.Fprintf(&stmtBuilder, " PRIMARY KEY (%[1]s) ORDER BY (%[1]s)", orderByStr)
		} else {
			stmtBuilder.WriteString(" ORDER BY tuple()")
		}

		if tableMapping != nil && tableMapping.PartitionByExpr != "" {
			allowNullableKey = true
			fmt.Fprintf(&stmtBuilder, " PARTITION BY (%s)", tableMapping.PartitionByExpr)
		} else {
			partitionByColumns, hasNullablePartitionKey := getOrderedPartitionByColumns(tableMapping, colNameMap, hasNullableKeyFn)
			if hasNullablePartitionKey {
				allowNullableKey = true
			}
			if len(partitionByColumns) > 0 {
				partitionByStr := strings.Join(partitionByColumns, ",")
				fmt.Fprintf(&stmtBuilder, " PARTITION BY (%s)", partitionByStr)
			}
		}

		if allowNullableKey {
			stmtBuilder.WriteString(" SETTINGS allow_nullable_key = 1")
		}

		if c.config.Cluster != "" {
			fmt.Fprintf(&stmtBuilderDistributed, " ENGINE = Distributed(%s,%s,%s",
				peerdb_clickhouse.QuoteIdentifier(c.config.Cluster),
				peerdb_clickhouse.QuoteIdentifier(c.config.Database),
				peerdb_clickhouse.QuoteIdentifier(tableIdentifier+"_shard"),
			)
			if tableMapping.ShardingKey != "" {
				stmtBuilderDistributed.WriteByte(',')
				stmtBuilderDistributed.WriteString(tableMapping.ShardingKey)
				if tableMapping.PolicyName != "" {
					stmtBuilderDistributed.WriteByte(',')
					stmtBuilderDistributed.WriteString(peerdb_clickhouse.QuoteLiteral(tableMapping.PolicyName))
				}
			}
			stmtBuilderDistributed.WriteByte(')')
		}
	}

	result := make([]string, len(builders))
	for idx, builder := range builders {
		result[idx] = builder.String()
	}
	return result, nil
}

// getOrderedOrderByColumns returns columns to be used for ordering in destination table operations.
// If no custom ordering is specified, return the source table's primary key columns as ordering keys.
// If custom ordering columns are specified, return those columns sorted by their ordering value.
// The boolean return value indicates whether ordering keys contain nullable keys.
func getOrderedOrderByColumns(
	tableMapping *protos.TableMapping,
	colNameMap map[string]string,
	sourcePkeys []string,
	hasNullableKeyFn func(string) bool,
) ([]string, bool) {
	columnOrderingMap := make(map[string]int32)
	if tableMapping != nil {
		for _, col := range tableMapping.Columns {
			if col.Ordering > 0 {
				columnOrderingMap[col.SourceName] = col.Ordering
			}
		}
	}

	// Case 1: No custom ordering - use primary keys
	if len(columnOrderingMap) == 0 {
		hasNullableKeys := false
		pkeys := make([]string, len(sourcePkeys))
		for idx, pk := range sourcePkeys {
			if !hasNullableKeys && hasNullableKeyFn(pk) {
				hasNullableKeys = true
			}
			pkeys[idx] = peerdb_clickhouse.QuoteIdentifier(getColName(colNameMap, pk))
		}
		return pkeys, hasNullableKeys
	}

	// Case 2: Custom ordering specified
	columnNames := slices.Collect(maps.Keys(columnOrderingMap))
	slices.SortStableFunc(columnNames, func(a, b string) int {
		return cmp.Compare(columnOrderingMap[a], columnOrderingMap[b])
	})

	orderbyColumns := make([]string, len(columnNames))
	hasNullableKeys := false
	for idx, col := range columnNames {
		orderbyColumns[idx] = peerdb_clickhouse.QuoteIdentifier(getColName(colNameMap, col))
		if !hasNullableKeys && hasNullableKeyFn(col) {
			hasNullableKeys = true
		}
	}
	return orderbyColumns, hasNullableKeys
}

// getOrderedPartitionByColumns returns columns to be used for partitioning in destination table operations.
// If custom partitioning columns are specified, return those columns sorted by their partitioning value.
// The boolean return value indicates whether partition keys contain nullable keys.
func getOrderedPartitionByColumns(
	tableMapping *protos.TableMapping,
	colNameMap map[string]string,
	hasNullableKeyFn func(string) bool,
) ([]string, bool) {
	columnPartitioningMap := make(map[string]int32)
	if tableMapping != nil {
		for _, col := range tableMapping.Columns {
			if col.Partitioning > 0 {
				columnPartitioningMap[col.SourceName] = col.Partitioning
			}
		}
	}

	columnNames := slices.Collect(maps.Keys(columnPartitioningMap))
	slices.SortStableFunc(columnNames, func(a, b string) int {
		return cmp.Compare(columnPartitioningMap[a], columnPartitioningMap[b])
	})

	partitionbyColumns := make([]string, len(columnNames))
	hasNullableKeys := false
	for idx, col := range columnNames {
		partitionbyColumns[idx] = peerdb_clickhouse.QuoteIdentifier(getColName(colNameMap, col))
		if !hasNullableKeys && hasNullableKeyFn(col) {
			hasNullableKeys = true
		}
	}

	return partitionbyColumns, hasNullableKeys
}

func buildIsNullableKeyFn(
	tableMapping *protos.TableMapping,
	sourceColumns []*protos.FieldDescription,
	tableNullableEnabled bool,
) func(string) bool {
	columnNullableMap := make(map[string]bool)
	for _, col := range sourceColumns {
		columnNullableMap[col.Name] = col.Nullable
	}

	columnNullableEnabledMap := make(map[string]bool)
	if tableMapping != nil {
		for _, col := range tableMapping.Columns {
			columnNullableEnabledMap[col.SourceName] = col.NullableEnabled
		}
	}

	isNullableEnabledFn := func(colName string) bool {
		nullable, exists := columnNullableMap[colName]
		if !exists {
			return false
		}
		nullableEnabled := tableNullableEnabled || columnNullableEnabledMap[colName]

		return nullable && nullableEnabled
	}

	return isNullableEnabledFn
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

	if err := c.copyAvroStagesToDestination(ctx, req.FlowJobName, normBatchID, req.SyncBatchID, req.Env, req.Version); err != nil {
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
			c.logger.Info("[clickhouse] table already synced to destination for this batch, skipping",
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
				c.chVersion,
				c.config.Cluster != "",
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
		peerdb_clickhouse.QuoteIdentifier(rawTbl), normalizeBatchID, syncBatchID)

	rows, err := c.query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("error while querying raw table for distinct table names in batch: %w", err)
	}
	defer rows.Close()
	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("error while scanning table name: %w", err)
		}

		if _, ok := tableToSchema[tableName]; ok {
			tableNames = append(tableNames, tableName)
		} else {
			c.logger.Warn("table not found in table to schema mapping", "table", tableName)
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
	version uint32,
) error {
	avroSyncMethod := c.avroSyncMethod(flowJobName, env, version)
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
	ctx context.Context, flowJobName string, normBatchID int64, syncBatchID int64, env map[string]string, version uint32,
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
		if err := c.copyAvroStageToDestination(ctx, flowJobName, s, env, version); err != nil {
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

func getColName(overrides map[string]string, name string) string {
	if newName, ok := overrides[name]; ok {
		return newName
	}
	return name
}
