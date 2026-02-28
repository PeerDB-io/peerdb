package connclickhouse

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	chinternal "github.com/PeerDB-io/peerdb/flow/internal/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	isDeletedColName    = "_peerdb_is_deleted"
	isDeletedColType    = "UInt8"
	versionColName      = "_peerdb_version"
	versionColType      = "UInt64"
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
	tableAlreadyExists, err := c.checkIfTableExists(ctx, c.Config.Database, destinationTableIdentifier)
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
		config.Flags,
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
	flags []string,
) ([]string, error) {
	var engine string
	tmEngine := protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE

	var tableMapping *protos.TableMapping
	for _, tm := range config.TableMappings {
		if tm.DestinationTableIdentifier == tableIdentifier {
			tmEngine = tm.Engine
			tableMapping = tm
			break
		}
	}

	isDeletedColumn := isDeletedColName
	isDeletedColumnPart := ""
	if config.SoftDeleteColName != "" {
		isDeletedColumn = config.SoftDeleteColName
		isDeletedColumnPart = ", " + peerdb_clickhouse.QuoteIdentifier(isDeletedColumn)
	}

	switch tmEngine {
	case protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE, protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE:
		if c.Config.Replicated {
			engine = fmt.Sprintf(
				"ReplicatedReplacingMergeTree('%s%s','{replica}',%s%s)",
				zooPathPrefix,
				peerdb_clickhouse.EscapeStr(tableIdentifier),
				peerdb_clickhouse.QuoteIdentifier(versionColName),
				isDeletedColumnPart,
			)
		} else {
			engine = fmt.Sprintf("ReplacingMergeTree(%s%s)", peerdb_clickhouse.QuoteIdentifier(versionColName), isDeletedColumnPart)
		}
	case protos.TableEngine_CH_ENGINE_MERGE_TREE, protos.TableEngine_CH_ENGINE_REPLICATED_MERGE_TREE:
		if c.Config.Replicated {
			engine = fmt.Sprintf(
				"ReplicatedMergeTree('%s%s','{replica}')",
				zooPathPrefix,
				peerdb_clickhouse.EscapeStr(tableIdentifier),
			)
		} else {
			engine = "MergeTree()"
		}
	case protos.TableEngine_CH_ENGINE_COALESCING_MERGE_TREE:
		if c.Config.Replicated {
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
	if c.Config.Cluster != "" && tmEngine != protos.TableEngine_CH_ENGINE_NULL {
		builders = []*strings.Builder{&stmtBuilder, &stmtBuilderDistributed}
	} else {
		builders = []*strings.Builder{&stmtBuilder}
	}

	colNameMap := make(map[string]string)
	shardSuffix := "_shard"
	if config.IsResync {
		shardSuffix += strconv.FormatInt(time.Now().Unix(), 10)
	}
	for idx, builder := range builders {
		if config.IsResync {
			builder.WriteString("CREATE OR REPLACE TABLE ")
		} else {
			builder.WriteString("CREATE TABLE IF NOT EXISTS ")
		}
		if c.Config.Cluster != "" && tmEngine != protos.TableEngine_CH_ENGINE_NULL && idx == 0 {
			// distributed table gets destination name, avoid naming conflict
			builder.WriteString(peerdb_clickhouse.QuoteIdentifier(tableIdentifier + shardSuffix))
		} else {
			builder.WriteString(peerdb_clickhouse.QuoteIdentifier(tableIdentifier))
		}
		if c.Config.Cluster != "" {
			fmt.Fprintf(builder, " ON CLUSTER %s", peerdb_clickhouse.QuoteIdentifier(c.Config.Cluster))
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
					ctx, colType, config.Env, protos.DBType_CLICKHOUSE, chVersion, column,
					tableSchema.NullableEnabled || columnNullableEnabled, flags,
				)
				if err != nil {
					return nil, fmt.Errorf("error while converting column type to ClickHouse type: %w", err)
				}
			} else if (tableSchema.NullableEnabled || columnNullableEnabled) && column.Nullable && !colType.IsArray() {
				clickHouseType = fmt.Sprintf("Nullable(%s)", clickHouseType)
			}

			fmt.Fprintf(builder, "%s %s, ", peerdb_clickhouse.QuoteIdentifier(dstColName), clickHouseType)
		}

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
			peerdb_clickhouse.QuoteIdentifier(isDeletedColumn), isDeletedColType,
			peerdb_clickhouse.QuoteIdentifier(versionColName), versionColType)
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
			stmtBuilder.WriteString(chinternal.NewCHSettingsString(chVersion, chinternal.SettingAllowNullableKey, "1"))
		}

		if c.Config.Cluster != "" {
			fmt.Fprintf(&stmtBuilderDistributed, " ENGINE = Distributed(%s,%s,%s",
				peerdb_clickhouse.QuoteIdentifier(c.Config.Cluster),
				peerdb_clickhouse.QuoteIdentifier(c.Config.Database),
				peerdb_clickhouse.QuoteIdentifier(tableIdentifier+shardSuffix),
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
	lastNormBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		c.logger.Error("[clickhouse] error while getting last sync and normalize batch id", slog.Any("error", err))
		return model.NormalizeResponse{}, err
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if lastNormBatchID >= req.SyncBatchID {
		return model.NormalizeResponse{
			StartBatchID: lastNormBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	groupBatches, err := internal.PeerDBGroupNormalize(ctx, req.Env)
	if err != nil || groupBatches <= 0 {
		c.logger.Error("failed to lookup PEERDB_GROUP_NORMALIZE, only normalizing 4 batches")
		groupBatches = 4
	}

	endBatchID := min(req.SyncBatchID, lastNormBatchID+groupBatches)

	if err := c.copyAvroStagesToDestination(ctx, req.FlowJobName, lastNormBatchID, endBatchID, req.Env, req.Version); err != nil {
		return model.NormalizeResponse{}, fmt.Errorf("failed to copy avro stages to destination: %w", err)
	}

	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		ctx,
		req.FlowJobName,
		endBatchID,
		lastNormBatchID,
		req.TableNameSchemaMapping,
	)
	if err != nil {
		c.logger.Error("[clickhouse] error while getting distinct table names in batch", slog.Any("error", err))
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
	// parallelize normalization up to the number of destination tables
	parallelNormalize = min(max(parallelNormalize, 1), len(destinationTableNames))
	c.logger.Info("[clickhouse-cdc] inserting batches...",
		slog.Int64("lastNormBatchID", lastNormBatchID),
		slog.Int64("endBatchID", endBatchID),
		slog.Int("connections", parallelNormalize))

	// This is for cases where currently normalizing can take a looooong time
	// there is no other indication of progress, so we log every 5 minutes.
	periodicLogger := common.Interval(ctx, 5*time.Minute, func() {
		c.logger.Info("[clickhouse-cdc] inserting batches...",
			slog.Int64("lastNormBatchID", lastNormBatchID),
			slog.Int64("endBatchID", endBatchID),
			slog.Int("connections", parallelNormalize))
	})
	defer periodicLogger()

	type queryInfo struct {
		table           string
		query           string
		lastNormBatchID int64
	}
	queriesCh := make(chan queryInfo)
	rawTbl := c.GetRawTableName(req.FlowJobName)

	group, errCtx := errgroup.WithContext(ctx)
	// create N=PEERDB_CLICKHOUSE_PARALLEL_NORMALIZE goroutines to process requests from queriesCh
	for i := range parallelNormalize {
		group.Go(func() error {
			var chConn clickhouse.Conn
			if i == 0 {
				chConn = c.database
			} else {
				var err error
				chConn, err = Connect(errCtx, req.Env, c.Config)
				if err != nil {
					return err
				}
				defer chConn.Close()
			}

			for q := range queriesCh {
				c.logger.Info("executing INSERT command to ClickHouse table",
					slog.String("table", q.table),
					slog.Int64("endBatchID", endBatchID),
					slog.Int64("lastNormBatchID", q.lastNormBatchID),
					slog.String("query", q.query),
					slog.Int("parallelWorker", i))

				if err := c.execWithConnection(errCtx, chConn, q.query); err != nil {
					c.logger.Error("[clickhouse] error while inserting into target clickhouse table",
						slog.String("table", q.table),
						slog.Int64("endBatchID", endBatchID),
						slog.Int64("lastNormBatchID", q.lastNormBatchID),
						slog.Int("parallelWorker", i),
						slog.Any("error", err))
					return fmt.Errorf("error while inserting into target clickhouse table %s: %w", q.table, err)
				}

				c.logger.Info("[clickhouse] set last normalized batch id for table",
					slog.String("table", q.table),
					slog.Int64("endBatchID", endBatchID),
					slog.Int64("lastNormBatchID", q.lastNormBatchID),
					slog.Int("parallelWorker", i))
				if err := c.SetLastNormalizedBatchIDForTable(errCtx, req.FlowJobName, q.table, endBatchID); err != nil {
					return fmt.Errorf("error while setting last synced batch id for table %s: %w", q.table, err)
				}

				c.logger.Info("executed INSERT command to ClickHouse",
					slog.String("table", q.table),
					slog.Int64("endBatchID", endBatchID),
					slog.Int64("lastNormBatchID", q.lastNormBatchID),
					slog.Int("parallelWorker", i))
			}
			return nil
		})
	}

	// wrap query generation logic in a function to ensure queriesCh always closes once
	if err := func() error {
		defer close(queriesCh)

		for _, tbl := range destinationTableNames {
			lastNormBatchIDForTable, err := c.GetLastNormalizedBatchIDForTable(ctx, req.FlowJobName, tbl)
			if err != nil {
				c.logger.Error("[clickhouse] error while getting last synced batch id for table", "table", tbl, slog.Any("error", err))
				return err
			}
			c.logger.Info("[clickhouse] last normalized batch id for table",
				"table", tbl, "lastNormBatchID", lastNormBatchIDForTable, "endBatchID", endBatchID)

			// Skip batches already normalized for this table. This can happen if a previous normalization run partially succeeded.
			lastNormBatchIDForTable = max(lastNormBatchID, lastNormBatchIDForTable)
			if lastNormBatchIDForTable >= endBatchID {
				c.logger.Info("[clickhouse] latest batch already synced to destination, skipping",
					"table", tbl, "lastNormBatchID", lastNormBatchIDForTable, "endBatchID", endBatchID)
				continue
			}

			queryGenerator := NewNormalizeQueryGenerator(
				tbl,
				req.TableNameSchemaMapping,
				req.TableMappings,
				endBatchID,
				lastNormBatchIDForTable,
				enablePrimaryUpdate,
				sourceSchemaAsDestinationColumn,
				req.Env,
				rawTbl,
				c.chVersion,
				c.Config.Cluster != "",
				req.SoftDeleteColName,
				req.Version,
				req.Flags,
			)
			query, err := queryGenerator.BuildQuery(ctx)
			if err != nil {
				c.logger.Error("[clickhouse] error while building insert into select query",
					slog.String("table", tbl),
					slog.Int64("endBatchID", endBatchID),
					slog.Int64("lastNormBatchID", lastNormBatchIDForTable),
					slog.Any("error", err))
				return fmt.Errorf("error while building insert into select query for table %s: %w", tbl, err)
			}

			select {
			case queriesCh <- queryInfo{
				table:           tbl,
				query:           query,
				lastNormBatchID: lastNormBatchIDForTable,
			}:
			case <-errCtx.Done():
				c.logger.Error("[clickhouse] context canceled while inserting data to ClickHouse",
					slog.Any("error", errCtx.Err()),
					slog.Any("cause", context.Cause(errCtx)))
				return context.Cause(errCtx)
			}
		}
		return nil
	}(); err != nil {
		return model.NormalizeResponse{}, err
	}

	if err := group.Wait(); err != nil {
		return model.NormalizeResponse{}, err
	}
	if err := c.UpdateNormalizeBatchID(ctx, req.FlowJobName, endBatchID); err != nil {
		c.logger.Error("[clickhouse] error while updating normalize batch id",
			slog.Int64("batchID", endBatchID), slog.Any("error", err))
		return model.NormalizeResponse{}, err
	}
	return model.NormalizeResponse{
		StartBatchID: lastNormBatchID + 1,
		EndBatchID:   endBatchID,
	}, nil
}

func (c *ClickHouseConnector) getDistinctTableNamesInBatch(
	ctx context.Context,
	flowJobName string,
	endBatchID int64,
	lastNormBatchID int64,
	tableToSchema map[string]*protos.TableSchema,
) ([]string, error) {
	rawTbl := c.GetRawTableName(flowJobName)

	q := fmt.Sprintf(
		"SELECT DISTINCT _peerdb_destination_table_name FROM %s WHERE _peerdb_batch_id>%d AND _peerdb_batch_id<=%d",
		peerdb_clickhouse.QuoteIdentifier(rawTbl), lastNormBatchID, endBatchID)

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
	defer avroFile.Cleanup(ctx)

	if err := avroSyncMethod.CopyStageToDestination(ctx, avroFile); err != nil {
		return fmt.Errorf("failed to copy stage to destination: %w", err)
	}
	return nil
}

func (c *ClickHouseConnector) copyAvroStagesToDestination(
	ctx context.Context, flowJobName string, lastNormBatchID int64, endBatchID int64, env map[string]string, version uint32,
) error {
	// Skip batches already copied to raw table. This can happen if a previous normalization
	// run failed after copying to raw table but before completing normalization.
	lastBatchIDInRawTable, err := c.GetLastBatchIDInRawTable(ctx, flowJobName)
	if err != nil {
		return fmt.Errorf("failed to get last batch id in raw table: %w", err)
	}
	lastCopiedBatchID := max(lastBatchIDInRawTable, lastNormBatchID)
	c.logger.Info("[clickhouse] pushing s3 data to raw table",
		slog.Int64("batchID", lastCopiedBatchID), slog.Int64("endBatchID", endBatchID))

	for batchID := lastCopiedBatchID + 1; batchID <= endBatchID; batchID++ {
		if err := c.copyAvroStageToDestination(ctx, flowJobName, batchID, env, version); err != nil {
			return fmt.Errorf("failed to copy avro stage to destination: %w", err)
		}
		c.logger.Info("[clickhouse] setting last batch id in raw table",
			slog.Int64("batchID", batchID))
		if err := c.SetLastBatchIDInRawTable(ctx, flowJobName, batchID); err != nil {
			c.logger.Error("[clickhouse] error while setting last batch id in raw table",
				slog.Int64("batchID", batchID), slog.Any("error", err))
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
