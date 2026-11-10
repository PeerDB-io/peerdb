package connclickhouse

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	chvalidate "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
)

func engineToString(e protos.TableEngine) (string, error) {
	switch e {
	case protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE:
		return chvalidate.EngineReplacingMergeTree, nil
	case protos.TableEngine_CH_ENGINE_MERGE_TREE:
		return chvalidate.EngineMergeTree, nil
	case protos.TableEngine_CH_ENGINE_NULL:
		return chvalidate.EngineNull, nil
	case protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE:
		return chvalidate.EngineReplicatedReplacingMergeTree, nil
	case protos.TableEngine_CH_ENGINE_REPLICATED_MERGE_TREE:
		return chvalidate.EngineReplicatedMergeTree, nil
	case protos.TableEngine_CH_ENGINE_COALESCING_MERGE_TREE:
		return chvalidate.EngineCoalescingMergeTree, nil
	default:
		return "", fmt.Errorf("unhandled TableEngine proto value: %v", e)
	}
}

func (c *ClickHouseConnector) ValidateMirrorDestination(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) error {
	if err := chvalidate.CheckNotSystemDatabase(c.Config.Database); err != nil {
		return err
	}

	if internal.PeerDBOnlyClickHouseAllowed() {
		err := chvalidate.CheckIfClickHouseCloudHasSharedMergeTreeEnabled(ctx, c.logger, c.database)
		if err != nil {
			return err
		}
	}

	if err := checkNativeJSONSetting(ctx, cfg.Env, c.chVersion); err != nil {
		return err
	}

	destinationTables := make([]string, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		destinationTable := tableMapping.DestinationTableIdentifier
		if cfg.Resync {
			destinationTable = internal.CDCResyncDestinationTable(tableMapping)
		}
		destinationTables = append(destinationTables, destinationTable)
	}
	var extraCount uint64
	if !cfg.Resync && !cfg.InitialSnapshotOnly {
		extraCount = 1 // raw table to be created
	}
	if err := chvalidate.ValidateTableCapacity(
		ctx, c.logger, c.database, destinationTables, extraCount, cfg.Resync,
	); err != nil {
		return err
	}

	// Without source schemas (full mirror resync) there is nothing left to validate, as the tables are created or replaced.
	// A table-level resync passes them to validate the new table settings.
	if cfg.Resync && tableNameSchemaMapping == nil {
		return nil
	}

	// this is for handling column exclusion, processed schema does that in a step
	processedMapping := internal.BuildProcessedSchemaMapping(cfg.TableMappings, tableNameSchemaMapping, c.logger)

	for _, tableMapping := range cfg.TableMappings {
		dstTableName := tableMapping.DestinationTableIdentifier
		if dstTableName == "" {
			return fmt.Errorf("destination table identifier is empty")
		}
		processedSchema, ok := processedMapping[dstTableName]
		if !ok {
			// if destination table is not a key, that means source table was not a key in the original schema mapping(?)
			return fmt.Errorf("source table %s not found in schema mapping", tableMapping.SourceTableIdentifier)
		}

		var sortingKeys []string
		for _, col := range tableMapping.Columns {
			if col.Ordering > 0 {
				sortingKeys = append(sortingKeys, col.SourceName)
			}
		}
		engine, err := engineToString(tableMapping.Engine)
		if err != nil {
			return err
		}
		if err := chvalidate.ValidateOrderingKeys(ctx, c.logger, c.database,
			c.chVersion, tableMapping.SourceTableIdentifier,
			len(processedSchema.PrimaryKeyColumns) > 0,
			sortingKeys, engine,
		); err != nil {
			return err
		}
		if err := chvalidate.ValidateClusterShardingKey(
			c.Config.Cluster,
			tableMapping.ShardingKey,
			tableMapping.SourceTableIdentifier,
			len(processedSchema.PrimaryKeyColumns) > 0,
			sortingKeys,
		); err != nil {
			return err
		}
	}

	// In the case of resync, we don't need to check the content or structure of the original tables;
	// they'll always get swapped out with the _resync tables which we CREATE OR REPLACE
	if cfg.Resync {
		return nil
	}

	peerDBColumns := []string{versionColName}
	if cfg.SoftDeleteColName != "" {
		peerDBColumns = append(peerDBColumns, cfg.SoftDeleteColName)
	} else {
		peerDBColumns = append(peerDBColumns, defaultIsDeletedColName)
	}
	if cfg.SyncedAtColName != "" {
		peerDBColumns = append(peerDBColumns, strings.ToLower(cfg.SyncedAtColName))
	}

	dstTableNames := slices.Collect(maps.Keys(processedMapping))

	// in case of this setting; multiple source tables can be mapped to the same destination table
	// so ignore the check in this case
	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, cfg.Env)
	if err != nil {
		return err
	}

	initialLoadAllowNonEmptyTables, err := internal.PeerDBClickHouseInitialLoadAllowNonEmptyTables(ctx, cfg.Env)
	if err != nil {
		return err
	}

	if !sourceSchemaAsDestinationColumn {
		if err := chvalidate.CheckIfTablesEmptyAndEngine(ctx, c.logger, c.database,
			dstTableNames, cfg.DoInitialSnapshot, internal.PeerDBOnlyClickHouseAllowed(), initialLoadAllowNonEmptyTables,
		); err != nil {
			return err
		}
	}
	// optimization: fetching columns for all tables at once
	chTableColumnsMapping, err := chvalidate.GetTableColumnsMapping(ctx, c.logger, c.database, dstTableNames)
	if err != nil {
		return err
	}

	for _, tableMapping := range cfg.TableMappings {
		dstTableName := tableMapping.DestinationTableIdentifier
		// if destination table does not exist, we're good
		chColumns, ok := chTableColumnsMapping[dstTableName]
		if !ok {
			continue
		}

		if err := c.processTableComparison(dstTableName, processedMapping[dstTableName],
			chColumns, peerDBColumns, tableMapping,
		); err != nil {
			return err
		}
	}
	return nil
}

// checkNativeJSONSetting rejects a mirror that turns PEERDB_CLICKHOUSE_ENABLE_JSON on against a ClickHouse
// without the native JSON type, which would otherwise silently fall back to String columns.
func checkNativeJSONSetting(ctx context.Context, env map[string]string, chVersion *chproto.Version) error {
	enabled, err := internal.PeerDBEnableClickHouseJSON(ctx, env)
	if err != nil {
		return fmt.Errorf("failed to read PEERDB_CLICKHOUSE_ENABLE_JSON: %w", err)
	}
	if enabled && !qvalue.NativeJSONTypeSupported(chVersion) {
		version := "unknown"
		if chVersion != nil {
			version = chVersion.String()
		}
		return fmt.Errorf("PEERDB_CLICKHOUSE_ENABLE_JSON needs ClickHouse 25.3 or newer for the native JSON type, "+
			"the destination runs %s; turn the setting off to map JSON columns to String", version)
	}
	return nil
}
