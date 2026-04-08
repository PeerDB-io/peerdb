package connclickhouse

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
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

	if cfg.Resync {
		return nil // no need to validate schema for resync, as we will create or replace the tables
	}

	peerDBColumns := []string{isDeletedColName, versionColName}
	if cfg.SyncedAtColName != "" {
		peerDBColumns = append(peerDBColumns, strings.ToLower(cfg.SyncedAtColName))
	}

	// this is for handling column exclusion, processed schema does that in a step
	processedMapping := internal.BuildProcessedSchemaMapping(cfg.TableMappings, tableNameSchemaMapping, c.logger)
	dstTableNames := slices.Collect(maps.Keys(processedMapping))

	// In the case of resync, we don't need to check the content or structure of the original tables;
	// they'll always get swapped out with the _resync tables which we CREATE OR REPLACE
	// also in case of this setting; multiple source tables can be mapped to the same destination table
	// so ignore the check in this case as well
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

		// if destination table does not exist, we're good
		if _, ok := chTableColumnsMapping[dstTableName]; !ok {
			continue
		}

		// for resync, we don't need to check the content or structure of the original tables;
		// they'll anyways get swapped out with the _resync tables which we CREATE OR REPLACE
		if err := c.processTableComparison(dstTableName, processedMapping[dstTableName],
			chTableColumnsMapping[dstTableName], peerDBColumns, tableMapping,
		); err != nil {
			return err
		}
	}
	return nil
}
