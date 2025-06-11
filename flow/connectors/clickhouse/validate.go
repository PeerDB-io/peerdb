package connclickhouse

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	chvalidate "github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
)

func (c *ClickHouseConnector) ValidateMirrorDestination(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigs,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) error {
	if internal.PeerDBOnlyClickHouseAllowed() {
		err := chvalidate.CheckIfClickHouseCloudHasSharedMergeTreeEnabled(ctx, c.logger, c.database)
		if err != nil {
			return err
		}
	}

	peerDBColumns := []string{signColName, versionColName}
	if cfg.SyncedAtColName != "" {
		peerDBColumns = append(peerDBColumns, strings.ToLower(cfg.SyncedAtColName))
	}
	// this is for handling column exclusion, processed schema does that in a step
	processedMapping := internal.BuildProcessedSchemaMapping(cfg.TableMappings, tableNameSchemaMapping, c.logger)
	dstTableNames := slices.Collect(maps.Keys(processedMapping))

	// In the case of resync, we don't need to check the content or structure of the original tables;
	// they'll anyways get swapped out with the _resync tables which we CREATE OR REPLACE
	// also in case of this setting; multiple source tables can be mapped to the same destination table
	// so ignore the check in this case as well
	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, cfg.Env)
	if err != nil {
		return err
	}

	if !(cfg.Resync || sourceSchemaAsDestinationColumn) {
		if err := chvalidate.CheckIfTablesEmptyAndEngine(ctx, c.logger, c.database,
			dstTableNames, cfg.DoInitialSnapshot, internal.PeerDBOnlyClickHouseAllowed()); err != nil {
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
		if _, ok := processedMapping[dstTableName]; !ok {
			// if destination table is not a key, that means source table was not a key in the original schema mapping(?)
			return fmt.Errorf("source table %s not found in schema mapping", tableMapping.SourceTableIdentifier)
		}
		// if destination table does not exist, we're good
		if _, ok := chTableColumnsMapping[dstTableName]; !ok {
			continue
		}

		if !cfg.Resync {
			// for resync, we don't need to check the content or structure of the original tables;
			// they'll anyways get swapped out with the _resync tables which we CREATE OR REPLACE
			if err := c.processTableComparison(dstTableName, processedMapping[dstTableName],
				chTableColumnsMapping[dstTableName], peerDBColumns, tableMapping,
			); err != nil {
				return err
			}
		}
	}
	return nil
}
