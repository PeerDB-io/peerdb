package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	bqvalidate "github.com/PeerDB-io/peerdb/flow/pkg/bigquery"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func (c *BigQueryConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	var missingTables []common.QualifiedTable
	dstDatasetTables := make(map[string]datasetTable, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		dstDatasetTable, err := c.convertToDatasetTable(tableMapping.SourceTableIdentifier)
		if err != nil {
			return err
		}
		dstDatasetTables[tableMapping.SourceTableIdentifier] = dstDatasetTable

		table := c.client.DatasetInProject(c.projectID, dstDatasetTable.dataset).Table(dstDatasetTable.table)

		if _, err := table.Metadata(ctx); err != nil {
			if c.isApiErrorWithStatusCode(err, http.StatusNotFound) {
				missingTables = append(missingTables, common.QualifiedTable{
					Namespace: dstDatasetTable.dataset,
					Table:     dstDatasetTable.table,
				})
				continue
			}
			return fmt.Errorf("failed to get metadata for table %s: %w", tableMapping.DestinationTableIdentifier, err)
		}
	}
	if len(missingTables) > 0 {
		return common.NewSourceTablesMissingError(missingTables)
	}

	if cfg.SnapshotStagingPath == "" {
		return fmt.Errorf("snapshot bucket is required for BigQuery source connector")
	}

	stagingPath, err := parseGCSPath(cfg.SnapshotStagingPath)
	if err != nil {
		return fmt.Errorf("invalid snapshot bucket: %w", err)
	}

	bucket := c.storageClient.Bucket(stagingPath.Bucket())

	it := bucket.Objects(ctx, &storage.Query{Prefix: stagingPath.QueryPrefix()})
	_, err = it.Next()
	if err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("failed to access staging bucket: %w", exceptions.NewBigQueryError(err))
	}

	// snapshot-only mirrors never touch CDC, so no need to validate mode-specific requirements below
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	replicationMode := cfg.GetBigqueryCdcConfig().GetReplicationMode()
	if replicationMode == protos.BigQueryReplicationMode_BIGQUERY_REPLICATION_MODE_QUERY {
		for _, tableMapping := range cfg.TableMappings {
			dstDatasetTable := dstDatasetTables[tableMapping.SourceTableIdentifier]
			watermarkColumn := tableMapping.GetWatermarkColumn()
			if watermarkColumn == "" {
				return fmt.Errorf("table %s has no watermark_column configured; QUERY replication mode requires "+
					"one TIMESTAMP column per table to incrementally scan", dstDatasetTable.string())
			}

			table := c.client.DatasetInProject(c.projectID, dstDatasetTable.dataset).Table(dstDatasetTable.table)
			metadata, err := table.Metadata(ctx)
			if err != nil {
				return fmt.Errorf("failed to get metadata for table %s: %w", tableMapping.SourceTableIdentifier, err)
			}

			field := bigQueryFieldByName(metadata.Schema, watermarkColumn)
			if field == nil {
				return fmt.Errorf("watermark column %s does not exist on table %s", watermarkColumn, dstDatasetTable.string())
			}
			if field.Type != bigquery.TimestampFieldType {
				return fmt.Errorf("watermark column %s on table %s must be TIMESTAMP, got %s",
					watermarkColumn, dstDatasetTable.string(), field.Type)
			}
		}
		return nil
	}

	needsChangeHistory := false
	for _, tableMapping := range cfg.TableMappings {
		if tableMapping.GetBigqueryCdcEventsFunction() == protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_CHANGES {
			needsChangeHistory = true
			break
		}
	}
	var changeHistoryByTable map[bqvalidate.DatasetTable]bool
	if needsChangeHistory {
		allDstDatasetTables := make([]bqvalidate.DatasetTable, 0, len(dstDatasetTables))
		for _, dstDatasetTable := range dstDatasetTables {
			allDstDatasetTables = append(allDstDatasetTables,
				bqvalidate.DatasetTable{Dataset: dstDatasetTable.dataset, Table: dstDatasetTable.table})
		}
		changeHistoryByTable, err = bqvalidate.TablesHaveChangeHistoryEnabled(ctx, c.client, c.projectID, allDstDatasetTables)
		if err != nil {
			return fmt.Errorf("failed to check enable_change_history option: %w", exceptions.NewBigQueryError(err))
		}
	}

	for _, tableMapping := range cfg.TableMappings {
		dstDatasetTable := dstDatasetTables[tableMapping.SourceTableIdentifier]
		table := c.client.DatasetInProject(c.projectID, dstDatasetTable.dataset).Table(dstDatasetTable.table)
		metadata, err := table.Metadata(ctx)
		if err != nil {
			return fmt.Errorf("failed to get metadata for table %s: %w", tableMapping.SourceTableIdentifier, err)
		}
		hasPK := tableHasPrimaryKey(metadata)
		destinationHasOrderingKey := hasPK || tableHasOrderingKey(tableMapping)

		switch tableMapping.GetBigqueryCdcEventsFunction() {
		case protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_CHANGES:
			if !destinationHasOrderingKey {
				return fmt.Errorf("table %s has no primary key constraint configured on BigQuery; "+
					"CHANGES mode requires either a real (NOT ENFORCED) PK constraint on the source table "+
					"or an explicit ordering key configured via column settings on the table mapping",
					dstDatasetTable.string())
			}
			key := bqvalidate.DatasetTable{Dataset: dstDatasetTable.dataset, Table: dstDatasetTable.table}
			if !changeHistoryByTable[key] {
				return fmt.Errorf("table %s does not have enable_change_history=TRUE set; "+
					"CHANGES mode requires it (run ALTER TABLE ... SET OPTIONS(enable_change_history=true) on the source table)",
					dstDatasetTable.string())
			}
		case protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS:
			if (tableMapping.Engine == protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE ||
				tableMapping.Engine == protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE) && !destinationHasOrderingKey {
				return fmt.Errorf("table %s has no primary key configured on BigQuery and no ordering key configured "+
					"via column settings, and the destination engine is a ReplacingMergeTree variant (the plain form "+
					"is the default); ORDER BY tuple() on a keyless ReplacingMergeTree collapses the table on writes, "+
					"so such a table must use an explicit CH_ENGINE_MERGE_TREE engine instead, or have an ordering key "+
					"configured",
					dstDatasetTable.string())
			}
		}
	}

	return nil
}

// bigQueryFieldByName returns the schema field named name, or nil if absent.
func bigQueryFieldByName(schema bigquery.Schema, name string) *bigquery.FieldSchema {
	for _, field := range schema {
		if field.Name == name {
			return field
		}
	}
	return nil
}

// tableHasPrimaryKey reports whether metadata carries a real (BigQuery PKs are
// always NOT ENFORCED) primary key constraint.
func tableHasPrimaryKey(metadata *bigquery.TableMetadata) bool {
	return metadata.TableConstraints != nil &&
		metadata.TableConstraints.PrimaryKey != nil &&
		len(metadata.TableConstraints.PrimaryKey.Columns) > 0
}

// tableHasOrderingKey reports whether the user configured an explicit ordering
// key on the table mapping (column ordering > 0) as a PK substitute.
func tableHasOrderingKey(tableMapping *protos.TableMapping) bool {
	for _, col := range tableMapping.Columns {
		if col.Ordering > 0 {
			return true
		}
	}
	return false
}
