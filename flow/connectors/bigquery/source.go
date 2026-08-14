package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
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

	cdcMode := cfg.GetBigqueryCdcConfig().GetCdcMode()
	for _, tableMapping := range cfg.TableMappings {
		dstDatasetTable := dstDatasetTables[tableMapping.SourceTableIdentifier]
		table := c.client.DatasetInProject(c.projectID, dstDatasetTable.dataset).Table(dstDatasetTable.table)
		metadata, err := table.Metadata(ctx)
		if err != nil {
			return fmt.Errorf("failed to get metadata for table %s: %w", tableMapping.SourceTableIdentifier, err)
		}
		hasPK := tableHasPrimaryKey(metadata)

		switch cdcMode {
		case protos.BigqueryCdcMode_BIGQUERY_CDC_MODE_CHANGES:
			if !hasPK {
				return fmt.Errorf("table %s has no primary key constraint configured on BigQuery; "+
					"CHANGES mode requires a real (NOT ENFORCED) PK constraint on the source table",
					dstDatasetTable.string())
			}
			hasChangeHistory, err := c.tableHasChangeHistoryEnabled(ctx, dstDatasetTable)
			if err != nil {
				return fmt.Errorf("failed to check enable_change_history option for table %s: %w",
					dstDatasetTable.string(), exceptions.NewBigQueryError(err))
			}
			if !hasChangeHistory {
				return fmt.Errorf("table %s does not have enable_change_history=TRUE set; "+
					"CHANGES mode requires it (run ALTER TABLE ... SET OPTIONS(enable_change_history=true) on the source table)",
					dstDatasetTable.string())
			}
		case protos.BigqueryCdcMode_BIGQUERY_CDC_MODE_APPENDS:
			if rejectKeylessReplacingMergeTree(hasPK, tableMapping.Engine) {
				return fmt.Errorf("table %s has no primary key configured on BigQuery and the destination engine "+
					"is a ReplacingMergeTree variant (the plain form is the default); ORDER BY tuple() on a keyless "+
					"ReplacingMergeTree collapses the table on writes, so a PK-less table must use an explicit "+
					"CH_ENGINE_MERGE_TREE engine instead",
					dstDatasetTable.string())
			}
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

// rejectKeylessReplacingMergeTree decides whether a keyless-table mirror must be
// rejected: ORDER BY tuple() on a keyless ReplacingMergeTree collapses the table
// on writes (errors on ClickHouse 25.12+, silently loses data before), so a
// table with no PK to key off of must use an explicit MergeTree engine instead.
// The replicated variant shares the same collapsing behavior (it's the same
// dedup engine, just wrapped for replication - see the ClickHouse connector's
// own normalize.go, which groups the two under one switch case) so it's
// rejected too.
func rejectKeylessReplacingMergeTree(hasPK bool, engine protos.TableEngine) bool {
	return !hasPK && (engine == protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE ||
		engine == protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE)
}

// tableHasChangeHistoryEnabled checks the enable_change_history table option via
// INFORMATION_SCHEMA.TABLE_OPTIONS. This option is not exposed as a typed field
// on bigquery.TableMetadata, and PeerDB never sets it — only checks it.
func (c *BigQueryConnector) tableHasChangeHistoryEnabled(ctx context.Context, dstDatasetTable datasetTable) (bool, error) {
	q := c.client.Query(fmt.Sprintf(
		"SELECT option_value FROM `%s`.INFORMATION_SCHEMA.TABLE_OPTIONS "+
			"WHERE table_name = @table_name AND option_name = 'enable_change_history'",
		dstDatasetTable.dataset))
	q.Parameters = []bigquery.QueryParameter{{Name: "table_name", Value: dstDatasetTable.table}}
	q.DefaultProjectID = c.projectID
	q.DefaultDatasetID = dstDatasetTable.dataset

	it, err := q.Read(ctx)
	if err != nil {
		return false, err
	}

	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		if errors.Is(err, iterator.Done) {
			return false, nil
		}
		return false, err
	}
	if len(row) == 0 {
		return false, nil
	}

	optionValue, ok := row[0].(string)
	return ok && strings.EqualFold(optionValue, "true"), nil
}
