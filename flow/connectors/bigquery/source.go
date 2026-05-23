package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func (c *BigQueryConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	if !cfg.InitialSnapshotOnly || !cfg.DoInitialSnapshot {
		return fmt.Errorf("BigQuery source connector only supports initial snapshot flows. CDC is not supported")
	}

	var missingTables []common.QualifiedTable
	for _, tableMapping := range cfg.TableMappings {
		dstDatasetTable, err := c.convertToDatasetTable(tableMapping.SourceTableIdentifier)
		if err != nil {
			return err
		}

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

	return nil
}
