package connbigquery

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *BigQueryConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	if !cfg.InitialSnapshotOnly || !cfg.DoInitialSnapshot {
		return errors.New("BigQuery source connector only supports initial snapshot flows. CDC is not supported")
	}

	for _, tableMapping := range cfg.TableMappings {
		dstDatasetTable, err := c.convertToDatasetTable(tableMapping.SourceTableIdentifier)
		if err != nil {
			return err
		}

		table := c.client.DatasetInProject(c.projectID, dstDatasetTable.dataset).Table(dstDatasetTable.table)

		if _, err := table.Metadata(ctx); err != nil {
			return fmt.Errorf("failed to get metadata for table %s: %w", tableMapping.DestinationTableIdentifier, err)
		}
	}

	if cfg.SnapshotStagingPath == "" {
		return errors.New("snapshot bucket is required for BigQuery source connector")
	}

	stagingPath, err := parseGCSPath(cfg.SnapshotStagingPath)
	if err != nil {
		return fmt.Errorf("invalid snapshot bucket: %w", err)
	}

	bucket := c.storageClient.Bucket(stagingPath.Bucket())

	it := bucket.Objects(ctx, &storage.Query{Prefix: stagingPath.QueryPrefix()})
	_, err = it.Next()
	if err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("failed to access staging bucket: %w", err)
	}

	return nil
}
