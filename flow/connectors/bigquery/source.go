package connbigquery

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	bqvalidate "github.com/PeerDB-io/peerdb/flow/pkg/bigquery"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func (c *BigQueryConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	snapshotOnly := cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly

	// CDC-only mirrors (no initial snapshot) never stage data in GCS, so skip
	// the staging bucket requirement entirely.
	if cfg.DoInitialSnapshot {
		if cfg.SnapshotStagingPath == "" {
			return fmt.Errorf("snapshot bucket is required for BigQuery source connector")
		}

		stagingPath, err := parseGCSPath(cfg.SnapshotStagingPath)
		if err != nil {
			return fmt.Errorf("invalid snapshot bucket: %w", err)
		}

		bucket := c.storageClient.Bucket(stagingPath.Bucket())

		it := bucket.Objects(ctx, &storage.Query{Prefix: stagingPath.QueryPrefix()})
		if _, err := it.Next(); err != nil && !errors.Is(err, iterator.Done) {
			return fmt.Errorf("failed to access staging bucket: %w", exceptions.NewBigQueryError(err))
		}
	}

	sourceConfig := bqvalidate.SourceConfig{
		Client:         c.client,
		ProjectID:      c.projectID,
		DefaultDataset: c.datasetID,
		SnapshotOnly:   snapshotOnly,
	}
	if !snapshotOnly {
		switch cfg.GetBigqueryCdcConfig().GetReplicationMode() {
		case protos.BigQueryReplicationMode_BIGQUERY_REPLICATION_MODE_QUERY:
			sourceConfig.ReplicationMode = bqvalidate.ReplicationModeQuery
		case protos.BigQueryReplicationMode_BIGQUERY_REPLICATION_MODE_EVENTS:
			sourceConfig.ReplicationMode = bqvalidate.ReplicationModeEvents
		default:
			return fmt.Errorf("invalid replication mode: %v", cfg.GetBigqueryCdcConfig().GetReplicationMode())
		}
	}

	sourceConfig.Tables = make([]bqvalidate.SourceTableConfig, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		t := bqvalidate.SourceTableConfig{
			SourceTableIdentifier: tableMapping.SourceTableIdentifier,
			WatermarkColumn:       tableMapping.QueryCdcWatermarkColumn,
			Exclude:               tableMapping.Exclude,
			HasOrderingKey:        tableHasOrderingKey(tableMapping),
			RequiresOrderingKey: tableMapping.Engine == protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE ||
				tableMapping.Engine == protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE,
		}
		if sourceConfig.ReplicationMode == bqvalidate.ReplicationModeEvents {
			switch tableMapping.GetBigqueryCdcEventsFunction() {
			case protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS:
				t.CDCEventsFunction = bqvalidate.CDCEventsFunctionAppends
			case protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_CHANGES:
				t.CDCEventsFunction = bqvalidate.CDCEventsFunctionChanges
			}
		}
		sourceConfig.Tables = append(sourceConfig.Tables, t)
	}

	if _, err := bqvalidate.ValidateSource(ctx, sourceConfig); err != nil {
		return wrapExternalError(err)
	}

	return nil
}

// wrapExternalError converts a bqvalidate.ExternalError (a failed BigQuery
// API call, as opposed to a mirror configuration problem) into an
// exceptions.BigQueryError so the alerting classifier can recognize it.
func wrapExternalError(err error) error {
	if _, ok := errors.AsType[*bqvalidate.ExternalError](err); ok {
		return exceptions.NewBigQueryError(err)
	}
	return err
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
