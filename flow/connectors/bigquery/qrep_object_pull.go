package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// PullQRepObjects pulls QRep objects from GCS based on the provided configuration and partition information.
// It streams the objects to the provided QObjectStream and returns the total number of rows and bytes processed.
// Total number of rows is not known at this time. It will be calculated during sync with an info back from ClickHouse.
func (c *BigQueryConnector) PullQRepObjects(
	ctx context.Context,
	_ shared.CatalogPool,
	_ *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	_ protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QObjectStream,
) (int64, int64, error) {
	stream.SetFormat(model.QObjectStreamBigQueryExportParquetFormat)

	schema, err := c.getQRepSchema(ctx, config)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get QRep schema: %w", err)
	}
	stream.SetSchema(schema)

	if partition == nil || partition.Range == nil {
		return 0, 0, fmt.Errorf("partition and partition range must be provided")
	}

	objectRange := partition.Range.GetObjectIdRange()
	if objectRange == nil {
		return 0, 0, fmt.Errorf("invalid partition range")
	}

	stagingPath, err := parseGCSPath(config.StagingPath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse staging path %s: %w", config.StagingPath, err)
	}
	tablePath := stagingPath.JoinPath(config.WatermarkTable)
	bucketName := tablePath.Bucket()
	prefix := tablePath.QueryPrefix()

	bucket := c.storageClient.Bucket(bucketName)

	headerProvider := NewGCSHeaderProvider(c, bucketName, prefix)
	stream.SetHeaderProvider(headerProvider)

	var totalBytes int64

	processObject := func(attrs *storage.ObjectAttrs) error {
		if err := stream.Send(ctx, &model.Object{
			URL:  fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucketName, url.PathEscape(attrs.Name)),
			Size: attrs.Size,
		}); err != nil {
			return fmt.Errorf("failed to send object to stream: %w", err)
		}

		totalBytes += attrs.Size
		return nil
	}

	startOffset := objectRange.Start
	endOffset := objectRange.End
	if endOffset == startOffset {
		// If startOffset and endOffset are the same,
		// we can assume it's the last partition,
		// and it consists of one object only.

		attrs, err := bucket.Object(startOffset).Attrs(ctx)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to get object attrs for bucket %s with prefix %s and object %s: %w",
				bucketName, prefix, startOffset, err)
		}
		if err := processObject(attrs); err != nil {
			return 0, totalBytes, err
		}
		c.logger.Info("finished pulling single downloadable object",
			slog.String("bucket", bucketName),
			slog.String("prefix", prefix),
			slog.Int64("totalBytes", totalBytes))
		return 0, totalBytes, nil
	}

	// Partition ranges are inclusive on both ends, but GCS EndOffset is exclusive.
	// We must explicitly fetch endOffSet below.
	it := bucket.Objects(ctx, &storage.Query{
		Prefix:      prefix,
		Delimiter:   "/", // to avoid listing "folders"
		StartOffset: objectRange.Start,
		EndOffset:   objectRange.End,
	})

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			c.logger.Debug("finished listing objects in bucket",
				slog.String("bucket", bucketName),
				slog.String("prefix", prefix),
				slog.Int64("totalBytes", totalBytes))
			break
		}
		if err != nil {
			return 0, 0, fmt.Errorf("failed to list objects in bucket %s with prefix %s: %w", bucketName, prefix, err)
		}

		if attrs.Name == "" {
			// ObjectIterator may return empty Name for prefixes if Delimiter is set.
			// We have to skip those. See: https://cloud.google.com/storage/docs/listing-objects#code-samples
			continue
		}

		if err := processObject(attrs); err != nil {
			return 0, totalBytes, err
		}
	}

	endAttrs, err := bucket.Object(endOffset).Attrs(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get end object attrs for bucket %s with prefix %s and object %s: %w",
			bucketName, prefix, endOffset, err)
	}
	if err := processObject(endAttrs); err != nil {
		return 0, totalBytes, err
	}

	c.logger.Info("finished pulling downloadable objects",
		slog.String("bucket", bucketName),
		slog.String("prefix", prefix),
		slog.Int64("totalBytes", totalBytes))

	return 0, totalBytes, nil
}

func (c *BigQueryConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	dsTable, err := c.convertToDatasetTable(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table identifier %s: %w", config.WatermarkTable, err)
	}

	tableRef := c.client.DatasetInProject(c.projectID, dsTable.dataset).Table(dsTable.table)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata for %s.%s: %w", dsTable.dataset, dsTable.table, err)
	}

	totalRows := int64(metadata.NumRows)
	numRowsPerPartition := int64(config.NumRowsPerPartition)

	adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)

	c.logger.Info("[bigquery] partition details",
		slog.Int64("totalRows", totalRows),
		slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
		slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
		slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))

	stagingPath, err := parseGCSPath(config.StagingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse staging path %s: %w", config.StagingPath, err)
	}

	tablePath := stagingPath.JoinPath(config.WatermarkTable)
	bucketName := tablePath.Bucket()
	prefix := tablePath.QueryPrefix()

	bucket := c.storageClient.Bucket(bucketName)

	var startOffset string
	if last.Range != nil {
		objectRange := last.Range.GetObjectIdRange()
		if objectRange == nil {
			return nil, fmt.Errorf("invalid partition range type: %T", last.Range.Range)
		}
		startOffset = objectRange.Start
	}

	it := bucket.Objects(ctx, &storage.Query{
		Prefix:      prefix,
		Delimiter:   "/", // to avoid listing "folders"
		StartOffset: startOffset,
	})

	var partitions []*protos.QRepPartition
	var currentPartition *protos.QRepPartition
	var currentPartitionTotalSize uint64

	var projectedMaximumPartitionSize uint64
	var projectedPartitionSizeCalculated bool

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket %s with prefix %s: %w", bucketName, prefix, err)
		}

		if attrs.Name == "" {
			// ObjectIterator may return empty Name for prefixes if Delimiter is set.
			// We have to skip those. See: https://cloud.google.com/storage/docs/listing-objects#code-samples
			continue
		}

		// we only want the first object after the start offset
		if attrs.Name == startOffset {
			continue
		}

		if !projectedPartitionSizeCalculated {
			rowSize := parquetObjectAverageRowSize(ctx, c.logger, bucket.Object(attrs.Name))
			projectedMaximumPartitionSize = uint64(adjustedPartitions.AdjustedNumRowsPerPartition) * rowSize
			projectedPartitionSizeCalculated = true

			c.logger.Info(
				"[bigquery] estimated parquet object average row size",
				slog.Uint64("size", projectedMaximumPartitionSize),
			)
		}

		if currentPartition == nil {
			currentPartition = &protos.QRepPartition{
				PartitionId: uuid.NewString(),
				Range: &protos.PartitionRange{
					Range: &protos.PartitionRange_ObjectIdRange{
						ObjectIdRange: &protos.ObjectIdPartitionRange{
							Start: attrs.Name,
							End:   attrs.Name,
						},
					},
				},
			}
			currentPartitionTotalSize = 0
		}

		currentPartitionTotalSize += uint64(attrs.Size)
		currentPartition.Range.GetObjectIdRange().End = attrs.Name

		if currentPartitionTotalSize >= projectedMaximumPartitionSize {
			partitions = append(partitions, currentPartition)
			currentPartition = nil
			currentPartitionTotalSize = 0
		}
	}

	if currentPartition != nil {
		partitions = append(partitions, currentPartition)
	}

	return partitions, nil
}

func (c *BigQueryConnector) getQRepSchema(ctx context.Context, config *protos.QRepConfig) (types.QRecordSchema, error) {
	dsTable, err := c.convertToDatasetTable(config.WatermarkTable)
	if err != nil {
		return types.QRecordSchema{}, fmt.Errorf("failed to parse table identifier %s: %w", config.WatermarkTable, err)
	}

	tableRef := c.client.DatasetInProject(c.projectID, dsTable.dataset).Table(dsTable.table)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		return types.QRecordSchema{}, fmt.Errorf("failed to get table metadata for %s.%s: %w", dsTable.dataset, dsTable.table, err)
	}

	return bigQuerySchemaToQRecordSchema(metadata.Schema)
}

func bigQuerySchemaToQRecordSchema(schema bigquery.Schema) (types.QRecordSchema, error) {
	fields := make([]types.QField, 0, len(schema))

	for _, field := range schema {
		qValueKind := BigQueryTypeToQValueKind(field)
		if qValueKind == types.QValueKindInvalid {
			return types.QRecordSchema{}, fmt.Errorf("unsupported BigQuery field type: %s for field %s", field.Type, field.Name)
		}

		qField := types.QField{
			Name:         field.Name,
			Type:         qValueKind,
			OriginalType: string(field.Type),
			Nullable:     !field.Required,
			Precision:    int16(field.Precision),
			Scale:        int16(field.Scale),
		}

		fields = append(fields, qField)
	}

	return types.NewQRecordSchema(fields), nil
}

func (c *BigQueryConnector) EnsurePullability(
	ctx context.Context,
	req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, nil
}

func (c *BigQueryConnector) ExportTxSnapshot(
	ctx context.Context,
	flowName string,
	_ map[string]string,
) (*protos.ExportTxSnapshotOutput, any, error) {
	cfg, err := internal.FetchConfigFromDB(ctx, c.catalogPool, flowName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch flow config from db: %w", err)
	}

	_ = c.LogFlowInfo(ctx, flowName, "Starting snapshot BigQuery export to GCS staging bucket")

	// Make the staging path unique per-run so it can be cleaned up asynchronously on resync.
	activityInfo := activity.GetInfo(ctx)
	basePath, err := parseGCSPath(cfg.SnapshotStagingPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse snapshot staging path %q: %w", cfg.SnapshotStagingPath, err)
	}
	runSnapshotStagingPath := basePath.JoinPath(activityInfo.WorkflowExecution.RunID).String()
	c.logger.Info("Run snapshot staging path", slog.String("path", runSnapshotStagingPath))

	// snapshotTime (T) anchors every table's export below via FOR SYSTEM_TIME AS OF, so all
	// tables are read as of the same consistent point in time. It's queried from BigQuery's
	// own clock rather than local wall-clock so it lines up with BigQuery's time-travel
	// semantics. When the mirror continues into CDC, T also becomes the initial poll
	// checkpoint (see below) so a later PullRecords resumes from exactly where this snapshot
	// left off.
	//
	// Known limitation: T must stay within BigQuery's time-travel window (7 days by default)
	// for the entire snapshot duration across all tables, or later tables' exports will fail.
	// Not addressed here -- no retry/adjustment logic; a snapshot that runs long enough to
	// walk outside the window needs a shorter export or a larger table-level time-travel
	// setting.
	snapshotTime, err := c.currentBigQueryTimestamp(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get current BigQuery timestamp for snapshot: %w", err)
	}

	jobs := make(map[string]*bigquery.Job)
	for _, tm := range cfg.TableMappings {
		exportSQL, err := c.bigQueryExportQueryStatement(ctx, tm.SourceTableIdentifier, runSnapshotStagingPath, snapshotTime)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build export SQL for table %s: %w", tm.SourceTableIdentifier, err)
		}

		q := c.client.Query(exportSQL)
		job, err := q.Run(ctx)
		if err != nil {
			if apiErr, ok := errors.AsType[*googleapi.Error](err); ok {
				if apiErr.Code == 403 {
					_ = c.LogFlowInfo(ctx, flowName, fmt.Sprintf(
						"Permission denied error when starting export job for table %s: %s",
						tm.SourceTableIdentifier,
						apiErr.Message,
					))
				}
			}

			return nil, nil, fmt.Errorf("failed to start export job for table %s: %w", tm.SourceTableIdentifier, err)
		}
		jobs[tm.SourceTableIdentifier] = job
	}
	for sourceTableIdentifier, job := range jobs {
		if status, err := job.Wait(ctx); err != nil {
			return nil, nil, fmt.Errorf("error waiting for export job to complete: %w", err)
		} else if err := status.Err(); err != nil {
			return nil, nil, fmt.Errorf("export job completed with error: %w", err)
		}

		_ = c.LogFlowInfo(ctx, flowName, "Exported snapshot data to GCS for table "+sourceTableIdentifier)
	}

	// Persist T as the initial CDC checkpoint only when this mirror will keep polling for
	// changes after the snapshot. Pure-snapshot flows (InitialSnapshotOnly) have no CDC
	// follow-up, so there's nothing to resume and no checkpoint should be written.
	//
	// This is done here, in ExportTxSnapshot, rather than in SetupReplication (as
	// CDCPullConnectorCore's other implementations do for their equivalent offsets): for
	// BigQuery, SetupReplication and ExportTxSnapshot are called from disjoint branches of
	// SnapshotFlowWorkflow depending on InitialSnapshotOnly, so T (captured above, specific to
	// this export) is never in scope inside SetupReplication. See the workflow branching in
	// flow/workflows/snapshot_flow.go: SetupReplication runs only when InitialSnapshotOnly is
	// false, i.e. exactly when ExportTxSnapshot does *not* run.
	if !cfg.InitialSnapshotOnly {
		checkpoint := model.CdcCheckpoint{Text: snapshotTime.Format(time.RFC3339Nano)}
		if err := c.SetLastOffset(ctx, flowName, checkpoint); err != nil {
			return nil, nil, fmt.Errorf("failed to persist snapshot checkpoint: %w", err)
		}
	}

	return &protos.ExportTxSnapshotOutput{SnapshotStagingPath: runSnapshotStagingPath}, runSnapshotStagingPath, nil
}

// currentBigQueryTimestamp queries BigQuery's own clock (rather than local wall-clock) so the
// returned timestamp is consistent with BigQuery's FOR SYSTEM_TIME AS OF time-travel semantics.
func (c *BigQueryConnector) currentBigQueryTimestamp(ctx context.Context) (time.Time, error) {
	it, err := c.client.Query("SELECT CURRENT_TIMESTAMP() AS ts").Read(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to query current BigQuery timestamp: %w", err)
	}
	var row struct {
		Ts time.Time `bigquery:"ts"`
	}
	if err := it.Next(&row); err != nil {
		return time.Time{}, fmt.Errorf("failed to read current BigQuery timestamp: %w", err)
	}
	return row.Ts, nil
}

// bigQueryExportQueryStatement builds the EXPORT DATA SQL statement for exporting data from BigQuery to GCS in Parquet format.
// BigQuery SDK does not support query_statement overrides for export jobs.
// Parquet export does not support JSON columns, so we need to cast them.
// Therefore, we build a custom EXPORT DATA statement with the necessary casting.
// See: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/export-statements#syntax
func (c *BigQueryConnector) bigQueryExportQueryStatement(
	ctx context.Context,
	sourceTableIdentifier, snapshotStagingPath string,
	snapshotTime time.Time,
) (string, error) {
	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return "", fmt.Errorf("failed to parse table identifier %s: %w", sourceTableIdentifier, err)
	}

	tableRef := c.client.DatasetInProject(c.projectID, dsTable.dataset).Table(dsTable.table)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get table metadata for %s: %w", sourceTableIdentifier, err)
	}

	return buildBigQueryExportSQL(dsTable, metadata.Schema, snapshotStagingPath, sourceTableIdentifier, snapshotTime), nil
}

// buildBigQueryExportSQL builds the EXPORT DATA SQL string given an already-resolved schema, so
// it can be unit tested without a live BigQuery client. See bigQueryExportQueryStatement, which
// resolves the schema and delegates here.
func buildBigQueryExportSQL(
	dsTable datasetTable,
	schema bigquery.Schema,
	snapshotStagingPath, sourceTableIdentifier string,
	snapshotTime time.Time,
) string {
	columnSelects := make([]string, 0, len(schema))
	for _, field := range schema {
		quotedName := quotedIdentifier(field.Name)
		columnSelect := quotedName
		switch field.Type {
		case bigquery.JSONFieldType:
			// Cast JSON to STRING since Parquet doesn't support JSON type
			columnSelect = fmt.Sprintf("TO_JSON_STRING(%s) AS %s", quotedName, quotedName)
		case bigquery.GeographyFieldType:
			// Cast Geography to STRING since Parquet + ClickHouse doesn't support Geography type nicely
			columnSelect = fmt.Sprintf("ST_AsText(%s) AS %s", quotedName, quotedName)
		case bigquery.DateTimeFieldType:
			// Cast DATETIME to TIMESTAMP for Parquet export since BigQuery DATETIME
			// is timezone-unaware and its Parquet representation may not be compatible
			// with ClickHouse. Casting to TIMESTAMP (UTC) preserves the value.
			columnSelect = fmt.Sprintf("CAST(%s AS TIMESTAMP) AS %s", quotedName, quotedName)
		}
		columnSelects = append(columnSelects, columnSelect)
	}

	uri := fmt.Sprintf("%s/%s/*.parquet", snapshotStagingPath, url.PathEscape(sourceTableIdentifier))
	// BigQuery's TIMESTAMP literal format; matches the precision/format validated against a
	// live instance (see the FOR SYSTEM_TIME AS OF syntax docs referenced above).
	snapshotLiteral := snapshotTime.UTC().Format("2006-01-02 15:04:05.999999")

	return fmt.Sprintf(`EXPORT DATA OPTIONS(
			uri='%s',
			format='PARQUET',
			compression='GZIP',
			overwrite=true
		) AS
		SELECT %s FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP('%s UTC')`,
		uri,
		strings.Join(columnSelects, ", "),
		dsTable.stringQuoted(),
		snapshotLiteral,
	)
}

func (c *BigQueryConnector) FinishExport(v any) error {
	if v == nil {
		return nil
	}

	sv, ok := v.(string)
	if !ok || sv == "" {
		c.logger.Info("no staging path to cleanup")
		return nil
	}

	ctx := context.Background()
	stagingPath, err := parseGCSPath(sv)
	if err != nil {
		return fmt.Errorf("failed to parse staging path %s: %w", sv, err)
	}

	bucketName := stagingPath.Bucket()
	prefix := stagingPath.QueryPrefix()

	c.logger.Info("cleaning up GCS staging path after export",
		slog.String("bucket", bucketName),
		slog.String("prefix", prefix))

	bucket := c.storageClient.Bucket(bucketName)
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})

	deletedCount := 0
	failedCount := 0
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects in GCS bucket %s with prefix %s: %w", bucketName, prefix, err)
		}

		obj := bucket.Object(attrs.Name)
		if err := obj.Delete(ctx); err != nil {
			c.logger.Warn("failed to delete GCS object",
				slog.String("object", attrs.Name),
				slog.Any("error", err))
			// Continue with other objects even if one fails
			failedCount++
		} else {
			deletedCount++
		}
	}

	if failedCount > 0 {
		return fmt.Errorf("failed to delete %d objects in GCS bucket %s with prefix %s", failedCount, bucketName, prefix)
	}

	c.logger.Info("GCS cleanup completed after export",
		slog.Int("deletedObjects", deletedCount))

	return nil
}

func (c *BigQueryConnector) SetupReplConn(context.Context, map[string]string) error {
	return nil
}

func (c *BigQueryConnector) UpdateReplStateLastOffset(context.Context, model.CdcCheckpoint) error {
	return nil
}

func (c *BigQueryConnector) PullFlowCleanup(context.Context, string) error {
	return nil
}

func (c *BigQueryConnector) SetupReplication(
	context.Context, shared.CatalogPool,
	*protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	return model.SetupReplicationResult{}, nil
}
