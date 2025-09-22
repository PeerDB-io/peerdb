package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

// PullQRepObjects pulls QRep objects from GCS based on the provided configuration and partition information.
// It streams the objects to the provided QObjectStream and returns the total number of rows and bytes processed.
// Total number of rows is estimated based on the size of the first object and the average row size.
func (c *BigQueryConnector) PullQRepObjects(ctx context.Context, _ *otel_metrics.OtelManager, config *protos.QRepConfig, partition *protos.QRepPartition, stream *model.QObjectStream) (int64, int64, error) {
	defer close(stream.Objects)

	stream.SetFormat("Avro")

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
	bucketName := stagingPath.Bucket()
	prefix := stagingPath.QueryPrefix()

	bucket := c.storageClient.Bucket(bucketName)

	var token *auth.Token
	urlHeaders := make(http.Header)

	var totalBytes int64
	var totalRows int64

	var projectedRowSizeHasBeenCalculated bool
	var projectedRowSize uint64

	processObject := func(attrs *storage.ObjectAttrs) error {
		if token == nil || !token.IsValid() {
			token, err = c.storageDownScopedToken(ctx, bucketName, prefix)
			if err != nil {
				return fmt.Errorf("failed to get downscoped token for bucket %s with prefix %s: %w", bucketName, prefix, err)
			}

			urlHeaders.Set("Authorization", "Bearer "+token.Value)
		}

		stream.Objects <- &model.Object{
			URL:     fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucketName, url.PathEscape(attrs.Name)),
			Size:    attrs.Size,
			Headers: urlHeaders,
		}

		if !projectedRowSizeHasBeenCalculated {
			// estimate the row size based on the first object
			projectedRowSize = avroObjectAverageRowSize(ctx, c.logger, bucket.Object(attrs.Name))
			c.logger.Info("projected Avro row size", slog.Uint64("bytes", projectedRowSize))

			projectedRowSizeHasBeenCalculated = true
		}

		totalBytes += attrs.Size

		if projectedRowSize > 0 {
			totalRows += int64(float64(attrs.Size) / float64(projectedRowSize))
		}

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
			return 0, 0, err
		}
		c.logger.Info("finished pulling single downloadable object",
			slog.String("bucket", bucketName),
			slog.String("prefix", prefix),
			slog.Int64("totalRows", totalRows),
			slog.Int64("totalBytes", totalBytes))
		return totalRows, totalBytes, nil
	}

	it := bucket.Objects(ctx, &storage.Query{
		Prefix:      prefix,
		Delimiter:   "/",
		StartOffset: objectRange.Start,
		EndOffset:   objectRange.End,
	})

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			c.logger.Debug("finished listing objects in bucket",
				slog.String("bucket", bucketName),
				slog.String("prefix", prefix),
				slog.Int64("totalRows", totalRows),
				slog.Int64("totalBytes", totalBytes))
			break
		}
		if err != nil {
			return 0, 0, fmt.Errorf("failed to list objects in bucket %s with prefix %s: %w", bucketName, prefix, err)
		}

		if err := processObject(attrs); err != nil {
			return 0, 0, err
		}
	}

	c.logger.Info("finished pulling downloadable objects",
		slog.String("bucket", bucketName),
		slog.String("prefix", prefix),
		slog.Int64("totalRows", totalRows),
		slog.Int64("totalBytes", totalBytes))

	return totalRows, totalBytes, nil
}

func (c *BigQueryConnector) GetQRepPartitions(ctx context.Context, config *protos.QRepConfig, last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
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
		Delimiter:   "/",
		StartOffset: startOffset,
	})

	var partitions []*protos.QRepPartition
	var currentPartition *protos.QRepPartition
	var currentPartitionTotalSize uint64

	var projectedMaximumPartitionSize *uint64

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket %s with prefix %s: %w", bucketName, prefix, err)
		}

		// we only want the first object after the start offset
		if attrs.Name == startOffset {
			continue
		}

		if projectedMaximumPartitionSize == nil {
			rowSize := avroObjectAverageRowSize(ctx, c.logger, bucket.Object(attrs.Name))
			partitionSize := uint64(config.NumRowsPerPartition) * rowSize
			projectedMaximumPartitionSize = &partitionSize

			c.logger.Info("estimated avro object average row size", slog.Uint64("size", partitionSize))
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

		if currentPartitionTotalSize >= *projectedMaximumPartitionSize {
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
			return types.QRecordSchema{}, fmt.Errorf("unsupported BigQuery field type: %s for field %s", field.Type, field.Name) // todo: should we fail?
		}

		qField := types.QField{
			Name:      field.Name,
			Type:      qValueKind,
			Nullable:  !field.Required,
			Precision: int16(field.Precision),
			Scale:     int16(field.Scale),
		}

		fields = append(fields, qField)
	}

	return types.NewQRecordSchema(fields), nil
}
