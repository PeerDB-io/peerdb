package connmongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MongoConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	fullTablePartition := []*protos.QRepPartition{
		{
			PartitionId:        utils.FullTablePartitionID,
			Range:              nil,
			FullTablePartition: true,
		},
	}

	if config.WatermarkColumn == "" {
		return fullTablePartition, nil
	}

	if config.NumRowsPerPartition <= 0 {
		return nil, errors.New("num rows per partition must be greater than 0")
	} else if last != nil && last.Range != nil {
		return nil, fmt.Errorf("last partition is not supported for MongoDB connector, got: %v", last)
	}

	numRowsPerPartition := int64(config.NumRowsPerPartition)
	parseWatermarkTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("unable to parse watermark table: %w", err)
	}
	collection := c.client.Database(parseWatermarkTable.Namespace).Collection(parseWatermarkTable.Table)

	c.logger.Info("[mongo] fetching count of documents for partitioning",
		slog.String("watermark_table", config.WatermarkTable))
	// estimated, worst case we are off by a few documents but should be fine for partitioning
	totalRows, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to count documents in collection %s: %w", parseWatermarkTable.Table, err)
	}
	if totalRows == 0 {
		return []*protos.QRepPartition{}, nil
	}

	// Calculate the number of partitions
	adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
	c.logger.Info("[mongo] partition details",
		slog.Int64("totalRows", totalRows),
		slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
		slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
		slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))

	// no need to bother with bucketAuto if we have only one partition
	if adjustedPartitions.AdjustedNumPartitions == 1 {
		return fullTablePartition, nil
	}

	if config.WatermarkColumn != DefaultDocumentKeyColumnName {
		return nil, fmt.Errorf("only %s is currently supported as watermark column for MongoDB connector", DefaultDocumentKeyColumnName)
	}

	// Use bucketAuto to create partitions based on _id field
	bucketAutoPipeline := []bson.D{
		{
			{Key: "$bucketAuto", Value: bson.D{
				{Key: "groupBy", Value: "$" + config.WatermarkColumn},
				{Key: "buckets", Value: adjustedPartitions.AdjustedNumPartitions},
			}},
		},
	}

	cursor, err := collection.Aggregate(ctx, bucketAutoPipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate for bucket partitions: %w", err)
	}
	defer cursor.Close(ctx)

	partitions := make([]*protos.QRepPartition, 0, adjustedPartitions.AdjustedNumPartitions)
	for cursor.Next(ctx) {
		var bucket struct {
			ID struct {
				Min bson.ObjectID `bson:"min"`
				Max bson.ObjectID `bson:"max"`
			} `bson:"_id"`
		}
		if err := cursor.Decode(&bucket); err != nil {
			return nil, fmt.Errorf("failed to decode bucket: %w", err)
		}

		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_ObjectIdRange{
					ObjectIdRange: &protos.ObjectIdPartitionRange{
						Start: bucket.ID.Min.Hex(),
						End:   bucket.ID.Max.Hex(),
					},
				},
			},
			FullTablePartition: false,
		})
	}
	if err := cursor.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			c.logger.Warn("context canceled while performing bucketAuto aggregation",
				slog.String("watermark_table", config.WatermarkTable))
		} else {
			c.logger.Error("error while performing bucketAuto aggregation",
				slog.String("watermark_table", config.WatermarkTable),
				slog.String("error", err.Error()))
		}
		return nil, fmt.Errorf("cursor error during bucketAuto aggregation: %w", err)
	}

	return partitions, nil
}

func (c *MongoConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	return &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: nil,
	}, nil
}

func (c *MongoConnector) PullQRepRecords(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	var totalRecords int64

	parseWatermarkTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to parse watermark table: %w", err)
	}
	collection := c.client.Database(parseWatermarkTable.Namespace).Collection(parseWatermarkTable.Table)

	stream.SetSchema(GetDefaultSchema(config.Version))

	c.totalBytesRead.Store(0)
	c.deltaBytesRead.Store(0)
	shutDown := common.Interval(ctx, time.Minute, func() {
		read := c.deltaBytesRead.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
	})
	defer shutDown()

	filter := bson.D{}
	if !partition.FullTablePartition {
		filter, err = toRangeFilter(config.WatermarkColumn, partition.Range)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert partition range to filter: %w", err)
		}
	}
	c.logger.Info("[mongo] filter for partition",
		slog.Any("partition", partition.PartitionId),
		slog.String("watermark_table", config.WatermarkTable),
		slog.Any("filter", filter))

	batchSize := config.NumRowsPerPartition
	if config.NumRowsPerPartition == 0 || config.NumRowsPerPartition > math.MaxInt32 {
		batchSize = math.MaxInt32
	}

	c.logger.Info("[mongo] pulling records start")

	// MongoDb will use the lesser of batchSize and 16MiB
	// https://www.mongodb.com/docs/manual/reference/method/cursor.batchsize/
	cursor, err := collection.Find(ctx, filter, options.Find().SetBatchSize(int32(batchSize)))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query for records: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return 0, 0, fmt.Errorf("failed to decode record: %w", err)
		}

		record, err := QValuesFromDocument(doc)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert record: %w", err)
		}
		stream.Records <- record
		totalRecords += 1
		if totalRecords%50000 == 0 {
			c.logger.Info("[mongo] pulling records",
				slog.Int64("records", totalRecords),
				slog.Int64("bytes", c.totalBytesRead.Load()),
				slog.Int("channelLen", len(stream.Records)))
		}
	}
	close(stream.Records)
	if err := cursor.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			c.logger.Warn("context canceled while reading documents",
				slog.Any("partition", partition.PartitionId),
				slog.String("watermark_table", config.WatermarkTable))
		} else {
			c.logger.Error("error while reading documents",
				slog.Any("partition", partition.PartitionId),
				slog.String("watermark_table", config.WatermarkTable),
				slog.String("error", err.Error()))
		}
		return 0, 0, fmt.Errorf("cursor error: %w", err)
	}

	c.logger.Info("[mongo] pulled records",
		slog.Int64("records", totalRecords),
		slog.Int64("bytes", c.totalBytesRead.Load()),
		slog.Int("channelLen", len(stream.Records)))
	return totalRecords, c.deltaBytesRead.Swap(0), nil
}

func GetDefaultSchema(internalVersion uint32) types.QRecordSchema {
	fullDocumentColumnName := DefaultFullDocumentColumnName
	if internalVersion < shared.InternalVersion_MongoDBFullDocumentColumnToDoc {
		fullDocumentColumnName = LegacyFullDocumentColumnName
	}
	schema := make([]types.QField, 0, 2)
	schema = append(schema,
		types.QField{
			Name:     DefaultDocumentKeyColumnName,
			Type:     types.QValueKindString,
			Nullable: false,
		},
		types.QField{
			Name:     fullDocumentColumnName,
			Type:     types.QValueKindJSON,
			Nullable: false,
		})
	return types.QRecordSchema{Fields: schema}
}

// with $bucketAuto, buckets except the last bucket treat their max value as exclusive
// we can't tell what bucket is the "last" bucket without additional tracking, so we accept bounday records being inserted twice
func toRangeFilter(watermarkColumn string, partitionRange *protos.PartitionRange) (bson.D, error) {
	switch r := partitionRange.Range.(type) {
	case *protos.PartitionRange_ObjectIdRange:
		startObjectID, err := bson.ObjectIDFromHex(r.ObjectIdRange.Start)
		if err != nil {
			return nil, fmt.Errorf("invalid start ObjectId %s: %w", r.ObjectIdRange.Start, err)
		}
		endObjectID, err := bson.ObjectIDFromHex(r.ObjectIdRange.End)
		if err != nil {
			return nil, fmt.Errorf("invalid end ObjectId %s: %w", r.ObjectIdRange.End, err)
		}

		return bson.D{
			bson.E{Key: watermarkColumn, Value: bson.D{
				bson.E{Key: "$gte", Value: startObjectID},
				bson.E{Key: "$lte", Value: endObjectID},
			}},
		}, nil
	default:
		return nil, errors.New("unsupported partition range type")
	}
}

func QValuesFromDocument(doc bson.D) ([]types.QValue, error) {
	var qValues []types.QValue

	var qvalueId types.QValueString
	var err error
	for _, v := range doc {
		if v.Key == DefaultDocumentKeyColumnName {
			qvalueId, err = qValueStringFromKey(v.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert key %s: %w", DefaultDocumentKeyColumnName, err)
			}
			break
		}
	}
	if qvalueId.Val == "" {
		return nil, fmt.Errorf("key %s not found", DefaultDocumentKeyColumnName)
	}
	qValues = append(qValues, qvalueId)

	qvalueDoc, err := qValueJSONFromDocument(doc)
	if err != nil {
		return nil, err
	}
	qValues = append(qValues, qvalueDoc)

	return qValues, nil
}
