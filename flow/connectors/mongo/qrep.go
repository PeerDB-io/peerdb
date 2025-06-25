package connmongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const MongoFullTablePartitionId = "mongo-full-table-partition-id"

func (c *MongoConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	// if no watermark column is specified, return a single partition
	if config.WatermarkColumn == "" {
		return []*protos.QRepPartition{
			{
				PartitionId:        MongoFullTablePartitionId,
				Range:              nil,
				FullTablePartition: true,
			},
		}, nil
	}

	partitionHelper := utils.NewPartitionHelper(c.logger)
	return partitionHelper.GetPartitions(), nil
}

func (c *MongoConnector) PullQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	var totalRecords int64
	var totalBytes int64

	parseWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to parse watermark table: %w", err)
	}
	collection := c.client.Database(parseWatermarkTable.Schema).Collection(parseWatermarkTable.Table)

	stream.SetSchema(c.GetDefaultSchema())

	filter := bson.D{}
	if !partition.FullTablePartition {
		// For now partition range is always nil, see `GetQRepPartitions`
		if partition.Range != nil {
			filter, err = toRangeFilter(partition.Range)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to convert partition range to filter: %w", err)
			}
		}
	}

	batchSize := config.NumRowsPerPartition
	if config.NumRowsPerPartition <= 0 || config.NumRowsPerPartition > math.MaxInt32 {
		batchSize = math.MaxInt32
	}

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

		record, bytes, err := c.QValuesFromDocument(doc)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert record: %w", err)
		}
		stream.Records <- record
		totalRecords += 1
		totalBytes += bytes
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

	return totalRecords, totalBytes, nil
}

func (c *MongoConnector) GetDefaultSchema() types.QRecordSchema {
	schema := make([]types.QField, 0, 2)
	schema = append(schema,
		types.QField{
			Name:     DefaultDocumentKeyColumnName,
			Type:     types.QValueKindString,
			Nullable: false,
		},
		types.QField{
			Name:     DefaultFullDocumentColumnName,
			Type:     types.QValueKindJSON,
			Nullable: false,
		})
	return types.QRecordSchema{Fields: schema}
}

func toRangeFilter(partitionRange *protos.PartitionRange) (bson.D, error) {
	switch r := partitionRange.Range.(type) {
	case *protos.PartitionRange_ObjectIdRange:
		return bson.D{
			bson.E{Key: DefaultDocumentKeyColumnName, Value: bson.D{
				bson.E{Key: "$gte", Value: r.ObjectIdRange.Start},
				bson.E{Key: "$lte", Value: r.ObjectIdRange.End},
			}},
		}, nil
	default:
		return nil, errors.New("unsupported partition range type")
	}
}

func (c *MongoConnector) QValuesFromDocument(doc bson.D) ([]types.QValue, int64, error) {
	var qValues []types.QValue
	var size int64

	var qvalueId types.QValueString
	var err error
	for _, v := range doc {
		if v.Key == DefaultDocumentKeyColumnName {
			qvalueId, err = qValueStringFromKey(v.Value)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to convert key %s: %w", DefaultDocumentKeyColumnName, err)
			}
			break
		}
	}
	if qvalueId.Val == "" {
		return nil, 0, fmt.Errorf("key %s not found", DefaultDocumentKeyColumnName)
	}
	qValues = append(qValues, qvalueId)

	qvalueDoc, err := qValueJSONFromDocument(doc)
	if err != nil {
		return nil, 0, err
	}
	qValues = append(qValues, qvalueDoc)

	size += int64(len(qvalueDoc.Val))

	return qValues, size, nil
}
