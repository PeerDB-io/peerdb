package connmongo

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const MONGO_FULL_TABLE_PARTITION_ID = "mongo-full-table-partition-id"

func (c *MongoConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	// if no watermark column is specified, return a single partition
	if config.WatermarkColumn == "" {
		return []*protos.QRepPartition{
			{
				PartitionId:        MONGO_FULL_TABLE_PARTITION_ID,
				Range:              nil,
				FullTablePartition: true,
			},
		}, nil
	}
	if last != nil && last.Range != nil {
		return nil, fmt.Errorf("not implemented")
	}
	parseWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("unable to parse watermark table: %w", err)
	}
	collection := c.client.Database(parseWatermarkTable.Schema).Collection(parseWatermarkTable.Table)

	// TODO: support partitioning with bucket > 1
	// TODO: support partitioning by columns other than _id
	numBuckets := 1
	pipeline := mongo.Pipeline{
		bson.D{
			bson.E{Key: "$bucketAuto", Value: bson.D{
				bson.E{Key: "groupBy", Value: "$_id"},
				bson.E{Key: "buckets", Value: numBuckets},
			}},
		},
	}
	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	defer cursor.Close(ctx)

	var results []struct {
		ID struct {
			Min bson.ObjectID `bson:"min"`
			Max bson.ObjectID `bson:"max"`
		} `bson:"_id"`
		Count int `bson:"count"`
	}
	if err = cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode partitions: %w", err)
	}

	partitionHelper := utils.NewPartitionHelper(c.logger)
	for _, result := range results {
		if err := partitionHelper.AddPartition(result.ID.Min, result.ID.Max); err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}

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

	stream.SetSchema(getDefaultSchema())

	filter := bson.D{}
	if !partition.FullTablePartition {
		filter, err = toRangeFilter(partition.Range)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert partition range to filter: %w", err)
		}
	}

	batchSize := config.NumRowsPerPartition
	if config.NumRowsPerPartition == 0 || config.NumRowsPerPartition > math.MaxInt32 {
		batchSize = math.MaxInt32
	}
	// MongoDb will use the lesser of batchSize and 16MiB
	// https://www.mongodb.com/docs/manual/reference/method/cursor.batchsize/
	opts1 := options.Find().SetBatchSize(int32(batchSize))
	opts := []options.Lister[options.FindOptions]{opts1}
	cursor, err := collection.Find(ctx, filter, opts...)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query for records: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return 0, 0, fmt.Errorf("failed to decode record: %w", err)
		}

		record, bytes, err := qValueFromBsonValue(doc)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert record: %w", err)
		}
		stream.Records <- record
		totalRecords += 1
		totalBytes += bytes
	}

	close(stream.Records)
	return totalRecords, totalBytes, nil
}

func getDefaultSchema() types.QRecordSchema {
	schema := make([]types.QField, 0, 2)
	schema = append(schema, types.QField{
		Name:     "_id",
		Type:     types.QValueKindString,
		Nullable: false,
	})
	schema = append(schema, types.QField{
		Name:     "_full_document",
		Type:     types.QValueKindJSON,
		Nullable: false,
	})
	return types.QRecordSchema{Fields: schema}
}

func toRangeFilter(partitionRange *protos.PartitionRange) (bson.D, error) {
	switch r := partitionRange.Range.(type) {
	case *protos.PartitionRange_ObjectIdRange:
		return bson.D{
			bson.E{Key: "_id", Value: bson.D{
				bson.E{Key: "$gte", Value: r.ObjectIdRange.Start},
				bson.E{Key: "$lte", Value: r.ObjectIdRange.End},
			}},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported partition range type")
	}
}

func qValueFromBsonValue(doc bson.D) ([]types.QValue, int64, error) {
	var qValues []types.QValue
	var size int64

	var qvalueId types.QValueString
	for _, v := range doc {
		if v.Key == "_id" {
			switch oid := v.Value.(type) {
			case bson.ObjectID:
				qvalueId = types.QValueString{Val: oid.Hex()}
			default:
				return nil, 0, fmt.Errorf("unsupported type for _id field")
			}
			break
		}
	}
	qValues = append(qValues, qvalueId)

	jsonb, err := json.Marshal(doc)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert doc to json: %w", err)
	}
	qvalueDoc := types.QValueJSON{Val: string(jsonb), IsArray: false}
	size += int64(len(qvalueDoc.Val))
	qValues = append(qValues, qvalueDoc)

	return qValues, size, nil
}
