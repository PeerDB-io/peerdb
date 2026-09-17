package connmongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MongoConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn != DefaultDocumentKeyColumnName {
		c.logger.Warn("unexpected watermark column, falling back to full table partition")
		return utils.FullTablePartition(), nil
	}

	if config.NumRowsPerPartition <= 0 {
		return nil, fmt.Errorf("num rows per partition must be greater than 0")
	} else if last != nil && last.Range != nil {
		return nil, fmt.Errorf("last partition is not supported for MongoDB connector, got: %v", last)
	}

	numRowsPerPartition := int64(config.NumRowsPerPartition)
	parseWatermarkTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("unable to parse watermark table: %w", err)
	}
	collection := c.client.Database(parseWatermarkTable.Namespace).Collection(parseWatermarkTable.Table)

	// Firestore caps maxTimeMS at 600000; bound the context so count/boundary
	// commands don't inherit the multi-day activity deadline as maxTimeMS.
	ctx, cancel := c.boundOpContext(ctx)
	defer cancel()

	c.logger.Info("[mongo] fetching count of documents for partitioning",
		slog.String("watermark_table", config.WatermarkTable))
	// estimated, worst case we are off by a few documents but should be fine for partitioning
	totalRows, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to count documents in collection %s: %w", parseWatermarkTable.Table, err)
	}

	// Calculate the number of partitions
	adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
	c.logger.Info("[mongo] partition details",
		slog.Int64("totalRows", totalRows),
		slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
		slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
		slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))

	if adjustedPartitions.AdjustedNumPartitions <= 1 {
		c.logger.Info("[mongo] insufficient partitions for parallel snapshot, falling back to full table partition")
		return utils.FullTablePartition(), nil
	}

	return c.buildPartitions(ctx, collection, adjustedPartitions.AdjustedNumPartitions, config.Env)
}

func (c *MongoConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	mapping := make(map[string]string, len(input.TableMappings))
	for _, tm := range input.TableMappings {
		mapping[tm.SourceTableIdentifier] = DefaultDocumentKeyColumnName
	}
	return &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: mapping,
	}, nil
}

func (c *MongoConnector) PullQRepRecords(
	ctx context.Context,
	_catalogPool shared.CatalogPool,
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
	db := c.client.Database(parseWatermarkTable.Namespace)

	omitFullDocument, err := internal.PeerDBMongoDBOmitFullDocumentColumn(ctx, config.Env)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to resolve full-document column setting: %w", err)
	}

	errorOnUnmappedField, err := internal.PeerDBMongoDBErrorOnUnmappedField(ctx, config.Env)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to resolve unmapped-field setting: %w", err)
	}
	// knownFields is built once and reused across every document in the partition;
	// reportedUnmappedFields memoizes fields already surfaced so each is logged once.
	var knownFields, reportedUnmappedFields map[string]struct{}
	if errorOnUnmappedField {
		knownFields = buildKnownFieldSet(config.Columns)
		reportedUnmappedFields = make(map[string]struct{})
	}

	projections := buildProjections(config.Columns)
	stream.SetSchema(GetDefaultSchema(config.Version, omitFullDocument, projections))

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

	// Use RunCommandCursor instead of collection.Find so the driver sends maxTimeMS
	// (calculated from ctx deadline) to the server, overriding any server-side defaultMaxTimeMS.
	// collection.Find hardcodes OmitMaxTimeMS(true) which prevents this.
	findCmd := bson.D{
		{Key: "find", Value: parseWatermarkTable.Table},
		{Key: "filter", Value: filter},
		// MongoDb will use the lesser of batchSize and 16MiB
		// https://www.mongodb.com/docs/manual/reference/method/cursor.batchsize/
		{Key: "batchSize", Value: int32(batchSize)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}},
	}
	// Firestore caps maxTimeMS at 600000; bound the read context so the driver-issued
	// maxTimeMS is compliant. Sends to the destination stream keep the outer context.
	readCtx, cancelRead := c.boundOpContext(ctx)
	defer cancelRead()
	cursor, err := db.RunCommandCursor(readCtx, findCmd,
		options.RunCmd().SetReadPreference(protoToReadPref[c.config.ReadPreference]))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query for records: %w", err)
	}
	defer cursor.Close(readCtx)

	converter := NewDirectBsonConverter()
	for cursor.Next(readCtx) {
		if errorOnUnmappedField {
			if err := c.checkForUnmappedFields(cursor.Current, knownFields, reportedUnmappedFields, config.WatermarkTable); err != nil {
				return 0, 0, err
			}
		}

		record, err := QValuesFromBsonRaw(cursor.Current, config.Version, omitFullDocument, converter, config.WatermarkTable, projections)
		if err != nil {
			c.logger.Error("failed to convert record",
				slog.String("error", err.Error()),
				slog.Any("recordSize", len(cursor.Current)))
			return 0, 0, fmt.Errorf("failed to convert record: %w", err)
		}

		if err = stream.Send(ctx, record); err != nil {
			return 0, 0, fmt.Errorf("failed to send record to stream: %w", err)
		}

		totalRecords += 1
		if totalRecords%50000 == 0 {
			c.logger.Info("[mongo] pulling records",
				slog.Int64("records", totalRecords),
				slog.Int64("bytes", c.totalBytesRead.Load()),
				slog.Int("channelLen", len(stream.Records)))
		}
	}
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

func GetDefaultSchema(internalVersion uint32, omitFullDocument bool, projections []mongoProjection) types.QRecordSchema {
	fullDocumentColumnName := DefaultFullDocumentColumnName
	if internalVersion < shared.InternalVersion_MongoDBFullDocumentColumnToDoc {
		fullDocumentColumnName = LegacyFullDocumentColumnName
	}
	schema := make([]types.QField, 0, 2+len(projections))
	schema = append(schema,
		types.QField{
			Name:     DefaultDocumentKeyColumnName,
			Type:     types.QValueKindString,
			Nullable: false,
		})
	if !omitFullDocument {
		schema = append(schema, types.QField{
			Name:     fullDocumentColumnName,
			Type:     types.QValueKindJSON,
			Nullable: false,
		})
	}
	schema = append(schema, projectedQFields(projections)...)
	return types.QRecordSchema{Fields: schema}
}

// buildKnownFieldSet returns the set of top-level source field names that have a
// configured column mapping, plus the reserved _id key. Nested paths (e.g.
// "meta.addedSource.source") contribute their top-level segment ("meta").
func buildKnownFieldSet(columns []*protos.ColumnSetting) map[string]struct{} {
	known := make(map[string]struct{}, len(columns)+1)
	known[DefaultDocumentKeyColumnName] = struct{}{}
	for _, col := range columns {
		if col.SourceName == "" {
			continue
		}
		top := col.SourceName
		if idx := strings.IndexByte(top, '.'); idx >= 0 {
			top = top[:idx]
		}
		known[top] = struct{}{}
	}
	return known
}

// checkForUnmappedFields fails the initial load when a document carries a top-level
// field without a configured column mapping. reported memoizes fields already logged
// so each unmapped field surfaces once in the logs across the partition scan.
func (c *MongoConnector) checkForUnmappedFields(
	raw bson.Raw,
	known map[string]struct{},
	reported map[string]struct{},
	tableName string,
) error {
	elements, err := raw.Elements()
	if err != nil {
		return fmt.Errorf("failed to read document elements: %w", err)
	}
	for _, element := range elements {
		key := element.Key()
		if _, ok := known[key]; ok {
			continue
		}
		if _, seen := reported[key]; !seen {
			reported[key] = struct{}{}
			c.logger.Error("[mongo] field not among Postgres columns; add the column on Postgres with the appropriate data type",
				slog.String("field", key),
				slog.String("table", tableName))
		}
		return exceptions.NewMongoUnmappedFieldError(tableName, key)
	}
	return nil
}

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
	case *protos.PartitionRange_NumericRange:
		endOp := "$lt"
		if r.NumericRange.EndInclusive {
			endOp = "$lte"
		}
		return bson.D{
			bson.E{Key: watermarkColumn, Value: bson.D{
				bson.E{Key: "$gte", Value: r.NumericRange.Start},
				bson.E{Key: endOp, Value: r.NumericRange.End},
			}},
		}, nil
	case *protos.PartitionRange_StringRange:
		endOp := "$lt"
		if r.StringRange.EndInclusive {
			endOp = "$lte"
		}
		return bson.D{
			bson.E{Key: watermarkColumn, Value: bson.D{
				bson.E{Key: "$gte", Value: r.StringRange.Start},
				bson.E{Key: endOp, Value: r.StringRange.End},
			}},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported partition range type")
	}
}

// QValuesFromBsonRaw converts a raw BSON document to QValues, extracting the _id,
// producing JSON for the full document (unless omitFullDocument is set), and
// appending any user-configured typed projections (in projection order) using the
// provided converter.
func QValuesFromBsonRaw(
	raw bson.Raw,
	version uint32,
	omitFullDocument bool,
	converter BsonToQValueConverter,
	tableName string,
	projections []mongoProjection,
) ([]types.QValue, error) {
	rv := raw.Lookup(DefaultDocumentKeyColumnName)
	if rv.IsZero() || rv.Type == bson.TypeNull {
		return nil, exceptions.NewInvalidIdValueError(tableName)
	}
	idQValue, err := converter.QValueStringFromId(rv, version)
	if err != nil {
		return nil, fmt.Errorf("failed to convert key %s: %w", DefaultDocumentKeyColumnName, err)
	}

	values := make([]types.QValue, 0, 2+len(projections))
	values = append(values, idQValue)
	if !omitFullDocument {
		docQValue, err := converter.QValueJSONFromDocument(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to convert document %s: %w", DefaultFullDocumentColumnName, err)
		}
		values = append(values, docQValue)
	}
	return appendProjectedValues(values, raw, projections, converter)
}
