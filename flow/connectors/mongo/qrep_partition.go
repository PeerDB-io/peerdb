package connmongo

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	// partitionSampleOversample draws more samples than partitions so quantile
	// boundaries are well distributed even with clustered keys.
	partitionSampleOversample = 20
	// partitionSampleMaxSize caps the number of sampled keys, not server-side I/O.
	partitionSampleMaxSize = 100000
	// $sample can require a collection scan when its random cursor optimization
	// is unavailable. Bound planning time independently of the snapshot timeout.
	partitionSampleTimeout = 5 * time.Minute
)

// buildPartitions reads the collection's min and max _id (cheap, leverages the
// default _id index) and dispatches to a type-specific partitioner:
//   - ObjectID  -> sampling-based quantile boundaries
//   - Int32/64  -> uniform division of the numeric range
//   - String    -> sampling-based quantile boundaries
//
// Mixed-type _id, Double, or any other type fall back to a full-table partition.
func (c *MongoConnector) buildPartitions(
	ctx context.Context,
	collection *mongo.Collection,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	minRaw, err := findBoundaryID(ctx, collection, Lower, c.config.ReadPreference)
	if err != nil {
		return nil, fmt.Errorf("failed to find min _id: %w", err)
	}
	if minRaw.IsZero() {
		c.logger.Info("[mongo] no documents found, falling back to full table partition")
		return utils.FullTablePartition(), nil
	}
	maxRaw, err := findBoundaryID(ctx, collection, Upper, c.config.ReadPreference)
	if err != nil {
		return nil, fmt.Errorf("failed to find max _id: %w", err)
	}
	if maxRaw.IsZero() {
		c.logger.Info("[mongo] no documents found, falling back to full table partition")
		return utils.FullTablePartition(), nil
	}

	switch {
	case minRaw.Type == bson.TypeObjectID && maxRaw.Type == bson.TypeObjectID:
		return c.objectIDPartitions(ctx, collection, minRaw.ObjectID(), maxRaw.ObjectID(), numPartitions)
	case isNumericIDType(minRaw.Type) && isNumericIDType(maxRaw.Type):
		minVal, _ := rawValueToInt64(minRaw)
		maxVal, _ := rawValueToInt64(maxRaw)
		return c.numericPartitions(minVal, maxVal, numPartitions)
	case minRaw.Type == bson.TypeString && maxRaw.Type == bson.TypeString:
		return c.stringPartitions(ctx, collection, minRaw.StringValue(), maxRaw.StringValue(), numPartitions)
	default:
		c.logger.Info("[mongo] _id type not supported for partitioning (or mixed types), falling back to full table partition",
			slog.String("minType", minRaw.Type.String()),
			slog.String("maxType", maxRaw.Type.String()))
		return utils.FullTablePartition(), nil
	}
}

// objectIDPartitions samples the document distribution rather than dividing the
// ObjectID keyspace uniformly: equal-width timestamp ranges can contain vastly
// different numbers of documents, especially in the presence of outlying IDs.
func (c *MongoConnector) objectIDPartitions(
	ctx context.Context,
	collection *mongo.Collection,
	minID bson.ObjectID,
	maxID bson.ObjectID,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	if bytes.Compare(minID[:], maxID[:]) >= 0 {
		c.logger.Info("[mongo] min/max ObjectID range is non-positive, falling back to full table partition")
		return utils.FullTablePartition(), nil
	}

	rawSamples, err := c.sampleIDs(ctx, collection, numPartitions)
	if err != nil {
		return nil, err
	}
	samples := make([]bson.ObjectID, 0, len(rawSamples))
	for _, rv := range rawSamples {
		if rv.Type == bson.TypeObjectID {
			samples = append(samples, rv.ObjectID())
		}
	}
	ranges := computeObjectIDBoundaries(minID, maxID, samples, numPartitions)
	if len(ranges) < 2 {
		c.logger.Info("[mongo] insufficient ObjectID boundaries from sampling, falling back to full table partition",
			slog.Int("sampleCount", len(samples)))
		return utils.FullTablePartition(), nil
	}

	c.logger.Info("[mongo] using sampled ObjectID partitioning",
		slog.String("minID", minID.Hex()),
		slog.String("maxID", maxID.Hex()),
		slog.Int("numPartitions", len(ranges)),
		slog.Int("sampleCount", len(samples)))

	partitions := make([]*protos.QRepPartition, 0, len(ranges))
	for _, rng := range ranges {
		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_ObjectIdRange{
					ObjectIdRange: &protos.ObjectIdPartitionRange{
						Start: rng[0].Hex(),
						End:   rng[1].Hex(),
					},
				},
			},
			FullTablePartition: false,
		})
	}
	return partitions, nil
}

// numericPartitions divides a numeric (int32/int64) _id range uniformly. MongoDB
// sorts all numeric types by value, so integer min/max _ids cannot rule out
// fractional (double/decimal) _ids in-between. Therefore we use [start, end) to
// ensure contiguous boundaries.
func (c *MongoConnector) numericPartitions(
	minVal int64,
	maxVal int64,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	if maxVal <= minVal {
		c.logger.Info("[mongo] numeric min/max range is non-positive, falling back to full table partition")
		return utils.FullTablePartition(), nil
	}

	ranges := utils.ComputeRanges(minVal, maxVal, numPartitions)
	c.logger.Info("[mongo] using numeric _id partitioning", slog.Int("numPartitions", len(ranges)))
	partitions := make([]*protos.QRepPartition, 0, len(ranges))
	for i, rng := range ranges {
		partitions = append(partitions, utils.CreateNumericPartition(rng[0], rng[1], i == len(ranges)-1))
	}
	return partitions, nil
}

// stringPartitions builds partitions for a string _id collection. Because string
// keys (e.g. package names) are not uniformly distributed, uniform division of the
// keyspace would be badly skewed; instead we sample the collection and pick quantile
// boundaries so each partition holds a roughly equal share of documents.
func (c *MongoConnector) stringPartitions(
	ctx context.Context,
	collection *mongo.Collection,
	minVal string,
	maxVal string,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	if minVal >= maxVal {
		c.logger.Info("[mongo] string min/max range is non-positive, falling back to full table partition")
		return utils.FullTablePartition(), nil
	}

	rawSamples, err := c.sampleIDs(ctx, collection, numPartitions)
	if err != nil {
		return nil, err
	}
	samples := make([]string, 0, len(rawSamples))
	for _, rv := range rawSamples {
		if rv.Type == bson.TypeString {
			samples = append(samples, rv.StringValue())
		}
	}

	ranges := computeStringBoundaries(minVal, maxVal, samples, numPartitions)
	if len(ranges) < 2 {
		c.logger.Info("[mongo] insufficient string boundaries from sampling, falling back to full table partition",
			slog.Int("sampleCount", len(samples)))
		return utils.FullTablePartition(), nil
	}

	c.logger.Info("[mongo] using sampled string _id partitioning",
		slog.Int("numPartitions", len(ranges)),
		slog.Int("sampleCount", len(samples)))

	partitions := make([]*protos.QRepPartition, 0, len(ranges))
	for i, rng := range ranges {
		partitions = append(partitions, utils.CreateStringPartition(rng[0], rng[1], i == len(ranges)-1))
	}
	return partitions, nil
}

// sampleIDs draws a bounded sample of _id values. Keep $sample first to allow
// MongoDB's random cursor optimization when the collection and sample sizes permit
// it. Sorting after projection preserves database collation for string keys while
// limiting sort memory to the sampled IDs. Sampling is not guaranteed to be uniform.
func (c *MongoConnector) sampleIDs(
	ctx context.Context,
	collection *mongo.Collection,
	numPartitions int64,
) ([]bson.RawValue, error) {
	if numPartitions <= 0 {
		return nil, fmt.Errorf("number of partitions must be greater than 0")
	}
	sampleSize := min(numPartitions, partitionSampleMaxSize/partitionSampleOversample) * partitionSampleOversample
	ctx, cancel := context.WithTimeout(ctx, partitionSampleTimeout)
	defer cancel()

	aggCmd := bson.D{
		{Key: "aggregate", Value: collection.Name()},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: int32(sampleSize)}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: 1}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: 1}}}},
		}},
		{Key: "cursor", Value: bson.D{}},
	}

	cursor, err := collection.Database().RunCommandCursor(ctx, aggCmd,
		options.RunCmd().SetReadPreference(protoToReadPref[c.config.ReadPreference]))
	if err != nil {
		return nil, fmt.Errorf("failed to sample _id values: %w", err)
	}
	defer func() {
		// A sampling timeout must still allow killCursors to reach the server.
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer closeCancel()
		if err := cursor.Close(closeCtx); err != nil {
			c.logger.Warn("[mongo] failed to close _id sampling cursor", slog.Any("error", err))
		}
	}()

	samples := make([]bson.RawValue, 0, sampleSize)
	for cursor.Next(ctx) {
		rv := cursor.Current.Lookup(DefaultDocumentKeyColumnName)
		// Cursor.Current is only valid until the next call to Next.
		rv.Value = bytes.Clone(rv.Value)
		samples = append(samples, rv)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error while sampling _id values: %w", err)
	}
	return samples, nil
}

// computeObjectIDBoundaries builds closed, non-overlapping ranges covering the
// original [minID, maxID], including keys absent from the sample. Sampling and
// boundary reads are separate operations, so discard samples outside those bounds.
// Quantiles approximate document counts; they do not balance bytes or copy time.
func computeObjectIDBoundaries(
	minID bson.ObjectID,
	maxID bson.ObjectID,
	samples []bson.ObjectID,
	numPartitions int64,
) [][2]bson.ObjectID {
	if bytes.Compare(minID[:], maxID[:]) > 0 {
		return nil
	}
	if numPartitions <= 1 || minID == maxID {
		return [][2]bson.ObjectID{{minID, maxID}}
	}
	interior := make([]bson.ObjectID, 0, len(samples))
	for _, id := range samples {
		if bytes.Compare(id[:], minID[:]) > 0 && bytes.Compare(id[:], maxID[:]) < 0 {
			interior = append(interior, id)
		}
	}
	slices.SortFunc(interior, func(a, b bson.ObjectID) int { return bytes.Compare(a[:], b[:]) })

	// Retain duplicate samples when selecting quantiles, so their frequency still
	// contributes to the distribution. Only deduplicate the selected boundaries.
	ranges := make([][2]bson.ObjectID, 0, min(numPartitions, int64(len(interior))+1))
	start := minID
	if len(interior) > 0 {
		for i := int64(1); i < numPartitions; i++ {
			boundary := interior[i*int64(len(interior))/numPartitions]
			if boundary == start {
				continue
			}
			// ObjectID ranges already use inclusive ends. The predecessor of the
			// next start preserves that format without gaps or overlapping keys.
			end := boundary
			for j := len(end) - 1; j >= 0; j-- {
				end[j]--
				if end[j] != 0xff {
					break
				}
			}
			ranges = append(ranges, [2]bson.ObjectID{start, end})
			start = boundary
		}
	}
	return append(ranges, [2]bson.ObjectID{start, maxID})
}

// computeStringBoundaries turns a pre-sorted (by database collation) sample of
// string _ids plus the real min/max into a contiguous set of ranges. Interior
// boundaries are quantiles of the (deduplicated) sample; the first range starts
// at the real min and the last range ends at the real max. All ranges except the
// last are half-open [start, end); the last is closed [start, end].
//
// samples must already be sorted in the database's collation order; this function
// does not re-sort them, to preserve collation semantics.
//
// It is pure (no I/O) to keep the boundary math unit-testable. Returns fewer ranges
// than numPartitions when the sample yields too few distinct interior boundaries.
func computeStringBoundaries(minVal string, maxVal string, samples []string, numPartitions int64) [][2]string {
	// Keep unique sampled values strictly inside (min, max), preserving order.
	// We use exact equality to filter boundaries: MongoDB guarantees all sampled
	// _ids are within [minVal, maxVal] by its own collation; only exact matches
	// with the boundary values would create zero-width partitions.
	seen := make(map[string]struct{}, len(samples))
	interior := make([]string, 0, len(samples))
	for _, s := range samples {
		if s == minVal || s == maxVal {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		interior = append(interior, s)
	}

	// Pick up to numPartitions-1 evenly spaced interior boundaries (quantiles).
	desiredBoundaries := numPartitions - 1
	var picked []string
	if int64(len(interior)) <= desiredBoundaries {
		picked = interior
	} else {
		picked = make([]string, 0, desiredBoundaries)
		for i := int64(1); i <= desiredBoundaries; i++ {
			idx := i * int64(len(interior)) / numPartitions
			if idx >= int64(len(interior)) {
				idx = int64(len(interior)) - 1
			}
			picked = append(picked, interior[idx])
		}
	}

	// Build ranges: [min, p0), [p0, p1), ..., [pLast-1, pLast), [pLast, max].
	// The last range is closed (end_inclusive=true); all others are half-open.
	starts := append([]string{minVal}, picked...)
	ranges := make([][2]string, len(starts))
	for i := range starts {
		var end string
		if i+1 < len(starts) {
			end = starts[i+1]
		} else {
			end = maxVal
		}
		ranges[i] = [2]string{starts[i], end}
	}
	return ranges
}

func isNumericIDType(t bson.Type) bool {
	return t == bson.TypeInt32 || t == bson.TypeInt64
}

func rawValueToInt64(rv bson.RawValue) (int64, bool) {
	switch rv.Type {
	case bson.TypeInt32:
		return int64(rv.Int32()), true
	case bson.TypeInt64:
		return rv.Int64(), true
	default:
		return 0, false
	}
}

type Boundary int

const (
	Lower Boundary = 1
	Upper Boundary = -1
)

// findBoundaryID returns the lower- or upper-bound _id of a collection
func findBoundaryID(
	ctx context.Context,
	collection *mongo.Collection,
	boundary Boundary,
	readPreference protos.ReadPreference,
) (bson.RawValue, error) {
	findCmd := bson.D{
		{Key: "find", Value: collection.Name()},
		{Key: "sort", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: boundary}}},
		{Key: "limit", Value: 1},
		{Key: "projection", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: 1}}},
	}

	cursor, err := collection.Database().RunCommandCursor(ctx, findCmd, options.RunCmd().SetReadPreference(protoToReadPref[readPreference]))
	if err != nil {
		return bson.RawValue{}, fmt.Errorf("failed to run find command: %w", err)
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return bson.RawValue{}, fmt.Errorf("cursor error: %w", err)
		}
		// no results
		return bson.RawValue{}, nil
	}
	return cursor.Current.Lookup(DefaultDocumentKeyColumnName), nil
}
