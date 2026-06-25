package connmongo

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"slices"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	// stringSampleOversample draws more samples than partitions so quantile
	// boundaries are well distributed even with clustered keys.
	stringSampleOversample = 20
	// stringSampleMaxSize caps sampling cost on very large collections.
	stringSampleMaxSize = 100000
)

// fullTablePartitions returns the single-partition full-table fallback used when a
// collection cannot be partitioned (unsupported/mixed _id type, empty collection,
// or degenerate min/max range).
func fullTablePartitions() []*protos.QRepPartition {
	return []*protos.QRepPartition{{
		PartitionId:        utils.FullTablePartitionID,
		Range:              nil,
		FullTablePartition: true,
	}}
}

// buildPartitions reads the collection's min and max _id (cheap, leverages the
// default _id index) and dispatches to a type-specific partitioner:
//   - ObjectID  -> uniform division of the 12-byte ObjectID keyspace
//   - Int32/64  -> uniform division of the numeric range (reuses PartitionHelper)
//   - String    -> sampling-based quantile boundaries
//
// Mixed-type _id (min and max differ in type), Double, or any other type fall back
// to a full-table partition. The change stream resume token is captured before this
// runs, so any writes during partitioning are replayed by CDC.
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
		return fullTablePartitions(), nil
	}
	maxRaw, err := findBoundaryID(ctx, collection, Upper, c.config.ReadPreference)
	if err != nil {
		return nil, fmt.Errorf("failed to find max _id: %w", err)
	}
	if maxRaw.IsZero() {
		c.logger.Info("[mongo] no documents found, falling back to full table partition")
		return fullTablePartitions(), nil
	}

	switch {
	case minRaw.Type == bson.TypeObjectID && maxRaw.Type == bson.TypeObjectID:
		return c.objectIDPartitions(minRaw.ObjectID(), maxRaw.ObjectID(), numPartitions)
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
		return fullTablePartitions(), nil
	}
}

// objectIDPartitions divides the ObjectID keyspace uniformly using integer
// arithmetic on the full 12-byte value. Since the leading 4-byte timestamp is the
// most significant component, this naturally partitions by insertion time.
func (c *MongoConnector) objectIDPartitions(
	minID bson.ObjectID,
	maxID bson.ObjectID,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	minInt := new(big.Int).SetBytes(minID[:])
	maxInt := new(big.Int).SetBytes(maxID[:])
	intRange := new(big.Int).Sub(maxInt, minInt)
	if intRange.Sign() <= 0 {
		c.logger.Info("[mongo] min/max ObjectID range is non-positive, falling back to full table partition")
		return fullTablePartitions(), nil
	}

	c.logger.Info("[mongo] using min/max ObjectID partitioning",
		slog.String("minID", minID.Hex()),
		slog.String("maxID", maxID.Hex()),
		slog.Int64("numPartitions", numPartitions))

	step := shared.BigIntDivCeil(intRange, big.NewInt(numPartitions))

	partitions := make([]*protos.QRepPartition, 0, numPartitions)
	for start := new(big.Int).Set(minInt); start.Cmp(maxInt) <= 0; start.Add(start, step) {
		end := new(big.Int).Add(start, step)
		end.Sub(end, big.NewInt(1))
		if end.Cmp(maxInt) > 0 {
			end.Set(maxInt)
		}
		startObjectID, err := bigIntToObjectID(start)
		if err != nil {
			return nil, err
		}
		endObjectID, err := bigIntToObjectID(end)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_ObjectIdRange{
					ObjectIdRange: &protos.ObjectIdPartitionRange{
						Start: startObjectID.Hex(),
						End:   endObjectID.Hex(),
					},
				},
			},
			FullTablePartition: false,
		})
	}

	return partitions, nil
}

// numericPartitions divides a numeric (int32/int64) _id range uniformly, reusing
// the shared PartitionHelper which builds non-overlapping IntPartitionRange
// partitions with tested +1 boundary semantics covering [min, max] inclusive.
//
// Limitation: IntPartitionRange uses inclusive-inclusive [start, end] boundaries,
// so documents with double _id values that fall between two adjacent integer
// boundaries (e.g. _id=2.5 between [1,2] and [3,4]) would be missed. This is an
// edge case for collections with mixed int/double _ids; a follow-up will introduce
// a NumericPartitionRange with half-open [start, end) semantics to address it.
func (c *MongoConnector) numericPartitions(
	minVal int64,
	maxVal int64,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	if maxVal <= minVal {
		c.logger.Info("[mongo] numeric min/max range is non-positive, falling back to full table partition")
		return fullTablePartitions(), nil
	}

	c.logger.Info("[mongo] using numeric _id partitioning",
		slog.Int64("minID", minVal),
		slog.Int64("maxID", maxVal),
		slog.Int64("numPartitions", numPartitions))

	helper := utils.NewPartitionHelper(c.logger)
	if err := helper.AddPartitionsWithRange(minVal, maxVal, numPartitions); err != nil {
		return nil, fmt.Errorf("failed to build numeric partitions: %w", err)
	}
	return helper.GetPartitions(), nil
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
		return fullTablePartitions(), nil
	}

	samples, err := c.sampleStringIDs(ctx, collection, numPartitions)
	if err != nil {
		return nil, err
	}

	ranges := computeStringBoundaries(minVal, maxVal, samples, numPartitions)
	if len(ranges) < 2 {
		c.logger.Info("[mongo] insufficient string boundaries from sampling, falling back to full table partition",
			slog.Int("sampleCount", len(samples)))
		return fullTablePartitions(), nil
	}

	c.logger.Info("[mongo] using sampled string _id partitioning",
		slog.String("minID", minVal),
		slog.String("maxID", maxVal),
		slog.Int("numPartitions", len(ranges)),
		slog.Int("sampleCount", len(samples)))

	partitions := make([]*protos.QRepPartition, 0, len(ranges))
	for i, rng := range ranges {
		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_StringRange{
					StringRange: &protos.StringPartitionRange{
						Start:        rng[0],
						End:          rng[1],
						EndInclusive: i == len(ranges)-1,
					},
				},
			},
			FullTablePartition: false,
		})
	}
	return partitions, nil
}

// sampleStringIDs draws a random sample of string _id values. $sample with a size
// below ~5% of the collection uses WiredTiger's random cursor, so this is cheap even
// on large collections; it honours the configured read preference (run on a
// secondary to avoid loading the primary).
func (c *MongoConnector) sampleStringIDs(
	ctx context.Context,
	collection *mongo.Collection,
	numPartitions int64,
) ([]string, error) {
	sampleSize := numPartitions * stringSampleOversample
	if sampleSize > stringSampleMaxSize {
		sampleSize = stringSampleMaxSize
	}

	aggCmd := bson.D{
		{Key: "aggregate", Value: collection.Name()},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: int32(sampleSize)}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: 1}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: 1}}}},
		}},
		{Key: "cursor", Value: bson.D{}},
	}

	cursor, err := collection.Database().RunCommandCursor(ctx, aggCmd,
		options.RunCmd().SetReadPreference(protoToReadPref[c.config.ReadPreference]))
	if err != nil {
		return nil, fmt.Errorf("failed to sample _id values: %w", err)
	}
	defer cursor.Close(ctx)

	samples := make([]string, 0, sampleSize)
	for cursor.Next(ctx) {
		rv := cursor.Current.Lookup(DefaultDocumentKeyColumnName)
		if rv.Type == bson.TypeString {
			samples = append(samples, rv.StringValue())
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error while sampling _id values: %w", err)
	}
	return samples, nil
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
		picked = slices.Compact(picked) // drop consecutive duplicate quantiles
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

// bigIntToObjectID converts a big.Int back to a 12-byte ObjectID.
func bigIntToObjectID(n *big.Int) (bson.ObjectID, error) {
	var oid bson.ObjectID
	if n.BitLen() > 96 {
		return oid, fmt.Errorf("big.Int value exceeds 96 bits (ObjectID size)")
	}
	n.FillBytes(oid[:])
	return oid, nil
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
