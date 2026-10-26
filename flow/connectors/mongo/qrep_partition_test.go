package connmongo

import (
	"encoding/binary"
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// rawValueOf marshals a single-field document and returns the _id RawValue, so we
// can exercise the BSON type-detection helpers with real driver values.
func rawValueOf(t *testing.T, id any) bson.RawValue {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: DefaultDocumentKeyColumnName, Value: id}})
	require.NoError(t, err)
	return bson.Raw(raw).Lookup(DefaultDocumentKeyColumnName)
}

func TestRawValueToInt64(t *testing.T) {
	v, ok := rawValueToInt64(rawValueOf(t, int32(281704574)))
	require.True(t, ok)
	require.Equal(t, int64(281704574), v)

	v, ok = rawValueToInt64(rawValueOf(t, int64(9000000000)))
	require.True(t, ok)
	require.Equal(t, int64(9000000000), v)

	_, ok = rawValueToInt64(rawValueOf(t, "flipboard.app"))
	require.False(t, ok)

	_, ok = rawValueToInt64(rawValueOf(t, 3.14))
	require.False(t, ok)
}

func TestIsNumericIDType(t *testing.T) {
	require.True(t, isNumericIDType(rawValueOf(t, int32(1)).Type))
	require.True(t, isNumericIDType(rawValueOf(t, int64(1)).Type))
	require.False(t, isNumericIDType(rawValueOf(t, "x").Type))
	require.False(t, isNumericIDType(rawValueOf(t, 1.5).Type))
	oid, _ := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.False(t, isNumericIDType(rawValueOf(t, oid).Type))
}

// assertContiguous verifies the ranges form a gap-free, non-overlapping cover of
// [min, max]: first start == min, last end == max, and each range's end equals
// the next range's start (half-open except for the last which is closed).
func assertContiguous(t *testing.T, ranges [][2]string, minVal, maxVal string) {
	t.Helper()
	require.NotEmpty(t, ranges)
	require.Equal(t, minVal, ranges[0][0], "first partition must start at min")
	require.Equal(t, maxVal, ranges[len(ranges)-1][1], "last partition must end at max")
	for i := range ranges {
		require.Less(t, ranges[i][0], ranges[i][1], "range start must be < end")
		if i+1 < len(ranges) {
			require.Equal(t, ranges[i][1], ranges[i+1][0], "ranges must be contiguous")
		}
	}
}

func TestComputeStringBoundaries(t *testing.T) {
	t.Run("even distribution", func(t *testing.T) {
		samples := []string{"b", "c", "d", "e", "f", "g", "h", "i"}
		ranges := computeStringBoundaries("a", "z", samples, 4)
		require.Len(t, ranges, 4)
		assertContiguous(t, ranges, "a", "z")
	})

	t.Run("clustered samples still contiguous", func(t *testing.T) {
		samples := []string{"com.a", "com.b", "com.c", "com.d", "com.e", "com.f"}
		ranges := computeStringBoundaries("com.a", "org.z", samples, 3)
		require.LessOrEqual(t, len(ranges), 3)
		assertContiguous(t, ranges, "com.a", "org.z")
	})

	t.Run("fewer distinct samples than partitions", func(t *testing.T) {
		ranges := computeStringBoundaries("a", "z", []string{"m"}, 4)
		require.Len(t, ranges, 2) // one interior boundary -> two partitions
		assertContiguous(t, ranges, "a", "z")
		require.Equal(t, [2]string{"a", "m"}, ranges[0])
	})

	t.Run("duplicate samples deduplicated", func(t *testing.T) {
		samples := []string{"m", "m", "m", "m"}
		ranges := computeStringBoundaries("a", "z", samples, 4)
		require.Len(t, ranges, 2)
		assertContiguous(t, ranges, "a", "z")
	})

	t.Run("boundary samples ignored", func(t *testing.T) {
		// "a" == min and "z" == max must be dropped to avoid zero-width partitions;
		// only "m" is a valid interior boundary. Samples come pre-sorted from MongoDB.
		samples := []string{"a", "m", "z"}
		ranges := computeStringBoundaries("a", "z", samples, 4)
		require.Len(t, ranges, 2)
		assertContiguous(t, ranges, "a", "z")
		require.Equal(t, [2]string{"a", "m"}, ranges[0])
	})

	t.Run("no usable samples yields single range", func(t *testing.T) {
		ranges := computeStringBoundaries("a", "z", nil, 4)
		require.Len(t, ranges, 1) // caller treats <2 as full-table fallback
		assertContiguous(t, ranges, "a", "z")
	})

	t.Run("many samples produce requested partition count", func(t *testing.T) {
		samples := make([]string, 0, 100)
		for i := 'b'; i < 'b'+20; i++ {
			for j := 'a'; j < 'a'+5; j++ {
				samples = append(samples, string([]rune{i, j}))
			}
		}
		ranges := computeStringBoundaries("aa", "zz", samples, 8)
		require.Len(t, ranges, 8)
		assertContiguous(t, ranges, "aa", "zz")
	})
}

// objectIDFromParts builds an ObjectID from a leading 4-byte timestamp and a
// trailing 8-byte counter, so tests control both ordering and uniqueness.
func objectIDFromParts(ts uint32, counter uint64) bson.ObjectID {
	var oid bson.ObjectID
	binary.BigEndian.PutUint32(oid[:4], ts)
	binary.BigEndian.PutUint64(oid[4:], counter)
	return oid
}

// assertObjectIDRangesContiguous verifies the closed ranges form a gap-free,
// non-overlapping cover of [min, max]: first start == min, last end == max, and
// each range ends exactly one before the next range starts.
func assertObjectIDRangesContiguous(t *testing.T, ranges [][2]bson.ObjectID, minID, maxID bson.ObjectID) {
	t.Helper()
	require.NotEmpty(t, ranges)
	require.Equal(t, minID, ranges[0][0], "first partition must start at min")
	require.Equal(t, maxID, ranges[len(ranges)-1][1], "last partition must end at max")
	for i := range ranges {
		require.LessOrEqual(t, compareObjectIDs(ranges[i][0], ranges[i][1]), 0, "range start must be <= end")
		if i+1 < len(ranges) {
			afterEnd := new(big.Int).Add(new(big.Int).SetBytes(ranges[i][1][:]), big.NewInt(1))
			require.Zerof(t, afterEnd.Cmp(new(big.Int).SetBytes(ranges[i+1][0][:])),
				"range %d must end one before range %d starts; a gap would skip documents and an overlap would read them twice",
				i, i+1)
		}
	}
}

func TestComputeObjectIDBoundaries(t *testing.T) {
	minID := objectIDFromParts(1_000, 0)
	maxID := objectIDFromParts(2_000, 0)
	mid := objectIDFromParts(1_500, 0)

	t.Run("even distribution", func(t *testing.T) {
		samples := make([]bson.ObjectID, 0, 9)
		for ts := uint32(1_100); ts < 2_000; ts += 100 {
			samples = append(samples, objectIDFromParts(ts, 0))
		}
		ranges, err := computeObjectIDBoundaries(minID, maxID, samples, 4)
		require.NoError(t, err)
		require.Len(t, ranges, 4)
		assertObjectIDRangesContiguous(t, ranges, minID, maxID)
	})

	t.Run("unsorted samples give the same ranges as sorted", func(t *testing.T) {
		sorted := []bson.ObjectID{objectIDFromParts(1_200, 0), mid, objectIDFromParts(1_800, 0)}
		unsorted := []bson.ObjectID{objectIDFromParts(1_800, 0), objectIDFromParts(1_200, 0), mid}
		want, err := computeObjectIDBoundaries(minID, maxID, sorted, 4)
		require.NoError(t, err)
		got, err := computeObjectIDBoundaries(minID, maxID, unsorted, 4)
		require.NoError(t, err)
		require.Equal(t, want, got, "boundaries must not depend on the order samples arrive in")
	})

	t.Run("fewer distinct samples than partitions", func(t *testing.T) {
		ranges, err := computeObjectIDBoundaries(minID, maxID, []bson.ObjectID{mid}, 4)
		require.NoError(t, err)
		require.Len(t, ranges, 2) // one interior boundary -> two partitions
		assertObjectIDRangesContiguous(t, ranges, minID, maxID)
		require.Equal(t, mid, ranges[1][0])
	})

	t.Run("duplicate samples deduplicated", func(t *testing.T) {
		ranges, err := computeObjectIDBoundaries(minID, maxID, []bson.ObjectID{mid, mid, mid, mid}, 4)
		require.NoError(t, err)
		require.Len(t, ranges, 2)
		assertObjectIDRangesContiguous(t, ranges, minID, maxID)
	})

	t.Run("samples at or outside min and max ignored", func(t *testing.T) {
		// min and max would create zero-width partitions; samples outside the bounds
		// come from documents inserted after the bounds were read.
		samples := []bson.ObjectID{objectIDFromParts(900, 0), minID, mid, maxID, objectIDFromParts(2_100, 0)}
		ranges, err := computeObjectIDBoundaries(minID, maxID, samples, 4)
		require.NoError(t, err)
		require.Len(t, ranges, 2)
		assertObjectIDRangesContiguous(t, ranges, minID, maxID)
		require.Equal(t, mid, ranges[1][0])
	})

	t.Run("no usable samples yields single range", func(t *testing.T) {
		ranges, err := computeObjectIDBoundaries(minID, maxID, nil, 4)
		require.NoError(t, err)
		require.Equal(t, [][2]bson.ObjectID{{minID, maxID}}, ranges) // caller treats <2 as a fallback
	})

	t.Run("range end borrows across bytes", func(t *testing.T) {
		// mid's trailing bytes are all zero, so mid-1 must borrow from the timestamp.
		ranges, err := computeObjectIDBoundaries(minID, maxID, []bson.ObjectID{mid}, 2)
		require.NoError(t, err)
		require.Equal(t, objectIDFromParts(1_499, math.MaxUint64), ranges[0][1])
	})

	t.Run("skewed _ids still give balanced ranges", func(t *testing.T) {
		// Mirrors the collection that motivated sampling: a thin spread of _ids across
		// the whole keyspace (1970 to 2106 timestamps) plus most documents inside a few
		// hours. An even split of [min, max] puts nearly every document in one range.
		const background, dense, numPartitions = 2_000, 98_000, 100
		population := make([]bson.ObjectID, 0, background+dense)
		for i := range uint64(background) {
			population = append(population, objectIDFromParts(uint32(i*(math.MaxUint32/background)), i))
		}
		denseStart := uint32(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
		for i := range uint64(dense) {
			population = append(population, objectIDFromParts(denseStart+uint32(i/10), i))
		}
		slices.SortFunc(population, compareObjectIDs)
		popMin, popMax := population[0], population[len(population)-1]

		// $sample draws with replacement; a fixed seed keeps the test repeatable.
		rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic sampling for a repeatable test, not security
		samples := make([]bson.ObjectID, 0, numPartitions*partitionSampleOversample)
		for range numPartitions * partitionSampleOversample {
			samples = append(samples, population[rng.IntN(len(population))])
		}

		ranges, err := computeObjectIDBoundaries(popMin, popMax, samples, numPartitions)
		require.NoError(t, err)
		require.Len(t, ranges, numPartitions)
		assertObjectIDRangesContiguous(t, ranges, popMin, popMax)

		counts := make([]int, len(ranges))
		for _, id := range population {
			idx, _ := slices.BinarySearchFunc(ranges, id, func(r [2]bson.ObjectID, target bson.ObjectID) int {
				return compareObjectIDs(r[1], target)
			})
			counts[idx]++
		}
		mean := len(population) / len(ranges)
		largest := slices.Max(counts)
		require.LessOrEqualf(t, largest, 3*mean,
			"largest range holds %d of %d documents (mean %d); sampled boundaries must keep every range near the mean "+
				"even when most _ids sit in a narrow time window",
			largest, len(population), mean)
	})
}

func TestToRangeFilter(t *testing.T) {
	t.Run("numeric range is half-open", func(t *testing.T) {
		filter, err := toRangeFilter(DefaultDocumentKeyColumnName, utils.CreateNumericPartition(10, 20, false).Range)
		require.NoError(t, err)
		require.Equal(t, bson.D{
			bson.E{Key: DefaultDocumentKeyColumnName, Value: bson.D{
				bson.E{Key: "$gte", Value: int64(10)},
				bson.E{Key: "$lt", Value: int64(20)},
			}},
		}, filter)
	})

	t.Run("last numeric range is closed", func(t *testing.T) {
		filter, err := toRangeFilter(DefaultDocumentKeyColumnName, utils.CreateNumericPartition(20, 30, true).Range)
		require.NoError(t, err)
		require.Equal(t, bson.D{
			bson.E{Key: DefaultDocumentKeyColumnName, Value: bson.D{
				bson.E{Key: "$gte", Value: int64(20)},
				bson.E{Key: "$lte", Value: int64(30)},
			}},
		}, filter)
	})

	t.Run("string range is half-open", func(t *testing.T) {
		filter, err := toRangeFilter(DefaultDocumentKeyColumnName, utils.CreateStringPartition("com.a", "com.m", false).Range)
		require.NoError(t, err)
		require.Equal(t, bson.D{
			bson.E{Key: DefaultDocumentKeyColumnName, Value: bson.D{
				bson.E{Key: "$gte", Value: "com.a"},
				bson.E{Key: "$lt", Value: "com.m"},
			}},
		}, filter)
	})

	t.Run("last string range is closed", func(t *testing.T) {
		filter, err := toRangeFilter(DefaultDocumentKeyColumnName, utils.CreateStringPartition("com.m", "org.z", true).Range)
		require.NoError(t, err)
		require.Equal(t, bson.D{
			bson.E{Key: DefaultDocumentKeyColumnName, Value: bson.D{
				bson.E{Key: "$gte", Value: "com.m"},
				bson.E{Key: "$lte", Value: "org.z"},
			}},
		}, filter)
	})

	t.Run("object id range still supported", func(t *testing.T) {
		r := &protos.PartitionRange{Range: &protos.PartitionRange_ObjectIdRange{
			ObjectIdRange: &protos.ObjectIdPartitionRange{
				Start: "507f1f77bcf86cd799439011",
				End:   "507f1f77bcf86cd7994390ff",
			},
		}}
		_, err := toRangeFilter(DefaultDocumentKeyColumnName, r)
		require.NoError(t, err)
	})

	t.Run("unsupported range type errors", func(t *testing.T) {
		r := &protos.PartitionRange{Range: &protos.PartitionRange_NullRange{NullRange: &protos.NullPartitionRange{}}}
		_, err := toRangeFilter(DefaultDocumentKeyColumnName, r)
		require.Error(t, err)
	})
}
