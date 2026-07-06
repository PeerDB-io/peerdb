package connmongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

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

func intPartition(start, end int64) *protos.PartitionRange {
	return &protos.PartitionRange{Range: &protos.PartitionRange_IntRange{
		IntRange: &protos.IntPartitionRange{Start: start, End: end},
	}}
}

func stringPartition(start, end string, endInclusive bool) *protos.PartitionRange {
	return &protos.PartitionRange{Range: &protos.PartitionRange_StringRange{
		StringRange: &protos.StringPartitionRange{Start: start, End: end, EndInclusive: endInclusive},
	}}
}

func TestToRangeFilter(t *testing.T) {
	t.Run("int range is inclusive on both ends", func(t *testing.T) {
		filter, err := toRangeFilter(DefaultDocumentKeyColumnName, intPartition(10, 20))
		require.NoError(t, err)
		require.Equal(t, bson.D{
			bson.E{Key: DefaultDocumentKeyColumnName, Value: bson.D{
				bson.E{Key: "$gte", Value: int64(10)},
				bson.E{Key: "$lte", Value: int64(20)},
			}},
		}, filter)
	})

	t.Run("string range is half-open", func(t *testing.T) {
		filter, err := toRangeFilter(DefaultDocumentKeyColumnName, stringPartition("com.a", "com.m", false))
		require.NoError(t, err)
		require.Equal(t, bson.D{
			bson.E{Key: DefaultDocumentKeyColumnName, Value: bson.D{
				bson.E{Key: "$gte", Value: "com.a"},
				bson.E{Key: "$lt", Value: "com.m"},
			}},
		}, filter)
	})

	t.Run("last string range is closed", func(t *testing.T) {
		filter, err := toRangeFilter(DefaultDocumentKeyColumnName, stringPartition("com.m", "org.z", true))
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
