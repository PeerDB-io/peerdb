package connmongo

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func objectIDWithTestSuffix(value uint64) bson.ObjectID {
	var id bson.ObjectID
	binary.BigEndian.PutUint64(id[4:], value)
	return id
}

func parsePartitionTestObjectID(t *testing.T, value string) bson.ObjectID {
	t.Helper()
	id, err := bson.ObjectIDFromHex(value)
	require.NoError(t, err)
	return id
}

// Closed ObjectID ranges must meet at consecutive integer values, rather than
// sharing a boundary as the half-open string and numeric ranges do.
func assertClosedObjectIDCoverage(t *testing.T, ranges [][2]bson.ObjectID, minID, maxID bson.ObjectID) {
	t.Helper()
	require.NotEmpty(t, ranges)
	require.Equal(t, minID, ranges[0][0])
	require.Equal(t, maxID, ranges[len(ranges)-1][1])
	for i, partition := range ranges {
		require.LessOrEqual(t, bytes.Compare(partition[0][:], partition[1][:]), 0,
			"partition %d must not be inverted", i)
		if i+1 == len(ranges) {
			continue
		}
		require.Negative(t, bytes.Compare(partition[1][:], maxID[:]))
		next := partition[1]
		for j := len(next) - 1; j >= 0; j-- {
			next[j]++
			if next[j] != 0 {
				break
			}
		}
		require.Equal(t, next, ranges[i+1][0],
			"partitions %d and %d must neither overlap nor leave a gap", i, i+1)
	}
}

func TestComputeObjectIDBoundariesBalancesClusterWithOutliers(t *testing.T) {
	minID := bson.NilObjectID
	maxID := parsePartitionTestObjectID(t, "ffffffffffffffffffffffff")
	const (
		clusterSize   = 1000
		numPartitions = 16
	)

	// Almost every document occupies a tiny part of the ObjectID keyspace.
	// Neither extreme outlier is in the sample, and most clustered IDs are also
	// unsampled. Equal-width keyspace ranges put the entire cluster in one range.
	documents := []bson.ObjectID{minID, maxID}
	var samples []bson.ObjectID
	for i := 1; i <= clusterSize; i++ {
		id := objectIDWithTestSuffix(uint64(i))
		id[0] = 0x65
		documents = append(documents, id)
		if i%5 == 0 {
			samples = append(samples, id)
		}
	}

	ranges := computeObjectIDBoundaries(minID, maxID, samples, numPartitions)
	require.Len(t, ranges, numPartitions)
	assertClosedObjectIDCoverage(t, ranges, minID, maxID)

	counts := make([]int, len(ranges))
	for _, id := range documents {
		matches := 0
		for i, partition := range ranges {
			if bytes.Compare(id[:], partition[0][:]) >= 0 && bytes.Compare(id[:], partition[1][:]) <= 0 {
				counts[i]++
				matches++
			}
		}
		require.Equal(t, 1, matches, "document %s must appear in exactly one partition", id.Hex())
	}

	// This is a broad skew regression check, not a promise of exact row balance.
	averageCeiling := (len(documents) + numPartitions - 1) / numPartitions
	for i, count := range counts {
		require.Positive(t, count, "partition %d should contain part of the dense cluster", i)
		require.LessOrEqual(t, count, 2*averageCeiling, "partition %d should not monopolize the cluster", i)
	}
}

func TestComputeObjectIDBoundariesSortsAndClipsSamples(t *testing.T) {
	minID, maxID := objectIDWithTestSuffix(10), objectIDWithTestSuffix(100)
	samples := []bson.ObjectID{
		objectIDWithTestSuffix(80), objectIDWithTestSuffix(0), maxID,
		objectIDWithTestSuffix(40), minID, objectIDWithTestSuffix(120),
		objectIDWithTestSuffix(20), objectIDWithTestSuffix(60), objectIDWithTestSuffix(40),
	}
	ranges := computeObjectIDBoundaries(minID, maxID, samples, 4)
	require.Greater(t, len(ranges), 1)
	require.LessOrEqual(t, len(ranges), 4)
	assertClosedObjectIDCoverage(t, ranges, minID, maxID)
	for _, partition := range ranges[1:] {
		require.Contains(t, samples, partition[0], "interior starts must be sampled IDs")
	}
}

func TestComputeObjectIDBoundariesPreservesSampleMultiplicity(t *testing.T) {
	minID, maxID := objectIDWithTestSuffix(0), objectIDWithTestSuffix(100)
	frequentID := objectIDWithTestSuffix(20)
	samples := make([]bson.ObjectID, 12, 13)
	for i := range samples {
		samples[i] = frequentID
	}
	samples = append(samples, objectIDWithTestSuffix(80))

	ranges := computeObjectIDBoundaries(minID, maxID, samples, 4)
	// All three requested quantiles fall on the frequent value. Deduplicate the
	// chosen cuts, rather than changing the distribution by deduplicating first.
	require.Equal(t, [][2]bson.ObjectID{
		{minID, objectIDWithTestSuffix(19)},
		{frequentID, maxID},
	}, ranges)
	assertClosedObjectIDCoverage(t, ranges, minID, maxID)
}

func TestComputeObjectIDBoundariesSmallSamples(t *testing.T) {
	minID, maxID := objectIDWithTestSuffix(10), objectIDWithTestSuffix(100)
	t.Run("one interior sample", func(t *testing.T) {
		middle := objectIDWithTestSuffix(50)
		ranges := computeObjectIDBoundaries(minID, maxID, []bson.ObjectID{middle}, 20)
		require.Equal(t, [][2]bson.ObjectID{
			{minID, objectIDWithTestSuffix(49)},
			{middle, maxID},
		}, ranges)
		assertClosedObjectIDCoverage(t, ranges, minID, maxID)
	})
	t.Run("only duplicate interior samples", func(t *testing.T) {
		middle := objectIDWithTestSuffix(50)
		ranges := computeObjectIDBoundaries(minID, maxID, []bson.ObjectID{middle, middle, middle}, 10)
		require.Len(t, ranges, 2)
		assertClosedObjectIDCoverage(t, ranges, minID, maxID)
	})
	t.Run("no samples", func(t *testing.T) {
		require.Equal(t, [][2]bson.ObjectID{{minID, maxID}},
			computeObjectIDBoundaries(minID, maxID, nil, 4))
	})
	t.Run("no interior samples", func(t *testing.T) {
		samples := []bson.ObjectID{objectIDWithTestSuffix(9), minID, maxID, objectIDWithTestSuffix(101)}
		require.Equal(t, [][2]bson.ObjectID{{minID, maxID}},
			computeObjectIDBoundaries(minID, maxID, samples, 4))
	})
}

func TestComputeObjectIDBoundariesExtremeAndAdjacentValues(t *testing.T) {
	t.Run("zero and maximum endpoints", func(t *testing.T) {
		minID := bson.NilObjectID
		maxID := parsePartitionTestObjectID(t, "ffffffffffffffffffffffff")
		middle := parsePartitionTestObjectID(t, "800000000000000000000000")
		ranges := computeObjectIDBoundaries(minID, maxID, []bson.ObjectID{middle}, 2)
		require.Equal(t, [][2]bson.ObjectID{
			{minID, parsePartitionTestObjectID(t, "7fffffffffffffffffffffff")},
			{middle, maxID},
		}, ranges)
		assertClosedObjectIDCoverage(t, ranges, minID, maxID)
	})
	t.Run("adjacent cuts produce valid singleton ranges", func(t *testing.T) {
		minID, maxID := objectIDWithTestSuffix(254), objectIDWithTestSuffix(258)
		samples := []bson.ObjectID{objectIDWithTestSuffix(255), objectIDWithTestSuffix(256), objectIDWithTestSuffix(257)}
		ranges := computeObjectIDBoundaries(minID, maxID, samples, 4)
		require.Equal(t, [][2]bson.ObjectID{
			{minID, minID},
			{objectIDWithTestSuffix(255), objectIDWithTestSuffix(255)},
			{objectIDWithTestSuffix(256), objectIDWithTestSuffix(256)},
			{objectIDWithTestSuffix(257), maxID},
		}, ranges)
		assertClosedObjectIDCoverage(t, ranges, minID, maxID)
	})
}

func TestComputeObjectIDBoundariesDegenerateInputs(t *testing.T) {
	minID, maxID := objectIDWithTestSuffix(10), objectIDWithTestSuffix(100)
	samples := []bson.ObjectID{objectIDWithTestSuffix(50)}
	t.Run("reversed endpoints", func(t *testing.T) {
		require.Nil(t, computeObjectIDBoundaries(maxID, minID, samples, 4))
	})
	t.Run("single value", func(t *testing.T) {
		require.Equal(t, [][2]bson.ObjectID{{minID, minID}},
			computeObjectIDBoundaries(minID, minID, samples, 4))
	})
	for _, numPartitions := range []int64{-1, 0, 1} {
		require.Equal(t, [][2]bson.ObjectID{{minID, maxID}},
			computeObjectIDBoundaries(minID, maxID, samples, numPartitions),
			"partition count %d must yield one full range", numPartitions)
	}
}
