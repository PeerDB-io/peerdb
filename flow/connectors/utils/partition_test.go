package utils

import (
	"log/slog"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func intRangeOf(t *testing.T, partition *protos.QRepPartition) *protos.IntPartitionRange {
	t.Helper()
	r, ok := partition.Range.Range.(*protos.PartitionRange_IntRange)
	require.True(t, ok)
	return r.IntRange
}

func uintRangeOf(t *testing.T, partition *protos.QRepPartition) *protos.UIntPartitionRange {
	t.Helper()
	r, ok := partition.Range.Range.(*protos.PartitionRange_UintRange)
	require.True(t, ok)
	return r.UintRange
}

func assertContiguousRanges[T int64 | uint64](t *testing.T, ranges [][2]T, minVal, maxVal T) {
	t.Helper()
	require.NotEmpty(t, ranges)
	require.Equal(t, minVal, ranges[0][0])
	require.Equal(t, maxVal, ranges[len(ranges)-1][1])
	for i := range ranges {
		require.Less(t, ranges[i][0], ranges[i][1])
		if i+1 < len(ranges) {
			require.Equal(t, ranges[i][1], ranges[i+1][0])
		}
	}
}

func TestComputeRanges(t *testing.T) {
	t.Run("even division", func(t *testing.T) {
		ranges := ComputeRanges[int64](1, 100, 10)
		require.Len(t, ranges, 10)
		assertContiguousRanges(t, ranges, int64(1), int64(100))
	})

	t.Run("span smaller than partition count", func(t *testing.T) {
		ranges := ComputeRanges[int64](0, 5, 10)
		require.Len(t, ranges, 5)
		assertContiguousRanges(t, ranges, int64(0), int64(5))
	})

	t.Run("two adjacent values", func(t *testing.T) {
		ranges := ComputeRanges[int64](7, 8, 4)
		require.Len(t, ranges, 1)
		assertContiguousRanges(t, ranges, int64(7), int64(8))
	})

	t.Run("full int64 domain does not overflow", func(t *testing.T) {
		ranges := ComputeRanges[int64](math.MinInt64, math.MaxInt64, 7)
		require.Len(t, ranges, 7)
		assertContiguousRanges(t, ranges, int64(math.MinInt64), int64(math.MaxInt64))
	})

	t.Run("full uint64 domain does not overflow", func(t *testing.T) {
		ranges := ComputeRanges[uint64](0, math.MaxUint64, 7)
		require.Len(t, ranges, 7)
		assertContiguousRanges(t, ranges, uint64(0), uint64(math.MaxUint64))
	})

	t.Run("same min/max values", func(t *testing.T) {
		ranges := ComputeRanges[int64](7, 7, 4)
		require.Len(t, ranges, 1)
		require.Equal(t, int64(7), ranges[0][0])
		require.Equal(t, int64(7), ranges[len(ranges)-1][1])
	})
}

func TestAddPartitionsWithRange(t *testing.T) {
	logger := log.NewStructuredLogger(slog.Default())

	t.Run("negative to positive int range covers domain contiguously", func(t *testing.T) {
		helper := NewPartitionHelper(logger)
		require.NoError(t, helper.AddPartitionsWithRange(int64(-100), int64(100), 4))

		partitions := helper.GetPartitions()
		require.Len(t, partitions, 4)
		require.Equal(t, int64(-100), intRangeOf(t, partitions[0]).Start)
		require.Equal(t, int64(100), intRangeOf(t, partitions[len(partitions)-1]).End)
		for i := 1; i < len(partitions); i++ {
			require.Equal(t, intRangeOf(t, partitions[i-1]).End+1, intRangeOf(t, partitions[i]).Start)
		}
	})

	t.Run("int range spanning full int64 domain does not overflow", func(t *testing.T) {
		helper := NewPartitionHelper(logger)
		require.NoError(t, helper.AddPartitionsWithRange(int64(math.MinInt64), int64(math.MaxInt64), 7))

		partitions := helper.GetPartitions()
		require.Len(t, partitions, 7)
		require.Equal(t, int64(math.MinInt64), intRangeOf(t, partitions[0]).Start)
		require.Equal(t, int64(math.MaxInt64), intRangeOf(t, partitions[len(partitions)-1]).End)
		for i, partition := range partitions {
			r := intRangeOf(t, partition)
			require.LessOrEqual(t, r.Start, r.End)
			if i > 0 {
				require.Equal(t, intRangeOf(t, partitions[i-1]).End+1, r.Start)
			}
		}
	})

	t.Run("uint range spanning full uint64 domain does not overflow", func(t *testing.T) {
		helper := NewPartitionHelper(logger)
		require.NoError(t, helper.AddPartitionsWithRange(uint64(0), uint64(math.MaxUint64), 7))

		partitions := helper.GetPartitions()
		require.Len(t, partitions, 7)
		require.Equal(t, uint64(0), uintRangeOf(t, partitions[0]).Start)
		require.Equal(t, uint64(math.MaxUint64), uintRangeOf(t, partitions[len(partitions)-1]).End)
		for i, partition := range partitions {
			r := uintRangeOf(t, partition)
			require.LessOrEqual(t, r.Start, r.End)
			if i > 0 {
				require.Equal(t, uintRangeOf(t, partitions[i-1]).End+1, r.Start)
			}
		}
	})

	t.Run("span smaller than partition count yields fewer partitions", func(t *testing.T) {
		helper := NewPartitionHelper(logger)
		require.NoError(t, helper.AddPartitionsWithRange(int64(0), int64(5), 10))

		partitions := helper.GetPartitions()
		require.Len(t, partitions, 5)
		require.Equal(t, int64(5), intRangeOf(t, partitions[len(partitions)-1]).End)
	})
}
