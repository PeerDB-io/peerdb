package shared

import (
	"fmt"
	"math"

	"golang.org/x/exp/constraints"
)

func DivCeil[T constraints.Integer](x, y T) T {
	return (x + y - 1) / y
}

// AdjustedPartitions represents the adjusted partitioning parameters
type AdjustedPartitions struct {
	AdjustedNumPartitions       int64
	AdjustedNumRowsPerPartition int64
}

// AdjustNumPartitions takes the total number of rows and the desired number of rows per partition,
// and returns the adjusted number of partitions and rows per partition so that the partition count does not exceed 1000.
// It does so by increasing the rows-per-partition by a power-of-10 multiplier when necessary.
func AdjustNumPartitions(totalRows, desiredRowsPerPartition int64) AdjustedPartitions {
	const maxPartitions = 1000

	// Calculate the initial number of partitions.
	desiredPartitions := DivCeil(totalRows, desiredRowsPerPartition)

	// If the initial partition count is within the allowed limit, return it.
	if desiredPartitions <= maxPartitions {
		return AdjustedPartitions{
			AdjustedNumPartitions:       desiredPartitions,
			AdjustedNumRowsPerPartition: desiredRowsPerPartition,
		}
	}

	// Determine the multiplier needed. We require:
	//   totalRows / (desiredRowsPerPartition * multiplier) <= maxPartitions
	// Rearranging:
	//   multiplier >= totalRows / (desiredRowsPerPartition * maxPartitions)
	ratio := float64(totalRows) / (float64(desiredRowsPerPartition) * float64(maxPartitions))

	// Compute the smallest power-of-10 multiplier that is at least the ratio.
	exponent := math.Ceil(math.Log10(ratio))
	multiplier := int64(math.Pow(10, exponent))

	// Adjust the rows per partition.
	adjustedRowsPerPartition := desiredRowsPerPartition * multiplier

	// Recalculate the number of partitions using the adjusted rows per partition.
	adjustedPartitions := DivCeil(totalRows, adjustedRowsPerPartition)

	// If, for any reason, the adjusted partition count is still over the maximum,
	// cap it at maxPartitions.
	if adjustedPartitions > maxPartitions {
		adjustedPartitions = maxPartitions
		// Recalculate rows per partition based on maxPartitions
		adjustedRowsPerPartition = DivCeil(totalRows, maxPartitions)
	}

	return AdjustedPartitions{
		AdjustedNumPartitions:       adjustedPartitions,
		AdjustedNumRowsPerPartition: adjustedRowsPerPartition,
	}
}

// based on postgres pg_size_pretty
type prettyUnit struct {
	name  string
	limit int64
}

var prettyUnits = []prettyUnit{
	{"bytes", 10 * 1024},
	{"kB", 20*1024 - 1},
	{"MB", 20*1024 - 1},
	{"GB", 20*1024 - 1},
	{"TB", 20*1024 - 1},
	{"PB", math.MaxInt64},
}

func PrettyBytes(size int64) string {
	for idx, unit := range prettyUnits {
		if size < unit.limit {
			if idx > 0 {
				size = (size + 1) / 2
			}

			return fmt.Sprintf("%d %s", size, unit.name)
		}

		size >>= 9
		if idx > 0 {
			size >>= 1
		}
	}
	// should never reach here
	return fmt.Sprintf("%d bytes", size)
}
