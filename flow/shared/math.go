package shared

import (
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
// When adjustment is needed, resulting partition count will be in [901, 1000] range for 9K+ rows and [501, 1000] range for 1-9K rows.
func AdjustNumPartitions(totalRows int64, desiredRowsPerPartition int64) AdjustedPartitions {
	const maxPartitions int64 = 1000

	desiredPartitions := DivCeil(totalRows, desiredRowsPerPartition)
	if desiredPartitions <= maxPartitions {
		return AdjustedPartitions{
			AdjustedNumPartitions:       desiredPartitions,
			AdjustedNumRowsPerPartition: desiredRowsPerPartition,
		}
	}

	// minimum rows per partition to stay within maxPartitions
	leading := DivCeil(totalRows, maxPartitions)

	// shrink to 2 significant figures, rounding up at each step
	scale := int64(1)
	for leading >= 100 {
		leading = DivCeil(leading, 10)
		scale *= 10
	}

	// scale back up: e.g. leading=24, scale=100 -> 2400
	adjustedRowsPerPartition := leading * scale

	// resulting partitions will be in [901, 1000] range
	return AdjustedPartitions{
		AdjustedNumPartitions:       DivCeil(totalRows, adjustedRowsPerPartition),
		AdjustedNumRowsPerPartition: adjustedRowsPerPartition,
	}
}
