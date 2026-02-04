package shared

import (
	"slices"
	"testing"
)

func TestAdjustNumPartitions(t *testing.T) {
	tests := []struct {
		name                     string
		totalRows                int64
		desiredRowsPerPartition  int64
		expectedPartitions       int64
		expectedRowsPerPartition int64
	}{
		{
			name:                    "No adjustment needed",
			totalRows:               1000,
			desiredRowsPerPartition: 150,
			// partitions = ceil(1000 / 150) = 7, which is <= 1000.
			expectedPartitions:       7,
			expectedRowsPerPartition: 150,
		},
		{
			name:                    "At exactly limit",
			totalRows:               1000,
			desiredRowsPerPartition: 1,
			// partitions = 1000, no adjustment needed.
			expectedPartitions:       1000,
			expectedRowsPerPartition: 1,
		},
		{
			name:                    "Small totalRows",
			totalRows:               50,
			desiredRowsPerPartition: 1,
			// partitions = 50 (cannot exceed totalRows).
			expectedPartitions:       50,
			expectedRowsPerPartition: 1,
		},
		{
			name:                    "Moderate adjustment",
			totalRows:               100000,
			desiredRowsPerPartition: 50,
			// initial partitions = ceil(100000 / 50) = 2000 > 1000
			// leading = ceil(100000 / 1000) = 100 -> 10, scale = 10
			// adjustedRowsPerPartition = 10 * 10 = 100
			expectedPartitions:       1000,
			expectedRowsPerPartition: 100,
		},
		{
			name:                    "Large totalRows exactly max",
			totalRows:               1000000000, // 1e9
			desiredRowsPerPartition: 100000,
			// initial partitions = 1e9 / 100000 = 10000 > 1000
			// leading = ceil(1e9 / 1000) = 1000000 -> 10, scale = 100000
			// adjustedRowsPerPartition = 10 * 100000 = 1e6
			expectedPartitions:       1000,
			expectedRowsPerPartition: 1000000,
		},
		{
			name:                    "Large totalRows with multiplier",
			totalRows:               1000000000, // 1e9
			desiredRowsPerPartition: 50000,
			// initial partitions = 1e9 / 50000 = 20000 > 1000
			// leading = ceil(1e9 / 1000) = 1000000 -> 10, scale = 100000
			// adjustedRowsPerPartition = 10 * 100000 = 1e6
			expectedPartitions:       1000,
			expectedRowsPerPartition: 1000000,
		},
		{
			name:                    "Moderate values",
			totalRows:               500000,
			desiredRowsPerPartition: 250,
			// initial partitions = ceil(500000 / 250) = 2000 > 1000
			// leading = ceil(500000 / 1000) = 500 -> 50, scale = 10
			// adjustedRowsPerPartition = 50 * 10 = 500
			expectedPartitions:       1000,
			expectedRowsPerPartition: 500,
		},
		{
			name:                    "Exact division yields no adjustment",
			totalRows:               120000,
			desiredRowsPerPartition: 150,
			// initial partitions = 120000 / 150 = 800 exactly.
			expectedPartitions:       800,
			expectedRowsPerPartition: 150,
		},
		{
			name:                    "Non exact division with high desired",
			totalRows:               987654321,
			desiredRowsPerPartition: 80001,
			// initial partitions = ceil(987654321 / 80001) = 12346 > 1000
			// leading = ceil(987654321 / 1000) = 987655 -> 99, scale = 10000
			// adjustedRowsPerPartition = 99 * 10000 = 990000
			expectedPartitions:       998,
			expectedRowsPerPartition: 990000,
		},
		{
			name:                    "Desired rows per partition greater than totalRows",
			totalRows:               100,
			desiredRowsPerPartition: 150,
			// initial partitions = ceil(100/150) = 1, which is valid.
			expectedPartitions:       1,
			expectedRowsPerPartition: 150,
		},
		{
			name:                     "Leading 99, no rounding",
			totalRows:                99000,
			desiredRowsPerPartition:  1,
			expectedPartitions:       1000,
			expectedRowsPerPartition: 99,
		},
		{
			name:                     "Leading 100 rounds to 10",
			totalRows:                100000,
			desiredRowsPerPartition:  1,
			expectedPartitions:       1000,
			expectedRowsPerPartition: 100,
		},
		{
			name:                     "Leading 101 rounds to 11",
			totalRows:                101000,
			desiredRowsPerPartition:  1,
			expectedPartitions:       919,
			expectedRowsPerPartition: 110,
		},
		{
			name:                     "Leading 110 rounds to 11",
			totalRows:                110000,
			desiredRowsPerPartition:  1,
			expectedPartitions:       1000,
			expectedRowsPerPartition: 110,
		},
		{
			name:                     "Leading 111 rounds to 12",
			totalRows:                111000,
			desiredRowsPerPartition:  1,
			expectedPartitions:       925,
			expectedRowsPerPartition: 120,
		},
		{
			name:                     "Worst case 901 partitions",
			totalRows:                9001,
			desiredRowsPerPartition:  1,
			expectedPartitions:       901,
			expectedRowsPerPartition: 10,
		},
		{
			name:                     "Multi-iteration rounding",
			totalRows:                1010000001,
			desiredRowsPerPartition:  1,
			expectedPartitions:       919,
			expectedRowsPerPartition: 1100000,
		},
		{
			name:                     "Very large totalRows",
			totalRows:                9_000_000_000_000_000_000,
			desiredRowsPerPartition:  1,
			expectedPartitions:       1000,
			expectedRowsPerPartition: 9_000_000_000_000_000,
		},
		{
			name:                     "Small totalRows leading 2",
			totalRows:                1001,
			desiredRowsPerPartition:  1,
			expectedPartitions:       501,
			expectedRowsPerPartition: 2,
		},
		{
			name:                     "Small totalRows leading 5",
			totalRows:                4001,
			desiredRowsPerPartition:  1,
			expectedPartitions:       801,
			expectedRowsPerPartition: 5,
		},
		{
			name:                     "Small totalRows leading 9",
			totalRows:                8001,
			desiredRowsPerPartition:  1,
			expectedPartitions:       889,
			expectedRowsPerPartition: 9,
		},
	}

	for _, tc := range tests {
		got := AdjustNumPartitions(tc.totalRows, tc.desiredRowsPerPartition)
		if got.AdjustedNumPartitions != tc.expectedPartitions {
			t.Errorf("AdjustNumPartitions(totalRows=%d, desiredRowsPerPartition=%d).AdjustedNumPartitions = %d; want %d",
				tc.totalRows, tc.desiredRowsPerPartition, got.AdjustedNumPartitions, tc.expectedPartitions)
		}
		if got.AdjustedNumRowsPerPartition != tc.expectedRowsPerPartition {
			t.Errorf("AdjustNumPartitions(totalRows=%d, desiredRowsPerPartition=%d).AdjustedNumRowsPerPartition = %d; want %d",
				tc.totalRows, tc.desiredRowsPerPartition, got.AdjustedNumRowsPerPartition, tc.expectedRowsPerPartition)
		}
	}
}

func TestAdjustNumPartitionsProperties(t *testing.T) {
	for totalRows := int64(1); totalRows <= 1000000; totalRows++ {
		for desired := int64(1); desired <= 10; desired++ {
			result := AdjustNumPartitions(totalRows, desired)
			partitions := result.AdjustedNumPartitions
			rowsPerPartition := result.AdjustedNumRowsPerPartition

			if partitions > 1000 {
				t.Fatalf("totalRows=%d desired=%d: partitions=%d exceeds 1000", totalRows, desired, partitions)
			}
			if partitions*rowsPerPartition < totalRows {
				t.Fatalf("totalRows=%d desired=%d: %d*%d doesn't cover all rows", totalRows, desired, partitions, rowsPerPartition)
			}
			if DivCeil(totalRows, desired) > 1000 {
				if totalRows >= 9001 && partitions < 901 {
					t.Fatalf("totalRows=%d desired=%d: partitions=%d below 901", totalRows, desired, partitions)
				}
				if totalRows < 9001 && partitions < 501 {
					t.Fatalf("totalRows=%d desired=%d: partitions=%d below 501", totalRows, desired, partitions)
				}
			}
		}
	}
}

func TestParsePgArray(t *testing.T) {
	tests := []struct {
		input  string
		output []string
	}{
		{"[1:2]{1,2,\\\"3}", []string{"1", "2", "\"3"}},
		{"{  1,  \"a\\\"b\", 3\"2\"\\\\}", []string{"1", "a\"b", "32\\"}},
		{"{{1,2},{3},{4,5},{{6,7,8},{9}}}", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}},
		{
			"{{N,NU,NUL,NULL,NUL\\L,\\NULL,NULLL},{{NO,NUO,NULO,\"NULL\"},{ NULL}}}",
			[]string{"N", "NU", "NUL", "", "NULL", "NULL", "NULLL", "NO", "NUO", "NULO", "NULL", ""},
		},
		{"{}", nil},
		{"{,}", []string{""}},
	}
	for _, tc := range tests {
		got := ParsePgArrayStringToStringSlice(tc.input, ',')
		if !slices.Equal(got, tc.output) {
			t.Errorf("parsed %s to %v instead of %v", tc.input, got, tc.output)
		}
	}
}
