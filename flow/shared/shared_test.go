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
			// initial partitions = ceil(100000 / 50) = 2000, which is > 1000.
			// ratio = 100000 / (50 * 1000) = 2,
			// multiplier = 10^ceil(log10(2)) = 10,
			// adjustedRowsPerPartition = 50 * 10 = 500,
			// adjusted partitions = ceil(100000 / 500) = 200.
			expectedPartitions:       200,
			expectedRowsPerPartition: 500,
		},
		{
			name:                    "Large totalRows exactly max",
			totalRows:               1000000000, // 1e9
			desiredRowsPerPartition: 100000,
			// initial partitions = 1e9 / 100000 = 10000.
			// ratio = 1e9 / (100000 * 1000) = 10,
			// multiplier = 10^ceil(log10(10)) = 10,
			// adjustedRowsPerPartition = 100000 * 10 = 1e6,
			// adjusted partitions = 1e9 / 1e6 = 1000.
			expectedPartitions:       1000,
			expectedRowsPerPartition: 1000000,
		},
		{
			name:                    "Large totalRows with multiplier",
			totalRows:               1000000000, // 1e9
			desiredRowsPerPartition: 50000,
			// initial partitions = 1e9 / 50000 = 20000.
			// ratio = 1e9 / (50000 * 1000) = 20,
			// multiplier = 10^ceil(log10(20)) = 100,
			// adjustedRowsPerPartition = 50000 * 100 = 5e6,
			// adjusted partitions = 1e9 / 5e6 = 200.
			expectedPartitions:       200,
			expectedRowsPerPartition: 5000000,
		},
		{
			name:                    "Moderate values",
			totalRows:               500000,
			desiredRowsPerPartition: 250,
			// initial partitions = ceil(500000 / 250) = 2000.
			// ratio = 500000 / (250 * 1000) = 2,
			// multiplier = 10,
			// adjustedRowsPerPartition = 250 * 10 = 2500,
			// adjusted partitions = ceil(500000 / 2500) = 200.
			expectedPartitions:       200,
			expectedRowsPerPartition: 2500,
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
			// initial partitions = ceil(987654321 / 80001) ≈ 12346.
			// ratio = 987654321 / (80001 * 1000) ≈ 12.345,
			// multiplier = 10^ceil(log10(12.345)) = 100,
			// adjustedRowsPerPartition = 80001 * 100,
			// adjusted partitions = ceil(987654321 / 8000100) ≈ 124.
			expectedPartitions:       124,
			expectedRowsPerPartition: 8000100,
		},
		{
			name:                    "Desired rows per partition greater than totalRows",
			totalRows:               100,
			desiredRowsPerPartition: 150,
			// initial partitions = ceil(100/150) = 1, which is valid.
			expectedPartitions:       1,
			expectedRowsPerPartition: 150,
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

func TestParsePgArray(t *testing.T) {
	tests := []struct {
		input  string
		output []string
	}{
		{"[1:2]{1,2,\\\"3}", []string{"1", "2", "\"3"}},
		{"{  1,  \"a\\\"b\", 3\"2\"\\\\}", []string{"1", "a\"b", "32\\"}},
		{"{{1,2},{3},{4,5},{{6,7,8},{9}}}", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}},
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
