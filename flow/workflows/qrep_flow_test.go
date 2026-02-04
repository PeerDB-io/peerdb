package peerflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func makePartitions(n int) []*protos.QRepPartition {
	partitions := make([]*protos.QRepPartition, n)
	for i := range partitions {
		partitions[i] = &protos.QRepPartition{PartitionId: string(rune('A' + i))}
	}
	return partitions
}

func TestDistributePartitions(t *testing.T) {
	//nolint:govet // it's a test, no need for fieldalignment
	tests := []struct {
		name       string
		numParts   int
		numBatches int
		expected   [][]string
	}{
		{
			name:       "12 partitions 3 batches",
			numParts:   12,
			numBatches: 3,
			expected:   [][]string{{"A", "D", "G", "J"}, {"B", "E", "H", "K"}, {"C", "F", "I", "L"}},
		},
		{
			name:       "10 partitions 3 batches",
			numParts:   10,
			numBatches: 3,
			expected:   [][]string{{"A", "D", "G", "J"}, {"B", "E", "H"}, {"C", "F", "I"}},
		},
		{
			name:       "5 partitions 2 batches",
			numParts:   5,
			numBatches: 2,
			expected:   [][]string{{"A", "C", "E"}, {"B", "D"}},
		},
		{
			name:       "3 partitions 5 batches",
			numParts:   3,
			numBatches: 5,
			expected:   [][]string{{"A"}, {"B"}, {"C"}},
		},
		{
			name:       "0 partitions",
			numParts:   0,
			numBatches: 3,
			expected:   nil,
		},
		{
			name:       "0 batches",
			numParts:   5,
			numBatches: 0,
			expected:   nil,
		},
		{
			name:       "1 partition 3 batches",
			numParts:   1,
			numBatches: 3,
			expected:   [][]string{{"A"}},
		},
		{
			name:       "5 partitions 1 batch",
			numParts:   5,
			numBatches: 1,
			expected:   [][]string{{"A", "B", "C", "D", "E"}},
		},
		{
			name:       "5 partitions 5 batches",
			numParts:   5,
			numBatches: 5,
			expected:   [][]string{{"A"}, {"B"}, {"C"}, {"D"}, {"E"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			partitions := makePartitions(tc.numParts)
			batches := distributePartitions(partitions, tc.numBatches)

			if tc.expected == nil {
				require.Nil(t, batches)
				return
			}

			require.Len(t, batches, len(tc.expected))
			for i, batch := range batches {
				require.Len(t, batch, len(tc.expected[i]))
				for j, p := range batch {
					require.Equal(t, tc.expected[i][j], p.PartitionId)
				}
			}
		})
	}
}
