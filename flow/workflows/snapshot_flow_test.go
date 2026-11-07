package peerflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestPartitionKeyMayBeNull(t *testing.T) {
	schema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Nullable: false},
			{Name: "updated_at", Nullable: true},
		},
	}

	tests := []struct {
		name         string
		partitionKey string
		exclude      []string
		expected     bool
	}{
		{name: "nullable key", partitionKey: "updated_at", expected: true},
		{name: "not null key", partitionKey: "id", expected: false},
		{name: "excluded key", partitionKey: "created_at", exclude: []string{"created_at"}, expected: true},
		{name: "unrelated exclusion", partitionKey: "id", exclude: []string{"created_at"}, expected: false},
		{name: "ctid", partitionKey: "ctid", expected: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mapping := &protos.TableMapping{PartitionKey: tc.partitionKey, Exclude: tc.exclude}
			require.Equal(t, tc.expected, partitionKeyMayBeNull(schema, mapping))
		})
	}
}
