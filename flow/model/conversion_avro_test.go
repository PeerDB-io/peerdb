package model

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestAvroSchemaMultipleNamedTypeColumns(t *testing.T) {
	namedKinds := []types.QValueKind{
		types.QValueKindInt256,
		types.QValueKindUInt256,
	}

	for _, kind := range namedKinds {
		t.Run(string(kind), func(t *testing.T) {
			fields := make([]types.QField, 5)
			for i := range fields {
				fields[i] = types.QField{
					Name: fmt.Sprintf("col_%d", i),
					Type: kind,
				}
			}
			schema := types.QRecordSchema{Fields: fields}
			_, err := GetAvroSchemaDefinition(
				context.Background(), nil, "test_table", schema, protos.DBType_CLICKHOUSE, nil,
			)
			require.NoError(t, err)
		})
	}
}
