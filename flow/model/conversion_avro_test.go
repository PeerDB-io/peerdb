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

// TestAvroSchemaNumericScaleAbovePrecision reproduces https://github.com/PeerDB-io/peerdb/pull/4989#issuecomment-6809919726:
// PostgreSQL accepts NUMERIC(2,4), with a scale above the precision, while an Avro decimal requires scale <= precision.
// The schema has to be definable whatever the values, as NULLs and empty arrays need it too. Like Postgres,
// which stores values from -0.0099 to 0.0099 in such a column, the precision is widened to the scale.
func TestAvroSchemaNumericScaleAbovePrecision(t *testing.T) {
	fields := []types.QField{
		// NUMERIC(2,4), nullable: replicated NULL in the review's repro
		{Name: "scalar", Type: types.QValueKindNumeric, Precision: 2, Scale: 4, Nullable: true},
		// NUMERIC(2,4)[] NOT NULL: replicated '{}' in the review's repro
		{Name: "array", Type: types.QValueKindArrayNumeric, Precision: 2, Scale: 4},
	}
	env := map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false"}
	for _, targetDWH := range []protos.DBType{
		protos.DBType_CLICKHOUSE, protos.DBType_S3, protos.DBType_SNOWFLAKE, protos.DBType_BIGQUERY,
	} {
		for _, field := range fields {
			t.Run(targetDWH.String()+"/"+field.Name, func(t *testing.T) {
				definition, err := GetAvroSchemaDefinition(t.Context(), env, "numeric_scale_above_precision",
					types.QRecordSchema{Fields: []types.QField{field}}, targetDWH, nil)
				require.NoError(t, err)
				require.Contains(t, definition.Schema.String(), `"logicalType":"decimal","precision":4,"scale":4`)
			})
		}
	}
}
