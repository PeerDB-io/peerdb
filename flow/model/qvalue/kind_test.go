package qvalue

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Postgres 15+ accepts a numeric scale larger than its precision: numeric(3,5)
// holds values below 0.01, such as 0.00123. ClickHouse rejects Decimal(3, 5)
// ("Negative scales and scales larger than precision are not supported"), so the
// destination column needs a precision of at least the scale.
func TestToDWHColumnTypeNumericScaleAbovePrecision(t *testing.T) {
	t.Parallel()

	env := map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false"}
	for _, tc := range []struct {
		name      string
		precision int32
		scale     int32
		want      string
	}{
		{"numeric(10,2)", 10, 2, "Decimal(10, 2)"},
		{"numeric(5,5)", 5, 5, "Decimal(5, 5)"},
		{"numeric(3,5)", 3, 5, "Decimal(5, 5)"},
		{"numeric(1,20)", 1, 20, "Decimal(20, 20)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			column := &protos.FieldDescription{
				Name:         "n",
				Type:         string(types.QValueKindNumeric),
				TypeModifier: datatypes.MakeNumericTypmod(tc.precision, tc.scale),
			}
			colType, err := ToDWHColumnType(
				t.Context(), types.QValueKindNumeric, env, protos.DBType_CLICKHOUSE, nil, column, false, nil,
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, colType)

			// the Avro schema for the values has to agree with the column
			destType := GetNumericDestinationType(int16(tc.precision), int16(tc.scale), protos.DBType_CLICKHOUSE, false)
			require.GreaterOrEqual(t, destType.Precision, destType.Scale)
		})
	}
}
