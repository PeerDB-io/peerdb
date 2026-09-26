package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Postgres 15+ accepts a negative numeric scale: numeric(5,-2) rounds to hundreds
// and stores 12300. Postgres keeps the scale in the low 11 bits of the typmod as a
// signed value, so its atttypmod is 329730 = ((5 << 16) | (-2 & 0x7ff)) + 4.
func TestNumericTypmodNegativeScale(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		typmod int32
		want   string
	}{
		{"numeric(10,2)", 655366, "numeric(10,2)"},
		{"numeric(3,5)", 196617, "numeric(3,5)"},
		{"numeric(5,-2)", 329730, "numeric(5,-2)"},
		{"numeric(1000,-1000)", 65537052, "numeric(1000,-1000)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			column := &protos.FieldDescription{Name: "n", Type: "numeric", TypeModifier: tc.typmod, Nullable: true}
			sql := generateCreateTableSQLForNormalizedTable(
				&protos.SetupNormalizedTableBatchInput{},
				&common.QualifiedTable{Namespace: "public", Table: "t"},
				&protos.TableSchema{System: protos.TypeSystem_PG, Columns: []*protos.FieldDescription{column}},
			)
			require.Contains(t, sql, `"n" `+tc.want)

			// ClickHouse has no negative-scale Decimal; it must still get a valid type.
			chType, err := qvalue.ToDWHColumnType(t.Context(), types.QValueKindNumeric,
				map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false"},
				protos.DBType_CLICKHOUSE, nil, column, false, nil)
			require.NoError(t, err)
			require.NotContains(t, chType, "-")
		})
	}
}
