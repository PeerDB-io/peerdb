package connclickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TestBuildQueryNullableDestinationTypeOverride checks the normalize query agrees with the table DDL on
// nullability for `destination_type` overrides: with nullability enabled, the DDL creates the column as
// Nullable(<type>) so the query has to extract Nullable(<type>) too, or JSON nulls turn into the type's
// default value. An override already spelled Nullable(...) is used as is on both sides.
func TestBuildQueryNullableDestinationTypeOverride(t *testing.T) {
	schema := &protos.TableSchema{
		TableIdentifier:   "src.t1",
		PrimaryKeyColumns: []string{"id"},
		System:            protos.TypeSystem_Q,
		NullableEnabled:   true,
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindString), TypeModifier: -1},
			{Name: "num", Type: string(types.QValueKindInt64), TypeModifier: -1, Nullable: true},
			{Name: "tag", Type: string(types.QValueKindString), TypeModifier: -1, Nullable: true},
		},
	}
	tableMapping := &protos.TableMapping{
		SourceTableIdentifier:      "src.t1",
		DestinationTableIdentifier: "t1_dst",
		Columns: []*protos.ColumnSetting{
			{SourceName: "num", DestinationType: "Int64"},
			{SourceName: "tag", DestinationType: "Nullable(String)"},
		},
	}

	query, err := NewNormalizeQueryGenerator(
		"t1_dst",
		map[string]*protos.TableSchema{"t1_dst": schema},
		[]*protos.TableMapping{tableMapping},
		1, 0,
		false, false,
		nil, "_peerdb_raw_t1", nil, false, "", 0, nil,
	).BuildQuery(t.Context())
	require.NoError(t, err)

	// nullable-enabled override extracts as Nullable, matching the Nullable(Int64) column the DDL creates
	require.Contains(t, query, `JSONExtract(_peerdb_data, 'num', 'Nullable(Int64)') AS `+"`num`")
	// an already-Nullable override is used verbatim, not double wrapped
	require.Contains(t, query, `JSONExtract(_peerdb_data, 'tag', 'Nullable(String)') AS `+"`tag`")
	require.NotContains(t, query, "Nullable(Nullable(")
}
