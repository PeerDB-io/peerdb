package connclickhouse

import (
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
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

// TestBuildQueryNullableJSON checks the projection for native JSON columns.
// JSONExtractString yields `""` both for a JSON null and for a missing field so `"" ::JSON` errors.
// Nullable(JSON) column has to route `""` to NULL.
// A non-nullable JSON column keeps the plain ::JSON cast.
func TestBuildQueryNullableJSON(t *testing.T) {
	const (
		nullableProjection = `CAST(nullIf(JSONExtractString(_peerdb_data, 'payload'), ''), 'Nullable(JSON)') AS ` + "`payload`"
		nullableUpdate     = `CAST(nullIf(JSONExtractString(_peerdb_match_data, 'payload'), ''), 'Nullable(JSON)') AS ` + "`payload`"
		plainProjection    = `JSONExtractString(_peerdb_data, 'payload')::JSON AS ` + "`payload`"
		plainUpdate        = `JSONExtractString(_peerdb_match_data, 'payload')::JSON AS ` + "`payload`"
	)
	// ClickHouse 25.8 supports the native JSON type
	chVersion := &chproto.Version{Major: 25, Minor: 8}
	jsonEnabled := map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}

	newSchema := func(nullable bool) map[string]*protos.TableSchema {
		return map[string]*protos.TableSchema{"t1_dst": {
			TableIdentifier:   "src.t1",
			PrimaryKeyColumns: []string{"id"},
			System:            protos.TypeSystem_Q,
			NullableEnabled:   nullable,
			Columns: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindString), TypeModifier: -1},
				{Name: "payload", Type: string(types.QValueKindJSON), TypeModifier: -1, Nullable: nullable},
			},
		}}
	}
	newMapping := func(destinationType string) []*protos.TableMapping {
		mapping := &protos.TableMapping{
			SourceTableIdentifier:      "src.t1",
			DestinationTableIdentifier: "t1_dst",
		}
		if destinationType != "" {
			mapping.Columns = []*protos.ColumnSetting{{SourceName: "payload", DestinationType: destinationType}}
		}
		return []*protos.TableMapping{mapping}
	}
	build := func(t *testing.T, schema map[string]*protos.TableSchema, mappings []*protos.TableMapping,
		enablePrimaryUpdate bool, env map[string]string,
	) string {
		t.Helper()
		query, err := NewNormalizeQueryGenerator(
			"t1_dst", schema, mappings,
			1, 0,
			enablePrimaryUpdate, false,
			env, "_peerdb_raw_t1", chVersion, false, "", shared.InternalVersion_Latest, nil,
		).BuildQuery(t.Context())
		require.NoError(t, err)
		return query
	}

	t.Run("nullable JSON column via type mapping", func(t *testing.T) {
		query := build(t, newSchema(true), newMapping(""), false, jsonEnabled)
		require.Contains(t, query, nullableProjection)
		require.NotContains(t, query, "::JSON")
	})

	t.Run("nullable JSON column via type mapping with primary update", func(t *testing.T) {
		query := build(t, newSchema(true), newMapping(""), true, jsonEnabled)
		require.Contains(t, query, nullableProjection)
		require.Contains(t, query, nullableUpdate)
		require.NotContains(t, query, "::JSON")
	})

	t.Run("nullable JSON column via destination_type override", func(t *testing.T) {
		// JSON override on a nullable-enabled column is wrapped to Nullable(JSON), like the DDL does
		query := build(t, newSchema(true), newMapping("JSON"), true, nil)
		require.Contains(t, query, nullableProjection)
		require.Contains(t, query, nullableUpdate)
		require.NotContains(t, query, "Nullable(Nullable(")

		// an explicit Nullable(JSON) override is used as is
		query = build(t, newSchema(true), newMapping("Nullable(JSON)"), true, nil)
		require.Contains(t, query, nullableProjection)
		require.Contains(t, query, nullableUpdate)
		require.NotContains(t, query, "Nullable(Nullable(")
	})

	t.Run("non-nullable JSON column keeps plain cast", func(t *testing.T) {
		query := build(t, newSchema(false), newMapping(""), true, jsonEnabled)
		require.Contains(t, query, plainProjection)
		require.Contains(t, query, plainUpdate)
		require.NotContains(t, query, "Nullable(JSON)")

		query = build(t, newSchema(false), newMapping("JSON"), true, nil)
		require.Contains(t, query, plainProjection)
		require.Contains(t, query, plainUpdate)
		require.NotContains(t, query, "Nullable(JSON)")
	})
}
