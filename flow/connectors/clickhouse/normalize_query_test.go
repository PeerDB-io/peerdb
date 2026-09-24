package connclickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
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

func TestExtendedTimeToDateTime(t *testing.T) {
	ctx := context.Background()
	addr := fmt.Sprintf("%s:%d", testutil.ClickHouseTestHost(), testutil.ClickHouseTestPort())
	ch, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	if err := ch.Ping(ctx); err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	defer ch.Close()

	testCases := []struct {
		name     string
		duration time.Duration
	}{
		{"zero", 0},
		{"normal", 1*time.Hour + 2*time.Minute + 3*time.Second + 123456*time.Microsecond},
		{"extended_hours", 838*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond},
		{"negative", -1 * time.Minute},
		{"negative_extended", -123*time.Hour - 45*time.Minute - 7*time.Second - 899999*time.Microsecond},
		{"precision_test", 18*time.Hour + 18*time.Minute + 8*time.Second + 511787*time.Microsecond},
		{"trailing_zeros_test", 18*time.Hour + 18*time.Minute + 8*time.Second + 500000*time.Microsecond},
	}

	for _, time64Supported := range []bool{false, true} {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/time64Supported=%v", tc.name, time64Supported), func(t *testing.T) {
				projectionExpr := extendedTimeToDateTime(
					fmt.Sprintf("'%s'", types.FormatExtendedTimeDuration(tc.duration)), time64Supported)

				var result time.Time
				require.NoError(t, ch.QueryRow(ctx, "SELECT "+projectionExpr).Scan(&result))
				expected := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(tc.duration)
				require.Equal(t, expected.UnixMicro(), result.UnixMicro())
			})
		}
		t.Run(fmt.Sprintf("%s/time64Supported=%v", "null", time64Supported), func(t *testing.T) {
			var result *time.Time
			require.NoError(t, ch.QueryRow(ctx, "SELECT "+extendedTimeToDateTime("null", time64Supported)).Scan(&result))
			require.Nil(t, result)
		})
	}
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
