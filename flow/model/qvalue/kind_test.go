package qvalue

import (
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TestToDWHColumnTypeClickHouseJSONArray checks the ClickHouse type of json[]/jsonb[] without a destination type.
func TestToDWHColumnTypeClickHouseJSONArray(t *testing.T) {
	jsonCapable := &chproto.Version{Major: 25, Minor: 8}
	jsonIncapable := &chproto.Version{Major: 25, Minor: 2}

	for _, tc := range []struct {
		chVersion      *chproto.Version
		name           string
		nativeJSON     string
		want           string
		kind           types.QValueKind
		version        uint32
		nullableSet    bool
		columnNullable bool
		// sourceType is the mirror's source peer type, Postgres when nil
		sourceType *protos.DBType
	}{
		{
			name: "json array, native json on", kind: types.QValueKindArrayJSON, nativeJSON: "true",
			chVersion: jsonCapable, version: shared.InternalVersion_Latest, columnNullable: true, want: "Array(JSON)",
		},
		{
			name: "jsonb array, native json on", kind: types.QValueKindArrayJSONB, nativeJSON: "true",
			chVersion: jsonCapable, version: shared.InternalVersion_JSONArrayAsArrayJSON, want: "Array(JSON)",
		},
		{
			name: "nullable setting on", kind: types.QValueKindArrayJSON, nativeJSON: "true",
			chVersion: jsonCapable, version: shared.InternalVersion_Latest, nullableSet: true, columnNullable: true,
			want: "Array(Nullable(JSON))",
		},
		{
			name: "nullable setting on, NOT NULL source column", kind: types.QValueKindArrayJSONB, nativeJSON: "true",
			chVersion: jsonCapable, version: shared.InternalVersion_Latest, nullableSet: true,
			want: "Array(Nullable(JSON))",
		},
		{
			name: "mirror from before the array mapping", kind: types.QValueKindArrayJSONB, nativeJSON: "true",
			chVersion: jsonCapable, version: shared.InternalVersion_JSONArrayAsArrayJSON - 1, nullableSet: true, want: "String",
		},
		{
			name: "native json off", kind: types.QValueKindArrayJSON, nativeJSON: "false",
			chVersion: jsonCapable, version: shared.InternalVersion_Latest, nullableSet: true, want: "String",
		},
		{
			name: "server without the JSON type", kind: types.QValueKindArrayJSON, nativeJSON: "true",
			chVersion: jsonIncapable, version: shared.InternalVersion_Latest, want: "String",
		},
		{
			name: "CockroachDB source, whose reader does not wrap array elements", kind: types.QValueKindArrayJSON,
			nativeJSON: "true", chVersion: jsonCapable, version: shared.InternalVersion_Latest,
			sourceType: new(protos.DBType_COCKROACHDB), want: "String",
		},
		{
			name: "Mongo source", kind: types.QValueKindArrayJSON, nativeJSON: "true",
			chVersion: jsonCapable, version: shared.InternalVersion_Latest, sourceType: new(protos.DBType_MONGO), want: "String",
		},
		{
			name: "scalar json is unchanged by the array version", kind: types.QValueKindJSON, nativeJSON: "true",
			chVersion: jsonCapable, version: shared.InternalVersion_JSONArrayAsArrayJSON - 1, nullableSet: true, columnNullable: true,
			want: "Nullable(JSON)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": tc.nativeJSON}
			column := &protos.FieldDescription{Name: "c", Type: string(tc.kind), TypeModifier: -1, Nullable: tc.columnNullable}
			sourceType := protos.DBType_POSTGRES
			if tc.sourceType != nil {
				sourceType = *tc.sourceType
			}
			got, err := ToDWHColumnType(t.Context(), tc.kind, env, protos.DBType_CLICKHOUSE, tc.chVersion, column,
				tc.nullableSet, []string{shared.Flag_ClickHouseJSONSupported}, tc.version, sourceType)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
