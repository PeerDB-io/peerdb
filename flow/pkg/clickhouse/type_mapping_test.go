package clickhouse

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/pkg/datatypes"
	"github.com/PeerDB-io/peerdb/flow/pkg/postgres"
	"github.com/PeerDB-io/peerdb/flow/pkg/types"
)

func TestQValueKindToClickHouseType(t *testing.T) {
	unboundedNumeric := int32(-1)
	tests := []struct {
		name     string
		kind     types.QValueKind
		typmod   int32
		nullable bool
		opts     TypeMappingOptions
		expected string
	}{
		{name: "int64", kind: types.QValueKindInt64, typmod: -1, expected: "Int64"},
		{name: "unmapped kind falls back to String", kind: types.QValueKind("mystery"), typmod: -1, expected: "String"},
		{
			name: "bounded numeric", kind: types.QValueKindNumeric,
			typmod: datatypes.MakeNumericTypmod(10, 2), expected: "Decimal(10, 2)",
		},
		{
			name: "unbounded numeric", kind: types.QValueKindNumeric,
			typmod: unboundedNumeric, expected: "Decimal(76, 38)",
		},
		{
			name: "unbounded numeric as string", kind: types.QValueKindNumeric, typmod: unboundedNumeric,
			opts: TypeMappingOptions{NumericAsString: true}, expected: "String",
		},
		{
			name: "numeric precision beyond ClickHouse max", kind: types.QValueKindNumeric,
			typmod: datatypes.MakeNumericTypmod(100, 2), expected: "String",
		},
		{
			name: "numeric array", kind: types.QValueKindArrayNumeric,
			typmod: datatypes.MakeNumericTypmod(10, 2), expected: "Array(Decimal(10, 2))",
		},
		{name: "json without native json", kind: types.QValueKindJSON, typmod: -1, expected: "String"},
		{
			name: "json with native json", kind: types.QValueKindJSON, typmod: -1,
			opts: TypeMappingOptions{UseNativeJSONType: true}, expected: "JSON",
		},
		{
			name: "jsonb with native json", kind: types.QValueKindJSONB, typmod: -1,
			opts: TypeMappingOptions{UseNativeJSONType: true}, expected: "JSON",
		},
		{name: "time without time64", kind: types.QValueKindTime, typmod: -1, expected: "DateTime64(6)"},
		{
			name: "time with time64", kind: types.QValueKindTime, typmod: -1,
			opts: TypeMappingOptions{Time64Enabled: true}, expected: "Time64(6)",
		},
		{
			name: "timetz with time64", kind: types.QValueKindTimeTZ, typmod: -1,
			opts: TypeMappingOptions{Time64Enabled: true}, expected: "Time64(6)",
		},
		{
			name: "nullable disabled leaves type bare", kind: types.QValueKindString, typmod: -1,
			nullable: true, expected: "String",
		},
		{
			name: "nullable string", kind: types.QValueKindString, typmod: -1,
			nullable: true, opts: TypeMappingOptions{NullableEnabled: true}, expected: "Nullable(String)",
		},
		{
			name: "nullable enum keeps LowCardinality outside", kind: types.QValueKindEnum, typmod: -1,
			nullable: true, opts: TypeMappingOptions{NullableEnabled: true}, expected: "LowCardinality(Nullable(String))",
		},
		{
			name: "nullable array is not wrapped", kind: types.QValueKindArrayInt64, typmod: -1,
			nullable: true, opts: TypeMappingOptions{NullableEnabled: true}, expected: "Array(Int64)",
		},
		{
			name: "nullable numeric", kind: types.QValueKindNumeric, typmod: datatypes.MakeNumericTypmod(10, 2),
			nullable: true, opts: TypeMappingOptions{NullableEnabled: true}, expected: "Nullable(Decimal(10, 2))",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, QValueKindToClickHouseType(tt.kind, tt.typmod, tt.nullable, tt.opts))
		})
	}
}

func TestPostgresTypeToClickHouseType(t *testing.T) {
	typeMap := pgtype.NewMap()
	noCustomTypes := map[uint32]postgres.CustomDataType{}

	colType, err := PostgresTypeToClickHouseType(
		pgtype.Int8OID, -1, false, noCustomTypes, typeMap, common.InternalVersion_Latest, TypeMappingOptions{})
	require.NoError(t, err)
	require.Equal(t, "Int64", colType)

	colType, err = PostgresTypeToClickHouseType(
		pgtype.NumericOID, datatypes.MakeNumericTypmod(10, 2), false,
		noCustomTypes, typeMap, common.InternalVersion_Latest, TypeMappingOptions{})
	require.NoError(t, err)
	require.Equal(t, "Decimal(10, 2)", colType)

	colType, err = PostgresTypeToClickHouseType(
		pgtype.TimestamptzOID, -1, true, noCustomTypes, typeMap, common.InternalVersion_Latest,
		TypeMappingOptions{NullableEnabled: true})
	require.NoError(t, err)
	require.Equal(t, "Nullable(DateTime64(6))", colType)

	// unknown OID falls back to String without error
	colType, err = PostgresTypeToClickHouseType(
		999999, -1, false, noCustomTypes, typeMap, common.InternalVersion_Latest, TypeMappingOptions{})
	require.NoError(t, err)
	require.Equal(t, "String", colType)

	// custom enum type
	enumOID := uint32(999998)
	customTypes := map[uint32]postgres.CustomDataType{enumOID: {Name: "mood", Type: 'e'}}
	colType, err = PostgresTypeToClickHouseType(
		enumOID, -1, false, customTypes, typeMap, common.InternalVersion_Latest, TypeMappingOptions{})
	require.NoError(t, err)
	require.Equal(t, "LowCardinality(String)", colType)

	// pgvector maps to Array(Float32) from InternalVersion_PgVectorAsFloatArray on
	vectorOID := uint32(999997)
	vectorTypes := map[uint32]postgres.CustomDataType{vectorOID: {Name: "vector", Type: 'b'}}
	colType, err = PostgresTypeToClickHouseType(
		vectorOID, -1, false, vectorTypes, typeMap, common.InternalVersion_Latest, TypeMappingOptions{})
	require.NoError(t, err)
	require.Equal(t, "Array(Float32)", colType)

	colType, err = PostgresTypeToClickHouseType(
		vectorOID, -1, false, vectorTypes, typeMap, common.InternalVersion_First, TypeMappingOptions{})
	require.NoError(t, err)
	require.Equal(t, "String", colType)
}
