package conncockroachdb

import (
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestCrdbTypeToQValueKind(t *testing.T) {
	testCases := []struct {
		dataType string
		udtName  string
		expected types.QValueKind
	}{
		{"bigint", "int8", types.QValueKindInt64},
		{"integer", "int4", types.QValueKindInt32},
		{"smallint", "int2", types.QValueKindInt16},
		{"numeric", "numeric", types.QValueKindNumeric},
		{"double precision", "float8", types.QValueKindFloat64},
		{"real", "float4", types.QValueKindFloat32},
		{"boolean", "bool", types.QValueKindBoolean},
		{"character varying", "varchar", types.QValueKindString},
		{"text", "text", types.QValueKindString},
		{"bytes", "bytea", types.QValueKindBytes},
		{"uuid", "uuid", types.QValueKindUUID},
		{"jsonb", "jsonb", types.QValueKindJSON},
		{"timestamp without time zone", "timestamp", types.QValueKindTimestamp},
		{"timestamp with time zone", "timestamptz", types.QValueKindTimestampTZ},
		{"date", "date", types.QValueKindDate},
		{"time without time zone", "time", types.QValueKindTime},
		{"time with time zone", "timetz", types.QValueKindTimeTZ},
		{"interval", "interval", types.QValueKindInterval},
		{"inet", "inet", types.QValueKindINET},
		{"geography", "geography", types.QValueKindGeography},
		{"geometry", "geometry", types.QValueKindGeometry},
		{"USER-DEFINED", "my_enum", types.QValueKindEnum},
		{"vector", "vector", types.QValueKindString},
		{"ARRAY", "_int8", types.QValueKindArrayInt64},
		{"ARRAY", "_int4", types.QValueKindArrayInt32},
		{"ARRAY", "_int2", types.QValueKindArrayInt16},
		{"ARRAY", "_numeric", types.QValueKindArrayNumeric},
		{"ARRAY", "_float8", types.QValueKindArrayFloat64},
		{"ARRAY", "_float4", types.QValueKindArrayFloat32},
		{"ARRAY", "_bool", types.QValueKindArrayBoolean},
		{"ARRAY", "_uuid", types.QValueKindArrayUUID},
		{"ARRAY", "_jsonb", types.QValueKindArrayJSON},
		{"ARRAY", "_date", types.QValueKindArrayDate},
		{"ARRAY", "_timestamp", types.QValueKindArrayTimestamp},
		{"ARRAY", "_timestamptz", types.QValueKindArrayTimestampTZ},
		{"ARRAY", "_interval", types.QValueKindArrayInterval},
		{"ARRAY", "_text", types.QValueKindArrayString},
		{"ARRAY", "_my_enum", types.QValueKindArrayString},
	}

	for _, tc := range testCases {
		t.Run(tc.dataType+"/"+tc.udtName, func(t *testing.T) {
			require.Equal(t, tc.expected, crdbTypeToQValueKind(tc.dataType, tc.udtName))
		})
	}
}

func TestCrdbOIDToQValueKind(t *testing.T) {
	testCases := []struct {
		oid      uint32
		expected types.QValueKind
	}{
		{pgtype.BoolOID, types.QValueKindBoolean},
		{pgtype.Int2OID, types.QValueKindInt16},
		{pgtype.Int4OID, types.QValueKindInt32},
		{pgtype.Int8OID, types.QValueKindInt64},
		{pgtype.Float4OID, types.QValueKindFloat32},
		{pgtype.Float8OID, types.QValueKindFloat64},
		{pgtype.NumericOID, types.QValueKindNumeric},
		{pgtype.TextOID, types.QValueKindString},
		{pgtype.VarcharOID, types.QValueKindString},
		{pgtype.ByteaOID, types.QValueKindBytes},
		{pgtype.UUIDOID, types.QValueKindUUID},
		{pgtype.JSONBOID, types.QValueKindJSON},
		{pgtype.TimestampOID, types.QValueKindTimestamp},
		{pgtype.TimestamptzOID, types.QValueKindTimestampTZ},
		{pgtype.DateOID, types.QValueKindDate},
		{pgtype.TimeOID, types.QValueKindTime},
		{pgtype.TimetzOID, types.QValueKindTimeTZ},
		{pgtype.IntervalOID, types.QValueKindInterval},
		{pgtype.InetOID, types.QValueKindINET},
		{pgtype.Int8ArrayOID, types.QValueKindArrayInt64},
		{pgtype.UUIDArrayOID, types.QValueKindArrayUUID},
		{pgtype.TextArrayOID, types.QValueKindArrayString},
		{pgtype.JSONBArrayOID, types.QValueKindArrayJSON},
		{100500, types.QValueKindString}, // enums and other unknown types
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expected, crdbOIDToQValueKind(tc.oid), "oid %d", tc.oid)
	}
}

func TestQValueFromCrdbValue(t *testing.T) {
	typeMap := pgtype.NewMap()
	ts := time.Date(2024, 3, 15, 12, 30, 45, 123456000, time.UTC)
	testUUID := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

	field := func(kind types.QValueKind) types.QField {
		return types.QField{Name: "f", Type: kind, Nullable: true}
	}

	testCases := []struct {
		value    any
		expected types.QValue
		field    types.QField
		name     string
		oid      uint32
	}{
		{name: "null", field: field(types.QValueKindInt64), value: nil, expected: types.QValueNull(types.QValueKindInt64)},
		{name: "bool", field: field(types.QValueKindBoolean), value: true, expected: types.QValueBoolean{Val: true}},
		{name: "int16", field: field(types.QValueKindInt16), value: int16(1), expected: types.QValueInt16{Val: 1}},
		{name: "int32", field: field(types.QValueKindInt32), value: int32(2), expected: types.QValueInt32{Val: 2}},
		{name: "int64", field: field(types.QValueKindInt64), value: int64(3), expected: types.QValueInt64{Val: 3}},
		{
			name:  "oid as int64",
			field: field(types.QValueKindInt64), value: uint32(42),
			expected: types.QValueInt64{Val: 42},
		},
		{name: "float32", field: field(types.QValueKindFloat32), value: float32(1.5), expected: types.QValueFloat32{Val: 1.5}},
		{name: "float64", field: field(types.QValueKindFloat64), value: 2.5, expected: types.QValueFloat64{Val: 2.5}},
		{
			name:  "numeric",
			field: types.QField{Name: "f", Type: types.QValueKindNumeric, Nullable: true, Precision: 10, Scale: 2},
			value: pgtype.Numeric{Int: big.NewInt(12345), Exp: -2, Valid: true},
			expected: types.QValueNumeric{
				Val: decimal.NewFromBigInt(big.NewInt(12345), -2), Precision: 10, Scale: 2,
			},
		},
		{
			name:  "numeric nan is null when nullable",
			field: field(types.QValueKindNumeric),
			value: pgtype.Numeric{NaN: true, Valid: true},
			expected: types.QValueNull(
				types.QValueKindNumeric),
		},
		{name: "string", field: field(types.QValueKindString), value: "hello", oid: pgtype.TextOID, expected: types.QValueString{Val: "hello"}},
		{name: "enum", field: field(types.QValueKindEnum), value: "active", expected: types.QValueEnum{Val: "active"}},
		{name: "bytes", field: field(types.QValueKindBytes), value: []byte{1, 2}, expected: types.QValueBytes{Val: []byte{1, 2}}},
		{
			name:  "uuid from bytes",
			field: field(types.QValueKindUUID), value: [16]byte(testUUID),
			expected: types.QValueUUID{Val: testUUID},
		},
		{
			name:  "uuid from string",
			field: field(types.QValueKindUUID), value: testUUID.String(),
			expected: types.QValueUUID{Val: testUUID},
		},
		{name: "json", field: field(types.QValueKindJSON), value: `{"a":1}`, expected: types.QValueJSON{Val: `{"a":1}`}},
		{name: "timestamp", field: field(types.QValueKindTimestamp), value: ts, expected: types.QValueTimestamp{Val: ts}},
		{
			name:  "timestamp infinity is null when nullable",
			field: field(types.QValueKindTimestamp), value: pgtype.Infinity,
			expected: types.QValueNull(types.QValueKindTimestamp),
		},
		{name: "timestamptz", field: field(types.QValueKindTimestampTZ), value: ts, expected: types.QValueTimestampTZ{Val: ts}},
		{name: "date", field: field(types.QValueKindDate), value: ts, expected: types.QValueDate{Val: ts}},
		{
			name:  "time",
			field: field(types.QValueKindTime),
			value: pgtype.Time{Microseconds: 3723000001, Valid: true},
			expected: types.QValueTime{
				Val: 3723000001 * time.Microsecond,
			},
		},
		{
			name:  "inet",
			field: field(types.QValueKindINET), value: "192.168.0.1/24",
			expected: types.QValueINET{Val: "192.168.0.1/24"},
		},
		{
			name:  "array int64",
			field: field(types.QValueKindArrayInt64),
			value: []any{int64(1), int64(2)},
			expected: types.QValueArrayInt64{
				Val: []int64{1, 2},
			},
		},
		{
			name:  "array float64",
			field: field(types.QValueKindArrayFloat64),
			value: []any{1.5, 2.5},
			expected: types.QValueArrayFloat64{
				Val: []float64{1.5, 2.5},
			},
		},
		{
			name:  "array boolean",
			field: field(types.QValueKindArrayBoolean),
			value: []any{true, false},
			expected: types.QValueArrayBoolean{
				Val: []bool{true, false},
			},
		},
		{
			name:  "array timestamp",
			field: field(types.QValueKindArrayTimestamp),
			value: []any{ts},
			expected: types.QValueArrayTimestamp{
				Val: []time.Time{ts},
			},
		},
		{
			name:  "array uuid",
			field: field(types.QValueKindArrayUUID),
			value: []any{[16]byte(testUUID)},
			expected: types.QValueArrayUUID{
				Val: []uuid.UUID{testUUID},
			},
		},
		{
			name:  "array string from pg literal",
			field: field(types.QValueKindArrayString),
			value: "{a,b}",
			expected: types.QValueArrayString{
				Val: []string{"a", "b"},
			},
		},
		{
			name:  "array string from decoded values",
			field: field(types.QValueKindArrayString),
			value: []any{"a", "b"},
			expected: types.QValueArrayString{
				Val: []string{"a", "b"},
			},
		},
		{
			name:  "array numeric",
			field: field(types.QValueKindArrayNumeric),
			value: []any{pgtype.Numeric{Int: big.NewInt(5), Exp: 0, Valid: true}},
			expected: types.QValueArrayNumeric{
				Val: []decimal.Decimal{decimal.NewFromBigInt(big.NewInt(5), 0)},
			},
		},
		{
			name:  "unknown kind with string value",
			field: field(types.QValueKind("unknown")),
			value: "fallback",
			expected: types.QValueString{
				Val: "fallback",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qv, err := qvalueFromCrdbValue(tc.field, protos.DBType_CLICKHOUSE, tc.oid, typeMap, tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.expected, qv)
		})
	}
}

func TestQValueFromCrdbValueErrors(t *testing.T) {
	typeMap := pgtype.NewMap()
	field := types.QField{Name: "f", Type: types.QValueKindInt64, Nullable: false}

	_, err := qvalueFromCrdbValue(field, protos.DBType_CLICKHOUSE, 0, typeMap, "not an int")
	require.Error(t, err)
}

func TestParseTimeTZ(t *testing.T) {
	testCases := []struct {
		input    string
		expected time.Duration
	}{
		{"10:00:00+00", 10 * time.Hour},
		{"10:00:00+03", 7 * time.Hour},
		{"10:00:00-03:00", 13 * time.Hour},
		{"10:00:00.123456+00", 10*time.Hour + 123456*time.Microsecond},
		{"10:00:00+03:00:00", 7 * time.Hour},
		{"24:00:00+00", 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			d, err := parseTimeTZ(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, d)
		})
	}

	_, err := parseTimeTZ("not a time")
	require.Error(t, err)
}

func TestIntervalToString(t *testing.T) {
	interval := pgtype.Interval{
		Microseconds: 2*3600000000 + 30*60000000 + 1500000, // 2h30m1.5s
		Days:         3,
		Months:       14, // 1 year 2 months
		Valid:        true,
	}
	str, err := intervalToString(interval)
	require.NoError(t, err)
	require.JSONEq(t, `{"hours":2,"minutes":30,"seconds":1.5,"days":3,"months":2,"years":1,"valid":true}`, str)

	_, err = intervalToString(pgtype.Interval{})
	require.Error(t, err)
}

func TestNumericToDecimal(t *testing.T) {
	num, valid := numericToDecimal(pgtype.Numeric{Int: big.NewInt(31415), Exp: -4, Valid: true})
	require.True(t, valid)
	require.Equal(t, "3.1415", num.String())

	for _, invalid := range []pgtype.Numeric{
		{},
		{NaN: true, Valid: true},
		{InfinityModifier: pgtype.Infinity, Valid: true},
		{InfinityModifier: pgtype.NegativeInfinity, Valid: true},
	} {
		_, valid := numericToDecimal(invalid)
		require.False(t, valid)
	}
}

func TestParseUUIDArray(t *testing.T) {
	u1 := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	u2 := uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8")

	fromStrings, err := parseUUIDArray([]string{u1.String(), u2.String()})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{u1, u2}, fromStrings)

	fromBytes, err := parseUUIDArray([][16]byte{[16]byte(u1), [16]byte(u2)})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{u1, u2}, fromBytes)

	fromAny, err := parseUUIDArray([]any{[16]byte(u1), nil, u2.String()})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{u1, uuid.Nil, u2}, fromAny)

	_, err = parseUUIDArray(42)
	require.Error(t, err)
}
