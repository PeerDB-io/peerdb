package connbigquery

import (
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestFieldNormalizedTypeName(t *testing.T) {
	tests := []struct {
		name     string
		field    *bigquery.FieldSchema
		expected string
	}{
		// Basic types
		{
			name:     "string",
			field:    &bigquery.FieldSchema{Type: bigquery.StringFieldType},
			expected: "STRING",
		},
		{
			name:     "integer",
			field:    &bigquery.FieldSchema{Type: bigquery.IntegerFieldType},
			expected: "INTEGER",
		},
		{
			name:     "float converts to FLOAT64",
			field:    &bigquery.FieldSchema{Type: bigquery.FloatFieldType},
			expected: "FLOAT64",
		},
		{
			name:     "boolean converts to BOOL",
			field:    &bigquery.FieldSchema{Type: bigquery.BooleanFieldType},
			expected: "BOOL",
		},
		{
			name:     "timestamp",
			field:    &bigquery.FieldSchema{Type: bigquery.TimestampFieldType},
			expected: "TIMESTAMP",
		},
		{
			name:     "date",
			field:    &bigquery.FieldSchema{Type: bigquery.DateFieldType},
			expected: "DATE",
		},
		{
			name:     "time",
			field:    &bigquery.FieldSchema{Type: bigquery.TimeFieldType},
			expected: "TIME",
		},
		{
			name:     "bytes",
			field:    &bigquery.FieldSchema{Type: bigquery.BytesFieldType},
			expected: "BYTES",
		},
		{
			name:     "geography",
			field:    &bigquery.FieldSchema{Type: bigquery.GeographyFieldType},
			expected: "GEOGRAPHY",
		},
		{
			name:     "json",
			field:    &bigquery.FieldSchema{Type: bigquery.JSONFieldType},
			expected: "JSON",
		},

		// STRING with MaxLength
		{
			name:     "string with max length",
			field:    &bigquery.FieldSchema{Type: bigquery.StringFieldType, MaxLength: 100},
			expected: "STRING(100)",
		},
		{
			name:     "string with max length 255",
			field:    &bigquery.FieldSchema{Type: bigquery.StringFieldType, MaxLength: 255},
			expected: "STRING(255)",
		},

		// BYTES with MaxLength
		{
			name:     "bytes with max length",
			field:    &bigquery.FieldSchema{Type: bigquery.BytesFieldType, MaxLength: 1024},
			expected: "BYTES(1024)",
		},

		// NUMERIC with Precision and Scale
		{
			name:     "numeric without precision",
			field:    &bigquery.FieldSchema{Type: bigquery.NumericFieldType},
			expected: "NUMERIC",
		},
		{
			name:     "numeric with precision only",
			field:    &bigquery.FieldSchema{Type: bigquery.NumericFieldType, Precision: 38},
			expected: "NUMERIC(38)",
		},
		{
			name:     "numeric with precision and scale",
			field:    &bigquery.FieldSchema{Type: bigquery.NumericFieldType, Precision: 38, Scale: 9},
			expected: "NUMERIC(38,9)",
		},
		{
			name:     "numeric with custom precision and scale",
			field:    &bigquery.FieldSchema{Type: bigquery.NumericFieldType, Precision: 10, Scale: 2},
			expected: "NUMERIC(10,2)",
		},

		// BIGNUMERIC with Precision and Scale
		{
			name:     "bignumeric without precision",
			field:    &bigquery.FieldSchema{Type: bigquery.BigNumericFieldType},
			expected: "BIGNUMERIC",
		},
		{
			name:     "bignumeric with precision only",
			field:    &bigquery.FieldSchema{Type: bigquery.BigNumericFieldType, Precision: 76},
			expected: "BIGNUMERIC(76)",
		},
		{
			name:     "bignumeric with precision and scale",
			field:    &bigquery.FieldSchema{Type: bigquery.BigNumericFieldType, Precision: 76, Scale: 38},
			expected: "BIGNUMERIC(76,38)",
		},

		// Repeated (array) types
		{
			name:     "repeated string",
			field:    &bigquery.FieldSchema{Type: bigquery.StringFieldType, Repeated: true},
			expected: "ARRAY<STRING>",
		},
		{
			name:     "repeated integer",
			field:    &bigquery.FieldSchema{Type: bigquery.IntegerFieldType, Repeated: true},
			expected: "ARRAY<INTEGER>",
		},
		{
			name:     "repeated float converts to FLOAT64",
			field:    &bigquery.FieldSchema{Type: bigquery.FloatFieldType, Repeated: true},
			expected: "ARRAY<FLOAT64>",
		},
		{
			name:     "repeated boolean converts to BOOL",
			field:    &bigquery.FieldSchema{Type: bigquery.BooleanFieldType, Repeated: true},
			expected: "ARRAY<BOOL>",
		},

		// Repeated with MaxLength/Precision
		{
			name:     "repeated string with max length",
			field:    &bigquery.FieldSchema{Type: bigquery.StringFieldType, MaxLength: 50, Repeated: true},
			expected: "ARRAY<STRING(50)>",
		},
		{
			name:     "repeated numeric with precision and scale",
			field:    &bigquery.FieldSchema{Type: bigquery.NumericFieldType, Precision: 18, Scale: 4, Repeated: true},
			expected: "ARRAY<NUMERIC(18,4)>",
		},
		{
			name:     "repeated bignumeric with precision and scale",
			field:    &bigquery.FieldSchema{Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 10, Repeated: true},
			expected: "ARRAY<BIGNUMERIC(38,10)>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fieldNormalizedTypeName(tt.field)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBigQueryTypeToQValueKind(t *testing.T) {
	tests := []struct {
		name     string
		field    *bigquery.FieldSchema
		expected types.QValueKind
	}{
		{
			name:     "string",
			field:    &bigquery.FieldSchema{Type: bigquery.StringFieldType},
			expected: types.QValueKindString,
		},
		{
			name:     "integer",
			field:    &bigquery.FieldSchema{Type: bigquery.IntegerFieldType},
			expected: types.QValueKindInt64,
		},
		{
			name:     "float",
			field:    &bigquery.FieldSchema{Type: bigquery.FloatFieldType},
			expected: types.QValueKindFloat64,
		},
		{
			name:     "boolean",
			field:    &bigquery.FieldSchema{Type: bigquery.BooleanFieldType},
			expected: types.QValueKindBoolean,
		},
		{
			name:     "timestamp",
			field:    &bigquery.FieldSchema{Type: bigquery.TimestampFieldType},
			expected: types.QValueKindTimestampTZ,
		},
		{
			name:     "datetime is zoneless",
			field:    &bigquery.FieldSchema{Type: bigquery.DateTimeFieldType},
			expected: types.QValueKindTimestamp,
		},
		{
			name:     "date",
			field:    &bigquery.FieldSchema{Type: bigquery.DateFieldType},
			expected: types.QValueKindDate,
		},
		{
			name:     "time",
			field:    &bigquery.FieldSchema{Type: bigquery.TimeFieldType},
			expected: types.QValueKindTime,
		},
		{
			name:     "bytes",
			field:    &bigquery.FieldSchema{Type: bigquery.BytesFieldType},
			expected: types.QValueKindBytes,
		},
		{
			name:     "numeric",
			field:    &bigquery.FieldSchema{Type: bigquery.NumericFieldType},
			expected: types.QValueKindNumeric,
		},
		{
			name:     "bignumeric",
			field:    &bigquery.FieldSchema{Type: bigquery.BigNumericFieldType},
			expected: types.QValueKindNumeric,
		},
		{
			name:     "geography",
			field:    &bigquery.FieldSchema{Type: bigquery.GeographyFieldType},
			expected: types.QValueKindGeography,
		},
		{
			name:     "json",
			field:    &bigquery.FieldSchema{Type: bigquery.JSONFieldType},
			expected: types.QValueKindJSON,
		},
		{
			name:     "record",
			field:    &bigquery.FieldSchema{Type: bigquery.RecordFieldType},
			expected: types.QValueKindJSON,
		},
		// repeated (array) types
		{
			name:     "repeated string",
			field:    &bigquery.FieldSchema{Type: bigquery.StringFieldType, Repeated: true},
			expected: types.QValueKindArrayString,
		},
		{
			name:     "repeated bytes use base64 strings",
			field:    &bigquery.FieldSchema{Type: bigquery.BytesFieldType, Repeated: true},
			expected: types.QValueKindArrayString,
		},
		{
			name:     "repeated integer",
			field:    &bigquery.FieldSchema{Type: bigquery.IntegerFieldType, Repeated: true},
			expected: types.QValueKindArrayInt64,
		},
		{
			name:     "repeated float",
			field:    &bigquery.FieldSchema{Type: bigquery.FloatFieldType, Repeated: true},
			expected: types.QValueKindArrayFloat64,
		},
		{
			name:     "repeated boolean",
			field:    &bigquery.FieldSchema{Type: bigquery.BooleanFieldType, Repeated: true},
			expected: types.QValueKindArrayBoolean,
		},
		{
			name:     "repeated timestamp",
			field:    &bigquery.FieldSchema{Type: bigquery.TimestampFieldType, Repeated: true},
			expected: types.QValueKindArrayTimestampTZ,
		},
		{
			name:     "repeated date",
			field:    &bigquery.FieldSchema{Type: bigquery.DateFieldType, Repeated: true},
			expected: types.QValueKindArrayDate,
		},
		{
			name:     "repeated datetime is zoneless",
			field:    &bigquery.FieldSchema{Type: bigquery.DateTimeFieldType, Repeated: true},
			expected: types.QValueKindArrayTimestamp,
		},
		{
			name:     "repeated time",
			field:    &bigquery.FieldSchema{Type: bigquery.TimeFieldType, Repeated: true},
			expected: types.QValueKindArrayTime,
		},
		{
			name:     "repeated numeric",
			field:    &bigquery.FieldSchema{Type: bigquery.NumericFieldType, Repeated: true},
			expected: types.QValueKindArrayNumeric,
		},
		{
			name:     "repeated record",
			field:    &bigquery.FieldSchema{Type: bigquery.RecordFieldType, Repeated: true},
			expected: types.QValueKindArrayString,
		},
		{
			name:     "repeated geography uses WKT strings",
			field:    &bigquery.FieldSchema{Type: bigquery.GeographyFieldType, Repeated: true},
			expected: types.QValueKindArrayString,
		},
		{
			name:     "repeated json",
			field:    &bigquery.FieldSchema{Type: bigquery.JSONFieldType, Repeated: true},
			expected: types.QValueKindArrayJSON,
		},
		{
			name:     "interval",
			field:    &bigquery.FieldSchema{Type: bigquery.IntervalFieldType},
			expected: types.QValueKindInterval,
		},
		{
			name:     "repeated interval",
			field:    &bigquery.FieldSchema{Type: bigquery.IntervalFieldType, Repeated: true},
			expected: types.QValueKindArrayInterval,
		},
		{
			name:     "range uses canonical string",
			field:    &bigquery.FieldSchema{Type: bigquery.RangeFieldType},
			expected: types.QValueKindString,
		},
		{
			name:     "repeated range uses canonical strings",
			field:    &bigquery.FieldSchema{Type: bigquery.RangeFieldType, Repeated: true},
			expected: types.QValueKindArrayString,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BigQueryTypeToQValueKind(tt.field)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQValueFromBigQueryValue(t *testing.T) {
	qfield := func(bqType bigquery.FieldType, repeated bool, precision, scale int16) types.QField {
		return BigQueryFieldToQField(&bigquery.FieldSchema{
			Type: bqType, Repeated: repeated, Precision: int64(precision), Scale: int64(scale),
		})
	}
	convert := func(field types.QField, value bigquery.Value) (types.QValue, error) {
		return qvalueFromBigQueryValue(field, value, &bigquery.FieldSchema{
			Name:     field.Name,
			Type:     bigquery.FieldType(field.OriginalType),
			Repeated: field.Type.IsArray(),
		})
	}

	t.Run("null returns a kind-tagged QValueNull", func(t *testing.T) {
		f := qfield(bigquery.IntegerFieldType, false, 0, 0)
		v, err := convert(f, nil)
		require.NoError(t, err)
		assert.Equal(t, types.QValueNull(types.QValueKindInt64), v)
	})

	t.Run("scalar types", func(t *testing.T) {
		ts := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
		tests := []struct {
			name     string
			field    types.QField
			value    bigquery.Value
			expected types.QValue
		}{
			{"bool", qfield(bigquery.BooleanFieldType, false, 0, 0), true, types.QValueBoolean{Val: true}},
			{"int64", qfield(bigquery.IntegerFieldType, false, 0, 0), int64(42), types.QValueInt64{Val: 42}},
			{"float64", qfield(bigquery.FloatFieldType, false, 0, 0), 3.14, types.QValueFloat64{Val: 3.14}},
			{"bytes", qfield(bigquery.BytesFieldType, false, 0, 0), []byte("hi"), types.QValueBytes{Val: []byte("hi")}},
			{"string", qfield(bigquery.StringFieldType, false, 0, 0), "hello", types.QValueString{Val: "hello"}},
			{"json", qfield(bigquery.JSONFieldType, false, 0, 0), `{"a":1}`, types.QValueJSON{Val: `{"a":1}`}},
			{
				"geography",
				qfield(bigquery.GeographyFieldType, false, 0, 0),
				"POINT(1 1)",
				types.QValueGeography{Val: "POINT(1 1)"},
			},
			{"timestamp", qfield(bigquery.TimestampFieldType, false, 0, 0), ts, types.QValueTimestampTZ{Val: ts}},
			{
				"datetime carried as UTC timestamp",
				qfield(bigquery.DateTimeFieldType, false, 0, 0),
				civil.DateTimeOf(ts),
				types.QValueTimestamp{Val: ts},
			},
			{
				"date",
				qfield(bigquery.DateFieldType, false, 0, 0),
				civil.Date{Year: 2026, Month: 8, Day: 14},
				types.QValueDate{Val: time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)},
			},
			{
				"time",
				qfield(bigquery.TimeFieldType, false, 0, 0),
				civil.Time{Hour: 1, Minute: 2, Second: 3, Nanosecond: 4000},
				types.QValueTime{Val: time.Hour + 2*time.Minute + 3*time.Second + 4000*time.Nanosecond},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				v, err := convert(tt.field, tt.value)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, v)
			})
		}
	})

	t.Run("numeric converts *big.Rat honoring field scale", func(t *testing.T) {
		f := qfield(bigquery.NumericFieldType, false, 10, 2)
		v, err := convert(f, big.NewRat(1, 4)) // 0.25
		require.NoError(t, err)
		numeric, ok := v.(types.QValueNumeric)
		require.True(t, ok)
		assert.True(t, decimal.NewFromFloat(0.25).Equal(numeric.Val))
		assert.Equal(t, int16(10), numeric.Precision)
		assert.Equal(t, int16(2), numeric.Scale)
	})

	t.Run("unparameterized numeric types use BigQuery default precision and scale", func(t *testing.T) {
		tests := []struct {
			name      string
			fieldType bigquery.FieldType
			value     string
			precision int16
			scale     int16
		}{
			{"numeric", bigquery.NumericFieldType, "0.123456789", 38, 9},
			{"bignumeric", bigquery.BigNumericFieldType, "0.12345678901234567890123456789012345678", 76, 38},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rat, ok := new(big.Rat).SetString(tt.value)
				require.True(t, ok)
				f := qfield(tt.fieldType, false, 0, 0)
				v, err := convert(f, rat)
				require.NoError(t, err)
				numeric := v.(types.QValueNumeric)
				assert.Equal(t, tt.value, numeric.Val.String())
				assert.Equal(t, tt.precision, numeric.Precision)
				assert.Equal(t, tt.scale, numeric.Scale)
			})
		}
	})

	t.Run("interval and range", func(t *testing.T) {
		interval := &bigquery.IntervalValue{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6}
		v, err := convert(qfield(bigquery.IntervalFieldType, false, 0, 0), interval)
		require.NoError(t, err)
		assert.Equal(t, types.QValueInterval{Val: "1-2 3 4:5:6"}, v)

		rangeValue := &bigquery.RangeValue{
			Start: civil.Date{Year: 2024, Month: 1, Day: 1},
			End:   civil.Date{Year: 2024, Month: 2, Day: 1},
		}
		v, err = convert(qfield(bigquery.RangeFieldType, false, 0, 0), rangeValue)
		require.NoError(t, err)
		assert.Equal(t, types.QValueString{Val: "[2024-01-01, 2024-02-01)"}, v)
	})

	t.Run("record is encoded as schema-aware JSON", func(t *testing.T) {
		field := &bigquery.FieldSchema{
			Name: "record_col", Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{Name: "name", Type: bigquery.StringFieldType},
				{Name: "created", Type: bigquery.DateFieldType},
				{Name: "metadata", Type: bigquery.JSONFieldType},
			},
		}
		f := BigQueryFieldToQField(field)
		v, err := qvalueFromBigQueryValue(f, []bigquery.Value{
			"alice", civil.Date{Year: 2024, Month: 1, Day: 2}, `{"active":true}`,
		}, field)
		require.NoError(t, err)
		assert.JSONEq(t, `{"name":"alice","created":"2024-01-02","metadata":{"active":true}}`, v.Value().(string))
	})

	t.Run("record JSON recursively preserves BigQuery logical types", func(t *testing.T) {
		field := &bigquery.FieldSchema{
			Name: "record_col", Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{Name: "payload", Type: bigquery.BytesFieldType},
				{Name: "at", Type: bigquery.DateTimeFieldType},
				{Name: "amount", Type: bigquery.BigNumericFieldType},
				{Name: "duration", Type: bigquery.IntervalFieldType},
				{Name: "window", Type: bigquery.RangeFieldType},
				{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
				{Name: "nested", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "enabled", Type: bigquery.BooleanFieldType},
				}},
			},
		}
		amount, ok := new(big.Rat).SetString("0.12345678901234567890123456789012345678")
		require.True(t, ok)
		f := BigQueryFieldToQField(field)
		v, err := qvalueFromBigQueryValue(f, []bigquery.Value{
			[]byte("hi"),
			civil.DateTime{
				Date: civil.Date{Year: 2024, Month: 1, Day: 2},
				Time: civil.Time{Hour: 3, Minute: 4, Second: 5},
			},
			amount,
			&bigquery.IntervalValue{Days: 2},
			&bigquery.RangeValue{Start: civil.Date{Year: 2024, Month: 1, Day: 1}},
			[]bigquery.Value{"a", "b"},
			[]bigquery.Value{true},
		}, field)
		require.NoError(t, err)
		assert.JSONEq(t, `{
			"payload":"aGk=",
			"at":"2024-01-02 03:04:05",
			"amount":0.12345678901234567890123456789012345678,
			"duration":"0-0 2 0:0:0",
			"window":"[2024-01-01, UNBOUNDED)",
			"tags":["a","b"],
			"nested":{"enabled":true}
		}`, v.Value().(string))
	})

	t.Run("repeated columns", func(t *testing.T) {
		t.Run("array string", func(t *testing.T) {
			f := qfield(bigquery.StringFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{"a", "b"})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayString{Val: []string{"a", "b"}}, v)
		})

		t.Run("array int64", func(t *testing.T) {
			f := qfield(bigquery.IntegerFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{int64(1), int64(2)})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayInt64{Val: []int64{1, 2}}, v)
		})

		t.Run("array float64", func(t *testing.T) {
			f := qfield(bigquery.FloatFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{1.5, 2.5})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayFloat64{Val: []float64{1.5, 2.5}}, v)
		})

		t.Run("array boolean", func(t *testing.T) {
			f := qfield(bigquery.BooleanFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{true, false})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayBoolean{Val: []bool{true, false}}, v)
		})

		t.Run("array timestamp", func(t *testing.T) {
			ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
			f := qfield(bigquery.TimestampFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{ts})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayTimestampTZ{Val: []time.Time{ts}}, v)
		})

		t.Run("array datetime", func(t *testing.T) {
			dt := civil.DateTime{
				Date: civil.Date{Year: 2024, Month: 1, Day: 2},
				Time: civil.Time{Hour: 3, Minute: 4, Second: 5},
			}
			f := qfield(bigquery.DateTimeFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{dt})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayTimestamp{Val: []time.Time{dt.In(time.UTC)}}, v)
		})

		t.Run("array date", func(t *testing.T) {
			date := civil.Date{Year: 2024, Month: 1, Day: 2}
			f := qfield(bigquery.DateFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{date})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayDate{Val: []time.Time{date.In(time.UTC)}}, v)
		})

		t.Run("array bytes are base64 strings", func(t *testing.T) {
			f := qfield(bigquery.BytesFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{[]byte("hi"), []byte{0xff}})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayString{Val: []string{"aGk=", "/w=="}}, v)
		})

		t.Run("array time", func(t *testing.T) {
			f := qfield(bigquery.TimeFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{
				civil.Time{Hour: 1, Minute: 2, Second: 3, Nanosecond: 4000},
			})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayTime{
				Val: []time.Duration{time.Hour + 2*time.Minute + 3*time.Second + 4000*time.Nanosecond},
			}, v)
		})

		t.Run("array geography uses WKT strings", func(t *testing.T) {
			f := qfield(bigquery.GeographyFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{"POINT(1 2)"})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayString{Val: []string{"POINT(1 2)"}}, v)
		})

		t.Run("array JSON preserves JSON numbers", func(t *testing.T) {
			f := qfield(bigquery.JSONFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{`{"n":18446744073709551615}`, `[1,true]`})
			require.NoError(t, err)
			jsonValue := v.(types.QValueJSON)
			assert.True(t, jsonValue.IsArray)
			assert.JSONEq(t, `[{"n":18446744073709551615},[1,true]]`, jsonValue.Val)
		})

		t.Run("array interval", func(t *testing.T) {
			f := qfield(bigquery.IntervalFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{
				&bigquery.IntervalValue{Days: 2},
			})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayInterval{Val: []string{"0-0 2 0:0:0"}}, v)
		})

		t.Run("array range", func(t *testing.T) {
			f := qfield(bigquery.RangeFieldType, true, 0, 0)
			v, err := convert(f, []bigquery.Value{
				&bigquery.RangeValue{Start: civil.Date{Year: 2024, Month: 1, Day: 1}},
			})
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayString{Val: []string{"[2024-01-01, UNBOUNDED)"}}, v)
		})

		t.Run("array record uses schema-aware JSON strings", func(t *testing.T) {
			field := &bigquery.FieldSchema{
				Name: "records", Type: bigquery.RecordFieldType, Repeated: true,
				Schema: bigquery.Schema{{Name: "name", Type: bigquery.StringFieldType}},
			}
			f := BigQueryFieldToQField(field)
			v, err := qvalueFromBigQueryValue(f, []bigquery.Value{
				[]bigquery.Value{"alice"}, []bigquery.Value{"bob"},
			}, field)
			require.NoError(t, err)
			assert.Equal(t, types.QValueArrayString{Val: []string{`{"name":"alice"}`, `{"name":"bob"}`}}, v)
		})

		t.Run("array numeric", func(t *testing.T) {
			f := qfield(bigquery.NumericFieldType, true, 10, 2)
			v, err := convert(f, []bigquery.Value{big.NewRat(1, 2)})
			require.NoError(t, err)
			arr, ok := v.(types.QValueArrayNumeric)
			require.True(t, ok)
			require.Len(t, arr.Val, 1)
			assert.True(t, decimal.NewFromFloat(0.5).Equal(arr.Val[0]))
		})

		t.Run("element type mismatch errors", func(t *testing.T) {
			f := qfield(bigquery.IntegerFieldType, true, 0, 0)
			_, err := convert(f, []bigquery.Value{"not-an-int"})
			require.Error(t, err)
		})

		t.Run("non-slice value for repeated column errors", func(t *testing.T) {
			f := qfield(bigquery.IntegerFieldType, true, 0, 0)
			_, err := convert(f, int64(1))
			require.Error(t, err)
		})
	})

	t.Run("unsupported Go value type errors instead of silently misconverting", func(t *testing.T) {
		f := qfield(bigquery.IntegerFieldType, false, 0, 0)
		// e.g. a RECORD column's nested []bigquery.Value landing on a non-array qfield.
		_, err := convert(f, []bigquery.Value{int64(1)})
		require.Error(t, err)
	})
}

func TestBigQueryNumericPrecisionAndScale(t *testing.T) {
	tests := []struct {
		name             string
		field            *bigquery.FieldSchema
		precision, scale int16
	}{
		{
			name:      "unparameterized numeric uses BigQuery default",
			field:     &bigquery.FieldSchema{Type: bigquery.NumericFieldType},
			precision: 38, scale: 9,
		},
		{
			name:      "unparameterized bignumeric uses BigQuery default",
			field:     &bigquery.FieldSchema{Type: bigquery.BigNumericFieldType},
			precision: 76, scale: 38,
		},
		{
			name:      "declared precision and scale win",
			field:     &bigquery.FieldSchema{Type: bigquery.NumericFieldType, Precision: 10, Scale: 2},
			precision: 10, scale: 2,
		},
		{
			name:      "declared precision with implicit zero scale is kept",
			field:     &bigquery.FieldSchema{Type: bigquery.BigNumericFieldType, Precision: 40},
			precision: 40, scale: 0,
		},
		{
			name:      "non-numeric field has no precision or scale",
			field:     &bigquery.FieldSchema{Type: bigquery.StringFieldType},
			precision: 0, scale: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			precision, scale := bigQueryNumericPrecisionAndScale(tt.field)
			assert.Equal(t, tt.precision, precision)
			assert.Equal(t, tt.scale, scale)
		})
	}
}
