package connbigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"

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
			expected: types.QValueKindTimestamp,
		},
		{
			name:     "datetime",
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
			expected: types.QValueKindString,
		},
		// repeated (array) types
		{
			name:     "repeated string",
			field:    &bigquery.FieldSchema{Type: bigquery.StringFieldType, Repeated: true},
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
			expected: types.QValueKindArrayTimestamp,
		},
		{
			name:     "repeated date",
			field:    &bigquery.FieldSchema{Type: bigquery.DateFieldType, Repeated: true},
			expected: types.QValueKindArrayDate,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BigQueryTypeToQValueKind(tt.field)
			assert.Equal(t, tt.expected, result)
		})
	}
}
