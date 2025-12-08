package connbigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
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
