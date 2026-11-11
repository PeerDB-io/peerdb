package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

func TestFieldTypeFromDataType(t *testing.T) {
	tests := []struct {
		dataType     string
		wantType     bigquery.FieldType
		wantRepeated bool
	}{
		{dataType: "INT64", wantType: bigquery.IntegerFieldType},
		{dataType: "FLOAT64", wantType: bigquery.FloatFieldType},
		{dataType: "BOOL", wantType: bigquery.BooleanFieldType},
		{dataType: "STRING", wantType: bigquery.StringFieldType},
		{dataType: "STRING(10)", wantType: bigquery.StringFieldType},
		{dataType: "BYTES(16)", wantType: bigquery.BytesFieldType},
		{dataType: "NUMERIC(10, 2)", wantType: bigquery.NumericFieldType},
		{dataType: "BIGNUMERIC", wantType: bigquery.BigNumericFieldType},
		{dataType: "TIMESTAMP", wantType: bigquery.TimestampFieldType},
		{dataType: "DATETIME", wantType: bigquery.DateTimeFieldType},
		{dataType: "DATE", wantType: bigquery.DateFieldType},
		{dataType: "TIME", wantType: bigquery.TimeFieldType},
		{dataType: "GEOGRAPHY", wantType: bigquery.GeographyFieldType},
		{dataType: "JSON", wantType: bigquery.JSONFieldType},
		{dataType: "INTERVAL", wantType: bigquery.IntervalFieldType},
		{dataType: "RANGE<DATE>", wantType: bigquery.RangeFieldType},
		{dataType: "STRUCT<a BOOL, b NUMERIC(10, 2)>", wantType: bigquery.RecordFieldType},
		{dataType: "ARRAY<STRING>", wantType: bigquery.StringFieldType, wantRepeated: true},
		{dataType: "ARRAY<INT64>", wantType: bigquery.IntegerFieldType, wantRepeated: true},
		{dataType: "ARRAY<STRUCT<x INT64, y ARRAY<STRING>>>", wantType: bigquery.RecordFieldType, wantRepeated: true},
	}
	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			gotType, gotRepeated := fieldTypeFromDataType(tt.dataType)
			require.Equal(t, tt.wantType, gotType)
			require.Equal(t, tt.wantRepeated, gotRepeated)
		})
	}
}
