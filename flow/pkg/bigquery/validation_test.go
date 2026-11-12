package bigquery

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
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

func TestIsNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "googleapi 404", err: &googleapi.Error{Code: http.StatusNotFound}, want: true},
		{name: "googleapi 403", err: &googleapi.Error{Code: http.StatusForbidden}, want: false},
		{name: "bigquery notFound", err: &bigquery.Error{Reason: "notFound"}, want: true},
		{name: "bigquery accessDenied", err: &bigquery.Error{Reason: "accessDenied"}, want: false},
		{name: "wrapped bigquery notFound", err: fmt.Errorf("read: %w", &bigquery.Error{Reason: "notFound"}), want: true},
		{name: "other error", err: errors.New("boom"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isNotFoundError(tt.err))
		})
	}
}

func TestIsAccessDeniedError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "googleapi accessDenied",
			err:  &googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "accessDenied"}}},
			want: true,
		},
		{
			name: "googleapi rateLimitExceeded",
			err:  &googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}}},
			want: false,
		},
		{name: "googleapi 403 without reason", err: &googleapi.Error{Code: http.StatusForbidden}, want: false},
		{name: "bigquery accessDenied", err: &bigquery.Error{Reason: "accessDenied"}, want: true},
		{name: "wrapped googleapi accessDenied", err: fmt.Errorf("read: %w",
			&googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "accessDenied"}}}), want: true},
		{name: "other error", err: errors.New("boom"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isAccessDeniedError(tt.err))
		})
	}
}

func TestNumericPrecisionAndScale(t *testing.T) {
	tests := []struct {
		dataType      string
		wantPrecision int64
		wantScale     int64
		wantErr       bool
	}{
		{dataType: "INT64"},
		{dataType: "NUMERIC"},
		{dataType: "BIGNUMERIC"},
		{dataType: "STRING(10)"},
		{dataType: "NUMERIC(10, 2)", wantPrecision: 10, wantScale: 2},
		{dataType: "NUMERIC(10)", wantPrecision: 10},
		{dataType: "BIGNUMERIC(40, 10)", wantPrecision: 40, wantScale: 10},
		{dataType: "BIGNUMERIC(40)", wantPrecision: 40},
		{dataType: "ARRAY<NUMERIC(12, 3)>", wantPrecision: 12, wantScale: 3},
		{dataType: "ARRAY<NUMERIC>"},
		{dataType: "STRUCT<a NUMERIC(10, 2)>"},
		{dataType: "ARRAY<STRUCT<a BIGNUMERIC(40)>>"},
		{dataType: "NUMERIC(10, 2", wantErr: true},
		{dataType: "NUMERIC(x)", wantErr: true},
		{dataType: "NUMERIC(10, y)", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			precision, scale, err := numericPrecisionAndScale(tt.dataType)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantPrecision, precision)
			require.Equal(t, tt.wantScale, scale)
		})
	}
}
