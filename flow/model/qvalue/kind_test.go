package qvalue

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestGetNumericDestinationTypeWithClickHouseOverrides(t *testing.T) {
	tests := []struct {
		name               string
		precision          int16
		scale              int16
		chDefaultPrecision int32
		chDefaultScale     int32
		expectedPrecision  int16
		expectedScale      int16
		expectedIsString   bool
	}{
		{
			name:               "unbounded numeric with override",
			precision:          0,
			scale:              0,
			chDefaultPrecision: 60,
			chDefaultScale:     10,
			expectedPrecision:  60,
			expectedScale:      10,
			expectedIsString:   false,
		},
		{
			name:               "unbounded numeric without override",
			precision:          0,
			scale:              0,
			chDefaultPrecision: 0,
			chDefaultScale:     0,
			expectedPrecision:  76,
			expectedScale:      38,
			expectedIsString:   false,
		},
		{
			name:               "bounded numeric ignores override",
			precision:          20,
			scale:              5,
			chDefaultPrecision: 60,
			chDefaultScale:     10,
			expectedPrecision:  20,
			expectedScale:      5,
			expectedIsString:   false,
		},
		{
			name:               "unbounded numeric with high precision override",
			precision:          0,
			scale:              0,
			chDefaultPrecision: 76,
			chDefaultScale:     0,
			expectedPrecision:  76,
			expectedScale:      0,
			expectedIsString:   false,
		},
		{
			name:               "unbounded numeric with valid custom scale",
			precision:          0,
			scale:              0,
			chDefaultPrecision: 30,
			chDefaultScale:     10,
			expectedPrecision:  30,
			expectedScale:      10,
			expectedIsString:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetNumericDestinationType(
				tt.precision,
				tt.scale,
				protos.DBType_CLICKHOUSE,
				false, // unboundedNumericAsString
				tt.chDefaultPrecision,
				tt.chDefaultScale,
			)

			require.Equal(t, tt.expectedIsString, result.IsString)
			if !tt.expectedIsString {
				require.Equal(t, tt.expectedPrecision, result.Precision)
				require.Equal(t, tt.expectedScale, result.Scale)
			}
		})
	}
}

func TestDetermineNumericSettingForDWHClickHouse(t *testing.T) {
	tests := []struct {
		name               string
		precision          int16
		scale              int16
		chDefaultPrecision int32
		chDefaultScale     int32
		expectedPrecision  int16
		expectedScale      int16
	}{
		{
			name:               "unbounded with override - uses override",
			precision:          0,
			scale:              0,
			chDefaultPrecision: 50,
			chDefaultScale:     15,
			expectedPrecision:  50,
			expectedScale:      15,
		},
		{
			name:               "unbounded without override - uses defaults",
			precision:          0,
			scale:              0,
			chDefaultPrecision: 0,
			chDefaultScale:     0,
			expectedPrecision:  76,
			expectedScale:      38,
		},
		{
			name:               "bounded numeric ignores override",
			precision:          18,
			scale:              2,
			chDefaultPrecision: 60,
			chDefaultScale:     10,
			expectedPrecision:  18,
			expectedScale:      2,
		},
		{
			name:               "extreme override values",
			precision:          0,
			scale:              0,
			chDefaultPrecision: 76,
			chDefaultScale:     76,
			expectedPrecision:  76,
			expectedScale:      76,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, s := DetermineNumericSettingForDWH(
				tt.precision,
				tt.scale,
				protos.DBType_CLICKHOUSE,
				tt.chDefaultPrecision,
				tt.chDefaultScale,
			)

			require.Equal(t, tt.expectedPrecision, p, "precision mismatch")
			require.Equal(t, tt.expectedScale, s, "scale mismatch")
		})
	}
}

func TestDetermineNumericSettingForDWHNonClickHouse(t *testing.T) {
	// Non-ClickHouse destinations should ignore the override parameters
	tests := []struct {
		name              string
		dwh               protos.DBType
		expectedPrecision int16
		expectedScale     int16
	}{
		{
			name:              "Snowflake with override",
			dwh:               protos.DBType_SNOWFLAKE,
			expectedPrecision: 38,
			expectedScale:     20,
		},
		{
			name:              "BigQuery with override",
			dwh:               protos.DBType_BIGQUERY,
			expectedPrecision: 38,
			expectedScale:     20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pass override values but they should be ignored for non-ClickHouse
			p, s := DetermineNumericSettingForDWH(0, 0, tt.dwh, 60, 10)

			require.Equal(t, tt.expectedPrecision, p)
			require.Equal(t, tt.expectedScale, s)
		})
	}
}
