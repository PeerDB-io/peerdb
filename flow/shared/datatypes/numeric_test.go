package datatypes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetNumericTypeForWarehousePrecisionScale(t *testing.T) {
	tests := []struct {
		warehouse                        WarehouseNumericCompatibility
		precision, scale                 int16
		expectedPrecision, expectedScale int16
	}{
		// unconstrained NUMERIC takes the warehouse default
		{ClickHouseNumericCompatibility{}, 0, 0, 76, PeerDBClickHouseScale},
		{BigQueryNumericCompatibility{}, 0, 0, 38, PeerDBBigQueryScale},
		// within the warehouse limits, kept as is
		{ClickHouseNumericCompatibility{}, 20, 6, 20, 6},
		{SnowflakeNumericCompatibility{}, 10, 10, 10, 10},
		// scale above precision: NUMERIC(3, 5) holds -0.00999 to 0.00999, which DECIMAL(5, 5) holds exactly
		{ClickHouseNumericCompatibility{}, 3, 5, 5, 5},
		{ClickHouseNumericCompatibility{}, 2, 4, 4, 4},
		{SnowflakeNumericCompatibility{}, 3, 5, 5, 5},
		{BigQueryNumericCompatibility{}, 3, 5, 5, 5},
		{DefaultNumericCompatibility{}, 3, 5, 5, 5},
		// widened past the maximum precision, then past the maximum scale: the warehouse default
		{ClickHouseNumericCompatibility{}, 3, 80, 76, PeerDBClickHouseScale},
		{BigQueryNumericCompatibility{}, 1, 30, 38, PeerDBBigQueryScale},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%T/%d_%d", tc.warehouse, tc.precision, tc.scale), func(t *testing.T) {
			precision, scale := GetNumericTypeForWarehousePrecisionScale(tc.precision, tc.scale, tc.warehouse)
			require.Equal(t, tc.expectedPrecision, precision, "precision")
			require.Equal(t, tc.expectedScale, scale, "scale")
			require.LessOrEqual(t, scale, precision)
		})
	}
}
