package connbigquery

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParquetTotalRows(t *testing.T) {
	f, err := os.Open("../../e2e/test_data/1k.parquet")
	require.NoError(t, err)
	defer f.Close()

	totalRows, err := readParquetTotalRows(f)
	require.NoError(t, err)
	require.Equal(t, int64(1000), totalRows)
}
