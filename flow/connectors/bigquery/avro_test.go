package connbigquery

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvroTotalRows(t *testing.T) {
	f, err := os.Open("../../e2e/test_data/1k.avro")
	require.NoError(t, err)
	defer f.Close()

	totalRows, err := avroTotalRows(t.Context(), f)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), totalRows)
}
