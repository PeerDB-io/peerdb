package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSchemaMappingNoTables(t *testing.T) {
	t.Parallel()
	// Potential callers can't call this with an empty list today, but this is an
	// exported function in a shared module, so let's handle it gracefully.
	got, err := GetSchemaMapping(nil, "shop", nil)
	require.NoError(t, err)
	require.Empty(t, got.Columns)
	require.Empty(t, got.PrimaryKeys)
	require.NotNil(t, got.Columns, "callers index into these maps")
	require.NotNil(t, got.PrimaryKeys)
}
