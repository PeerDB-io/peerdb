package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestFoldSchemaMappingRows(t *testing.T) {
	t.Parallel()
	// Both sets are {table, column}. columnRows arrive ordered by ORDINAL_POSITION, pkRows
	// by SEQ_IN_INDEX, so orders' key is (region, id) though its columns are declared
	// (id, region, total).
	columnRows := [][2]string{
		{"orders", "id"},
		{"orders", "region"},
		{"orders", "total"},
		{"events", "id"},
		{"events", "payload"},
	}
	pkRows := [][2]string{
		{"orders", "region"},
		{"orders", "id"},
	}

	got := foldSchemaMappingRows("shop", columnRows, pkRows)

	orders := common.QualifiedTable{Namespace: "shop", Table: "orders"}
	events := common.QualifiedTable{Namespace: "shop", Table: "events"}

	require.Equal(t, []string{"id", "region", "total"}, got.Columns[orders])
	require.Equal(t, []string{"region", "id"}, got.PrimaryKeys[orders], "index order, not column order")
	require.Equal(t, []string{"id", "payload"}, got.Columns[events])
	// No primary key means no PrimaryKeys entry at all: callers check len(...) > 0.
	require.NotContains(t, got.PrimaryKeys, events)
}

func TestGetSchemaMappingNoTables(t *testing.T) {
	t.Parallel()
	// Potential callers can't call this with an empty list today, but this is an
	// exported function in a shared module, so like the original, let's handle it gracefully.
	got, err := GetSchemaMapping(nil, "shop", nil)
	require.NoError(t, err)
	require.Empty(t, got.Columns)
	require.Empty(t, got.PrimaryKeys)
	require.NotNil(t, got.Columns, "callers index into these maps")
	require.NotNil(t, got.PrimaryKeys)
}
