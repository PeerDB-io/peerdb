package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PostgresIndexTestSuite struct {
	t         *testing.T
	connector *PostgresConnector
	schema    string
}

func SetupIndexSuite(t *testing.T) PostgresIndexTestSuite {
	t.Helper()

	connector, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)

	setupTx, err := connector.conn.Begin(t.Context())
	require.NoError(t, err)
	defer func() {
		err := setupTx.Rollback(t.Context())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()
	schema := "pgindex_" + strings.ToLower(shared.RandomString(8))
	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	require.NoError(t, setupTx.Commit(t.Context()))

	return PostgresIndexTestSuite{
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (s PostgresIndexTestSuite) Teardown(ctx context.Context) {
	teardownTx, err := s.connector.conn.Begin(ctx)
	require.NoError(s.t, err)
	defer func() {
		err := teardownTx.Rollback(ctx)
		if err != pgx.ErrTxClosed {
			require.NoError(s.t, err)
		}
	}()
	_, err = teardownTx.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", s.schema))
	require.NoError(s.t, err)
	require.NoError(s.t, teardownTx.Commit(ctx))

	require.NoError(s.t, s.connector.ConnectionActive(ctx))
	require.NoError(s.t, s.connector.Close())
	require.Error(s.t, s.connector.ConnectionActive(ctx))
}

func (s PostgresIndexTestSuite) TestGetAllIndexes() {
	tableName := "test_indexes"
	fullyQualifiedTable := fmt.Sprintf("%s.%s", s.schema, tableName)

	// Create table with various types of indexes
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email TEXT,
			name TEXT,
			age INT
		);
		CREATE INDEX idx_email ON %s(email);
		CREATE UNIQUE INDEX idx_name_unique ON %s(name);
		CREATE INDEX idx_age_desc ON %s(age DESC);
		CREATE INDEX idx_multi_column ON %s(name, age);
	`, fullyQualifiedTable, fullyQualifiedTable, fullyQualifiedTable, fullyQualifiedTable, fullyQualifiedTable))
	require.NoError(s.t, err)

	// Get all indexes
	indexes, err := s.connector.GetIndexes(s.t.Context(), &utils.SchemaTable{
		Schema: s.schema,
		Table:  tableName,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, indexes)

	// Should have: primary key + 4 explicitly created indexes = 5 total
	require.Len(s.t, indexes, 5)

	// Helper function to find index by name
	findIndex := func(name string) *IndexMetadata {
		for _, idx := range indexes {
			if strings.Contains(idx.IndexName, name) {
				return idx
			}
		}
		return nil
	}

	// Verify primary key index
	pkIndex := findIndex("pkey")
	require.NotNil(s.t, pkIndex, "primary key index should exist")
	require.True(s.t, pkIndex.IsPrimary)
	require.True(s.t, pkIndex.IsUnique)
	require.Equal(s.t, []string{"id"}, pkIndex.Columns)

	// Verify regular index
	emailIdx := findIndex("idx_email")
	require.NotNil(s.t, emailIdx, "email index should exist")
	require.False(s.t, emailIdx.IsUnique)
	require.False(s.t, emailIdx.IsPrimary)
	require.Equal(s.t, []string{"email"}, emailIdx.Columns)
	require.Contains(s.t, emailIdx.IndexDef, "CREATE INDEX")

	// Verify unique index
	nameIdx := findIndex("idx_name_unique")
	require.NotNil(s.t, nameIdx, "unique name index should exist")
	require.True(s.t, nameIdx.IsUnique)
	require.False(s.t, nameIdx.IsPrimary)
	require.Equal(s.t, []string{"name"}, nameIdx.Columns)
	require.Contains(s.t, nameIdx.IndexDef, "UNIQUE")

	// Verify multi-column index
	multiIdx := findIndex("idx_multi_column")
	require.NotNil(s.t, multiIdx, "multi-column index should exist")
	require.False(s.t, multiIdx.IsUnique)
	require.False(s.t, multiIdx.IsPrimary)
	require.Equal(s.t, []string{"name", "age"}, multiIdx.Columns)
}

func (s PostgresIndexTestSuite) TestGetAllIndexesEmptyTable() {
	tableName := "test_no_indexes"
	fullyQualifiedTable := fmt.Sprintf("%s.%s", s.schema, tableName)

	// Create table with only primary key (no explicit indexes)
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			data TEXT
		);
	`, fullyQualifiedTable))
	require.NoError(s.t, err)

	// Get all indexes
	indexes, err := s.connector.GetIndexes(s.t.Context(), &utils.SchemaTable{
		Schema: s.schema,
		Table:  tableName,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, indexes)

	// Should only have the primary key index
	require.Len(s.t, indexes, 1)
	require.True(s.t, indexes[0].IsPrimary)
	require.True(s.t, indexes[0].IsUnique)
}

func (s PostgresIndexTestSuite) TestGetAllIndexesNonexistentTable() {
	// Try to get indexes for a table that doesn't exist
	indexes, err := s.connector.GetIndexes(s.t.Context(), &utils.SchemaTable{
		Schema: s.schema,
		Table:  "nonexistent_table",
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, indexes)
	require.Len(s.t, indexes, 0) // Should return empty slice, not error
}

func TestPostgresIndexTestSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupIndexSuite)
}
