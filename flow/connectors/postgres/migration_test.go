package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type MigrationTestSuite struct {
	t             *testing.T
	sourceConn    *PostgresConnector
	targetConn    *PostgresConnector
	sourceSchema  string
	targetSchema  string
	sourceConnRaw *pgx.Conn
	targetConnRaw *pgx.Conn
}

func SetupMigrationSuite(t *testing.T) MigrationTestSuite {
	t.Helper()

	// Create source connector
	sourceConnector, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)

	// Create target connector (can be same DB, different schema for testing)
	targetConnector, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)

	// Get raw connections for direct queries
	sourceConnRaw := sourceConnector.conn
	targetConnRaw := targetConnector.conn

	// Create test schemas
	sourceSchema := "migrate_src_" + strings.ToLower(shared.RandomString(8))
	targetSchema := "migrate_dst_" + strings.ToLower(shared.RandomString(8))

	// Setup source schema
	setupTx, err := sourceConnRaw.Begin(t.Context())
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", sourceSchema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+sourceSchema)
	require.NoError(t, err)
	require.NoError(t, setupTx.Commit(t.Context()))

	// Setup target schema
	setupTx2, err := targetConnRaw.Begin(t.Context())
	require.NoError(t, err)
	_, err = setupTx2.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", targetSchema))
	require.NoError(t, err)
	_, err = setupTx2.Exec(t.Context(), "CREATE SCHEMA "+targetSchema)
	require.NoError(t, err)
	require.NoError(t, setupTx2.Commit(t.Context()))

	return MigrationTestSuite{
		t:             t,
		sourceConn:    sourceConnector,
		targetConn:    targetConnector,
		sourceSchema:  sourceSchema,
		targetSchema:  targetSchema,
		sourceConnRaw: sourceConnRaw,
		targetConnRaw: targetConnRaw,
	}
}

func (s MigrationTestSuite) Teardown(ctx context.Context) {
	// Cleanup schemas
	teardownTx, err := s.sourceConnRaw.Begin(ctx)
	require.NoError(s.t, err)
	_, err = teardownTx.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", s.sourceSchema))
	require.NoError(s.t, err)
	_, err = teardownTx.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", s.targetSchema))
	require.NoError(s.t, err)
	require.NoError(s.t, teardownTx.Commit(ctx))

	require.NoError(s.t, s.sourceConn.Close())
	require.NoError(s.t, s.targetConn.Close())
}

func TestSchemaMigration(t *testing.T) {
	e2eshared.RunSuite(t, func(t *testing.T) MigrationTestSuite {
		suite := SetupMigrationSuite(t)
		defer suite.Teardown(t.Context())

		// Create source table with schema
		tableName := "test_table"
		sourceTable := fmt.Sprintf("%s.%s", suite.sourceSchema, tableName)
		targetTable := fmt.Sprintf("%s.%s", suite.targetSchema, tableName)

		// Create source table
		_, err := suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				email VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				age INT
			)`, sourceTable))
		require.NoError(t, err)

		// Insert some test data
		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s (id, name, email, age) VALUES 
			(1, 'Alice', 'alice@example.com', 30),
			(2, 'Bob', 'bob@example.com', 25)`, sourceTable))
		require.NoError(t, err)

		// Migrate schema
		tableMappings := []*protos.TableMapping{
			{
				SourceTableIdentifier:      sourceTable,
				DestinationTableIdentifier: targetTable,
			},
		}

		err = MigrateSchemaFromSource(t.Context(), suite.sourceConn, suite.targetConn, tableMappings)
		require.NoError(t, err)

		// Verify target table exists and has correct schema
		var columnCount int
		err = suite.targetConnRaw.QueryRow(t.Context(), fmt.Sprintf(`
			SELECT COUNT(*) FROM information_schema.columns 
			WHERE table_schema = '%s' AND table_name = '%s'`,
			suite.targetSchema, tableName)).Scan(&columnCount)
		require.NoError(t, err)
		require.Equal(t, 5, columnCount, "Target table should have 5 columns")

		// Verify primary key
		var pkCount int
		err = suite.targetConnRaw.QueryRow(t.Context(), fmt.Sprintf(`
			SELECT COUNT(*) FROM information_schema.table_constraints 
			WHERE table_schema = '%s' AND table_name = '%s' AND constraint_type = 'PRIMARY KEY'`,
			suite.targetSchema, tableName)).Scan(&pkCount)
		require.NoError(t, err)
		require.Equal(t, 1, pkCount, "Target table should have a primary key")

		return suite
	})
}

func TestTriggerMigration(t *testing.T) {
	e2eshared.RunSuite(t, func(t *testing.T) MigrationTestSuite {
		suite := SetupMigrationSuite(t)
		defer suite.Teardown(t.Context())

		tableName := "test_table"
		sourceTable := fmt.Sprintf("%s.%s", suite.sourceSchema, tableName)
		targetTable := fmt.Sprintf("%s.%s", suite.targetSchema, tableName)

		// Create source table
		_, err := suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				updated_at TIMESTAMP
			)`, sourceTable))
		require.NoError(t, err)

		// Create trigger function
		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION %s.update_updated_at()
			RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = CURRENT_TIMESTAMP;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql`, suite.sourceSchema))
		require.NoError(t, err)

		// Create trigger on source table
		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE TRIGGER update_timestamp
			BEFORE UPDATE ON %s
			FOR EACH ROW
			EXECUTE FUNCTION %s.update_updated_at()`, sourceTable, suite.sourceSchema))
		require.NoError(t, err)

		// First migrate schema
		tableMappings := []*protos.TableMapping{
			{
				SourceTableIdentifier:      sourceTable,
				DestinationTableIdentifier: targetTable,
			},
		}

		err = MigrateSchemaFromSource(t.Context(), suite.sourceConn, suite.targetConn, tableMappings)
		require.NoError(t, err)

		// Create trigger function in target schema
		_, err = suite.targetConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION %s.update_updated_at()
			RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = CURRENT_TIMESTAMP;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql`, suite.targetSchema))
		require.NoError(t, err)

		// Migrate triggers
		err = MigrateTriggersFromSource(t.Context(), suite.sourceConn, suite.targetConn, tableMappings)
		require.NoError(t, err)

		// Verify trigger exists on target table
		var triggerCount int
		err = suite.targetConnRaw.QueryRow(t.Context(), fmt.Sprintf(`
			SELECT COUNT(*) FROM information_schema.triggers 
			WHERE trigger_schema = '%s' AND event_object_table = '%s'`,
			suite.targetSchema, tableName)).Scan(&triggerCount)
		require.NoError(t, err)
		require.GreaterOrEqual(t, triggerCount, 1, "Target table should have at least one trigger")

		return suite
	})
}

func TestIndexMigration(t *testing.T) {
	e2eshared.RunSuite(t, func(t *testing.T) MigrationTestSuite {
		suite := SetupMigrationSuite(t)
		defer suite.Teardown(t.Context())

		tableName := "test_table"
		sourceTable := fmt.Sprintf("%s.%s", suite.sourceSchema, tableName)
		targetTable := fmt.Sprintf("%s.%s", suite.targetSchema, tableName)

		// Create source table
		_, err := suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				email VARCHAR(255),
				created_at TIMESTAMP
			)`, sourceTable))
		require.NoError(t, err)

		// Create indexes on source table
		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE INDEX idx_name ON %s (name)`, sourceTable))
		require.NoError(t, err)

		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE UNIQUE INDEX idx_email ON %s (email)`, sourceTable))
		require.NoError(t, err)

		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE INDEX idx_created_at ON %s (created_at DESC)`, sourceTable))
		require.NoError(t, err)

		// First migrate schema
		tableMappings := []*protos.TableMapping{
			{
				SourceTableIdentifier:      sourceTable,
				DestinationTableIdentifier: targetTable,
			},
		}

		err = MigrateSchemaFromSource(t.Context(), suite.sourceConn, suite.targetConn, tableMappings)
		require.NoError(t, err)

		// Migrate indexes
		err = MigrateIndexesFromSource(t.Context(), suite.sourceConn, suite.targetConn, tableMappings)
		require.NoError(t, err)

		// Verify indexes exist on target table
		var indexCount int
		err = suite.targetConnRaw.QueryRow(t.Context(), fmt.Sprintf(`
			SELECT COUNT(*) FROM pg_indexes 
			WHERE schemaname = '%s' AND tablename = '%s' AND indexname NOT LIKE '%%_pkey'`,
			suite.targetSchema, tableName)).Scan(&indexCount)
		require.NoError(t, err)
		require.GreaterOrEqual(t, indexCount, 2, "Target table should have at least 2 indexes (excluding primary key)")

		// Verify specific indexes
		var idxNameExists bool
		err = suite.targetConnRaw.QueryRow(t.Context(), fmt.Sprintf(`
			SELECT EXISTS (
				SELECT 1 FROM pg_indexes 
				WHERE schemaname = '%s' AND tablename = '%s' AND indexname = 'idx_name'
			)`, suite.targetSchema, tableName)).Scan(&idxNameExists)
		require.NoError(t, err)
		require.True(t, idxNameExists, "idx_name index should exist")

		return suite
	})
}

func TestFullMigration(t *testing.T) {
	e2eshared.RunSuite(t, func(t *testing.T) MigrationTestSuite {
		suite := SetupMigrationSuite(t)
		defer suite.Teardown(t.Context())

		tableName := "full_test_table"
		sourceTable := fmt.Sprintf("%s.%s", suite.sourceSchema, tableName)
		targetTable := fmt.Sprintf("%s.%s", suite.targetSchema, tableName)

		// Create comprehensive source table
		_, err := suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				email VARCHAR(255) UNIQUE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP
			)`, sourceTable))
		require.NoError(t, err)

		// Create trigger function
		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION %s.update_updated_at()
			RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = CURRENT_TIMESTAMP;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql`, suite.sourceSchema))
		require.NoError(t, err)

		// Create trigger
		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE TRIGGER update_timestamp
			BEFORE UPDATE ON %s
			FOR EACH ROW
			EXECUTE FUNCTION %s.update_updated_at()`, sourceTable, suite.sourceSchema))
		require.NoError(t, err)

		// Create indexes
		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE INDEX idx_name ON %s (name)`, sourceTable))
		require.NoError(t, err)

		_, err = suite.sourceConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE INDEX idx_created_at ON %s (created_at DESC)`, sourceTable))
		require.NoError(t, err)

		tableMappings := []*protos.TableMapping{
			{
				SourceTableIdentifier:      sourceTable,
				DestinationTableIdentifier: targetTable,
			},
		}

		// Migrate schema
		err = MigrateSchemaFromSource(t.Context(), suite.sourceConn, suite.targetConn, tableMappings)
		require.NoError(t, err)

		// Create trigger function in target
		_, err = suite.targetConnRaw.Exec(t.Context(), fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION %s.update_updated_at()
			RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = CURRENT_TIMESTAMP;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql`, suite.targetSchema))
		require.NoError(t, err)

		// Migrate triggers
		err = MigrateTriggersFromSource(t.Context(), suite.sourceConn, suite.targetConn, tableMappings)
		require.NoError(t, err)

		// Migrate indexes
		err = MigrateIndexesFromSource(t.Context(), suite.sourceConn, suite.targetConn, tableMappings)
		require.NoError(t, err)

		// Verify everything
		// Check table exists
		var tableExists bool
		err = suite.targetConnRaw.QueryRow(t.Context(), fmt.Sprintf(`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables 
				WHERE table_schema = '%s' AND table_name = '%s'
			)`, suite.targetSchema, tableName)).Scan(&tableExists)
		require.NoError(t, err)
		require.True(t, tableExists, "Target table should exist")

		// Check trigger exists
		var triggerExists bool
		err = suite.targetConnRaw.QueryRow(t.Context(), fmt.Sprintf(`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.triggers 
				WHERE trigger_schema = '%s' AND event_object_table = '%s'
			)`, suite.targetSchema, tableName)).Scan(&triggerExists)
		require.NoError(t, err)
		require.True(t, triggerExists, "Target table should have triggers")

		// Check indexes exist
		var indexCount int
		err = suite.targetConnRaw.QueryRow(t.Context(), fmt.Sprintf(`
			SELECT COUNT(*) FROM pg_indexes 
			WHERE schemaname = '%s' AND tablename = '%s' AND indexname NOT LIKE '%%_pkey'`,
			suite.targetSchema, tableName)).Scan(&indexCount)
		require.NoError(t, err)
		require.GreaterOrEqual(t, indexCount, 2, "Target table should have at least 2 indexes")

		return suite
	})
}
