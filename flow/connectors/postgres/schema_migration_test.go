package connpostgres

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PostgresSchemaMigrationTestSuite struct {
	t         *testing.T
	connector *PostgresConnector
	schema    string
}

// getTestPostgresConfig creates a PostgresConfig from environment variables for testing.
// This avoids requiring PeerDB's catalog schema - any PostgreSQL database will work.
func getTestPostgresConfig(t *testing.T) *protos.PostgresConfig {
	t.Helper()

	host := os.Getenv("PEERDB_CATALOG_HOST")
	if host == "" {
		host = "localhost"
	}

	portStr := os.Getenv("PEERDB_CATALOG_PORT")
	port := uint32(5432)
	if portStr != "" {
		if p, err := strconv.ParseUint(portStr, 10, 32); err == nil {
			port = uint32(p)
		}
	}

	user := os.Getenv("PEERDB_CATALOG_USER")
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("PEERDB_CATALOG_PASSWORD")
	if password == "" {
		password = "postgres"
	}

	database := os.Getenv("PEERDB_CATALOG_DATABASE")
	if database == "" {
		database = "postgres"
	}

	requireTls := false
	if tlsStr := os.Getenv("PEERDB_CATALOG_REQUIRE_TLS"); tlsStr != "" {
		if tls, err := strconv.ParseBool(tlsStr); err == nil {
			requireTls = tls
		}
	}

	return &protos.PostgresConfig{
		Host:       host,
		Port:       port,
		User:       user,
		Password:   password,
		Database:   database,
		RequireTls: requireTls,
	}
}

func SetupSchemaMigrationSuite(t *testing.T) PostgresSchemaMigrationTestSuite {
	t.Helper()

	// Use a direct PostgresConfig instead of catalog config to avoid requiring catalog schema
	pgConfig := getTestPostgresConfig(t)
	connector, err := NewPostgresConnector(t.Context(), nil, pgConfig)
	require.NoError(t, err)

	setupTx, err := connector.conn.Begin(t.Context())
	require.NoError(t, err)
	defer func() {
		err := setupTx.Rollback(t.Context())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()
	schema := "pgmigrate_" + strings.ToLower(shared.RandomString(8))
	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	require.NoError(t, setupTx.Commit(t.Context()))

	return PostgresSchemaMigrationTestSuite{
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (s PostgresSchemaMigrationTestSuite) TestTriggerMigration() {
	tableName := s.schema + ".test_trigger_migration"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE %s(
			id INT PRIMARY KEY,
			name VARCHAR(100),
			updated_at TIMESTAMP
		)`, tableName))
	require.NoError(s.t, err)

	// Create a function for the trigger
	_, err = s.connector.conn.Exec(s.t.Context(),
		`CREATE OR REPLACE FUNCTION update_updated_at()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`)
	require.NoError(s.t, err)

	// Create trigger on source
	_, err = s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TRIGGER trg_update_timestamp
			BEFORE UPDATE ON %s
			FOR EACH ROW
			EXECUTE FUNCTION update_updated_at()`, tableName))
	require.NoError(s.t, err)

	// Extract triggers
	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	triggers, err := s.connector.GetTableTriggers(s.t.Context(), schemaTable.Schema, schemaTable.Table)
	require.NoError(s.t, err)
	require.Len(s.t, triggers, 1, "Should have 1 trigger")

	// Create destination table without trigger
	destTableName := s.schema + ".test_trigger_migration_dest"
	_, err = s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE %s(
			id INT PRIMARY KEY,
			name VARCHAR(100),
			updated_at TIMESTAMP
		)`, destTableName))
	require.NoError(s.t, err)

	// Apply trigger via schema delta
	schemaDelta := &protos.TableSchemaDelta{
		SrcTableName: tableName,
		DstTableName: destTableName,
		Triggers:     triggers,
		System:       protos.TypeSystem_Q,
	}

	err = s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "test_flow", nil, []*protos.TableSchemaDelta{schemaDelta})
	require.NoError(s.t, err)

	// Verify trigger was created
	destSchemaTable, err := utils.ParseSchemaTable(destTableName)
	require.NoError(s.t, err)

	var triggerCount int
	err = s.connector.conn.QueryRow(s.t.Context(),
		`SELECT COUNT(*) 
		 FROM pg_trigger t
		 JOIN pg_class c ON t.tgrelid = c.oid
		 JOIN pg_namespace n ON c.relnamespace = n.oid
		 WHERE n.nspname = $1 AND c.relname = $2 AND NOT t.tgisinternal`,
		destSchemaTable.Schema, destSchemaTable.Table).Scan(&triggerCount)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, triggerCount, "Destination should have 1 trigger")
}

func (s PostgresSchemaMigrationTestSuite) Teardown(ctx context.Context) {
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

func (s PostgresSchemaMigrationTestSuite) TestUniqueConstraintMigration() {
	// Simplified test: Just verify that ApplyConstraints can create a UNIQUE constraint
	tableName := s.schema + ".test_unique_simple"

	// Drop table if it exists
	_, _ = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))

	// Create table without UNIQUE constraint
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE %s(id INT PRIMARY KEY, email VARCHAR(100))`, tableName))
	require.NoError(s.t, err)

	// Manually create a constraint definition to apply
	constraint := &protos.ConstraintDefinition{
		Name:       "email_unique",
		TableName:  "test_unique_simple",
		Type:       "UNIQUE",
		Definition: "UNIQUE (email)",
		Columns:    []string{"email"},
	}

	// Apply the constraint using ReplayTableSchemaDeltas
	schemaDelta := &protos.TableSchemaDelta{
		SrcTableName: tableName,
		DstTableName: tableName,
		Constraints:  []*protos.ConstraintDefinition{constraint},
		System:       protos.TypeSystem_Q,
	}

	err = s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "test_flow", nil, []*protos.TableSchemaDelta{schemaDelta})
	require.NoError(s.t, err)

	// Verify constraint was created
	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)

	var constraintExists bool
	err = s.connector.conn.QueryRow(s.t.Context(),
		`SELECT EXISTS(
			SELECT 1 FROM pg_constraint con
			JOIN pg_class t ON con.conrelid = t.oid
			JOIN pg_namespace n ON t.relnamespace = n.oid
			WHERE n.nspname = $1 AND t.relname = $2 AND con.conname = $3 AND con.contype = 'u'
		)`, schemaTable.Schema, schemaTable.Table, "email_unique").Scan(&constraintExists)
	require.NoError(s.t, err)
	require.True(s.t, constraintExists, "UNIQUE constraint should be created")
}

func (s PostgresSchemaMigrationTestSuite) TestNotNullConstraintMigration() {
	tableName := s.schema + ".test_notnull_constraint"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE %s(
			id INT PRIMARY KEY,
			email VARCHAR(100) CONSTRAINT email_notnull NOT NULL,
			name VARCHAR(100) NOT NULL
		)`, tableName))
	require.NoError(s.t, err)

	// Extract constraints
	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)

	constraints, err := s.connector.GetTableConstraints(s.t.Context(), schemaTable.Schema, schemaTable.Table)
	require.NoError(s.t, err)

	// Count NOT NULL constraints (excluding primary key)
	notNullCount := 0
	for _, c := range constraints {
		if c.Type == "NOT NULL" {
			notNullCount++
			s.t.Logf("Found NOT NULL constraint: %s", c.Name)
		}
	}
	require.GreaterOrEqual(s.t, notNullCount, 1, "Should have at least 1 NOT NULL constraint")

	// Create destination table without NOT NULL
	destTableName := s.schema + ".test_notnull_constraint_dest"
	_, err = s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE %s(
			id INT PRIMARY KEY,
			email VARCHAR(100),
			name VARCHAR(100)
		)`, destTableName))
	require.NoError(s.t, err)

	// Apply constraints via schema delta
	schemaDelta := &protos.TableSchemaDelta{
		SrcTableName: tableName,
		DstTableName: destTableName,
		Constraints:  constraints,
		System:       protos.TypeSystem_Q,
	}

	err = s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "test_flow", nil, []*protos.TableSchemaDelta{schemaDelta})
	require.NoError(s.t, err)

	// Verify NOT NULL constraints were created in destination
	destSchemaTable, err := utils.ParseSchemaTable(destTableName)
	require.NoError(s.t, err)

	destConstraints, err := s.connector.GetTableConstraints(s.t.Context(), destSchemaTable.Schema, destSchemaTable.Table)
	require.NoError(s.t, err)

	destNotNullCount := 0
	for _, c := range destConstraints {
		if c.Type == "NOT NULL" {
			destNotNullCount++
		}
	}
	require.Equal(s.t, notNullCount, destNotNullCount, "Destination should have same number of NOT NULL constraints")
}

func TestPostgresSchemaMigrationTestSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupSchemaMigrationSuite)
}
