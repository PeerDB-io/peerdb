package connpostgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PostgresSchemaObjectsTestSuite struct {
	t         *testing.T
	connector *PostgresConnector
	schema    string
}

func SetupSchemaObjectsSuite(t *testing.T) PostgresSchemaObjectsTestSuite {
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
	schema := "pgobjects_" + strings.ToLower(shared.RandomString(8))
	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	require.NoError(t, setupTx.Commit(t.Context()))

	return PostgresSchemaObjectsTestSuite{
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (s PostgresSchemaObjectsTestSuite) TestAddIndex() {
	tableName := s.schema + ".test_add_index"
	
	// Create source table with an index
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255),
			name VARCHAR(255)
		)`, tableName))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_email ON %s(email)", tableName))
	require.NoError(s.t, err)

	// Get the index definition
	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	
	indexes, err := s.connector.GetIndexesForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	require.NotEmpty(s.t, indexes, "should have at least one index")

	// Verify index was created
	var indexName string
	err = s.connector.conn.QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT indexname FROM pg_indexes 
			WHERE schemaname = '%s' AND tablename = 'test_add_index' 
			AND indexname = 'idx_email'`, s.schema)).Scan(&indexName)
	require.NoError(s.t, err)
	require.Equal(s.t, "idx_email", indexName)

	// Test ReplayTableSchemaDeltas with index addition
	dstTableName := s.schema + ".test_add_index_dst"
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255),
			name VARCHAR(255)
		)`, dstTableName))
	require.NoError(s.t, err)

	// Drop the source index to avoid name conflicts (in real scenarios, source and dest are in different schemas/databases)
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf("DROP INDEX %s.idx_email", s.schema))
	require.NoError(s.t, err)

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_objects_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: dstTableName,
		AddedIndexes: indexes,
	}}))

	// Verify index was created on destination
	err = s.connector.conn.QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT indexname FROM pg_indexes 
			WHERE schemaname = '%s' AND tablename = 'test_add_index_dst' 
			AND indexname = 'idx_email'`, s.schema)).Scan(&indexName)
	require.NoError(s.t, err)
}

func (s PostgresSchemaObjectsTestSuite) TestAddUniqueIndex() {
	tableName := s.schema + ".test_add_unique_index"
	
	// Create table with unique index
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255)
		)`, tableName))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE UNIQUE INDEX idx_unique_email ON %s(email)", tableName))
	require.NoError(s.t, err)

	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	
	indexes, err := s.connector.GetIndexesForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	require.NotEmpty(s.t, indexes)
	
	// Verify the index is marked as unique
	var foundUnique bool
	for _, idx := range indexes {
		if idx.IndexName == "idx_unique_email" {
			require.True(s.t, idx.IsUnique, "index should be marked as unique")
			foundUnique = true
		}
	}
	require.True(s.t, foundUnique, "should find the unique index")
}

func (s PostgresSchemaObjectsTestSuite) TestAddTrigger() {
	tableName := s.schema + ".test_add_trigger"
	
	// Create table with trigger
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			value INT,
			updated_at TIMESTAMP
		)`, tableName))
	require.NoError(s.t, err)

	// Create a trigger function
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.update_timestamp()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.schema))
	require.NoError(s.t, err)

	// Create trigger
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TRIGGER trg_update_timestamp
		BEFORE UPDATE ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s.update_timestamp()`, tableName, s.schema))
	require.NoError(s.t, err)

	// Get the trigger definition
	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	
	triggers, err := s.connector.GetTriggersForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	require.NotEmpty(s.t, triggers, "should have at least one trigger")

	// Verify trigger properties
	var foundTrigger bool
	for _, trg := range triggers {
		if trg.TriggerName == "trg_update_timestamp" {
			require.Equal(s.t, "BEFORE", trg.Timing)
			require.Equal(s.t, "ROW", trg.ForEach)
			foundTrigger = true
		}
	}
	require.True(s.t, foundTrigger, "should find the trigger")

	// Test ReplayTableSchemaDeltas with trigger addition
	dstTableName := s.schema + ".test_add_trigger_dst"
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			value INT,
			updated_at TIMESTAMP
		)`, dstTableName))
	require.NoError(s.t, err)

	// Drop the source trigger to avoid name conflicts (in real scenarios, source and dest are in different schemas/databases)
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf("DROP TRIGGER trg_update_timestamp ON %s", tableName))
	require.NoError(s.t, err)

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_objects_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName:   tableName,
		DstTableName:   dstTableName,
		AddedTriggers: triggers,
	}}))

	// Verify trigger was created on destination
	var triggerName string
	err = s.connector.conn.QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT tgname FROM pg_trigger t
			JOIN pg_class c ON t.tgrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = '%s' AND c.relname = 'test_add_trigger_dst'
			AND t.tgname = 'trg_update_timestamp'`, s.schema)).Scan(&triggerName)
	require.NoError(s.t, err)
	require.Equal(s.t, "trg_update_timestamp", triggerName)
}

func (s PostgresSchemaObjectsTestSuite) TestDropIndex() {
	tableName := s.schema + ".test_drop_index"
	
	// Create table with index
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255)
		)`, tableName))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_to_drop ON %s(email)", tableName))
	require.NoError(s.t, err)

	// Test dropping the index via ReplayTableSchemaDeltas
	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_objects_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName:    tableName,
		DstTableName:    tableName,
		DroppedIndexes: []string{"idx_to_drop"},
	}}))

	// Verify index was dropped
	var count int
	err = s.connector.conn.QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT COUNT(*) FROM pg_indexes 
			WHERE schemaname = '%s' AND tablename = 'test_drop_index' 
			AND indexname = 'idx_to_drop'`, s.schema)).Scan(&count)
	require.NoError(s.t, err)
	require.Equal(s.t, 0, count, "index should be dropped")
}

func (s PostgresSchemaObjectsTestSuite) TestDropTrigger() {
	tableName := s.schema + ".test_drop_trigger"
	
	// Create table with trigger
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			value INT
		)`, tableName))
	require.NoError(s.t, err)

	// Create a simple trigger function
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.simple_trigger_func()
		RETURNS TRIGGER AS $$
		BEGIN
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.schema))
	require.NoError(s.t, err)

	// Create trigger
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TRIGGER trg_to_drop
		BEFORE INSERT ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s.simple_trigger_func()`, tableName, s.schema))
	require.NoError(s.t, err)

	// Test dropping the trigger via ReplayTableSchemaDeltas
	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_objects_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName:     tableName,
		DstTableName:     tableName,
		DroppedTriggers: []string{"trg_to_drop"},
	}}))

	// Verify trigger was dropped
	var count int
	err = s.connector.conn.QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT COUNT(*) FROM pg_trigger t
			JOIN pg_class c ON t.tgrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = '%s' AND c.relname = 'test_drop_trigger'
			AND t.tgname = 'trg_to_drop'`, s.schema)).Scan(&count)
	require.NoError(s.t, err)
	require.Equal(s.t, 0, count, "trigger should be dropped")
}

func (s PostgresSchemaObjectsTestSuite) TestCompareIndexes() {
	srcTableName := s.schema + ".test_compare_src"
	dstTableName := s.schema + ".test_compare_dst"
	
	// Create source table with indexes
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255),
			name VARCHAR(255)
		)`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_src_email ON %s(email)", srcTableName))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_src_name ON %s(name)", srcTableName))
	require.NoError(s.t, err)

	// Create destination table with different indexes
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255),
			name VARCHAR(255)
		)`, dstTableName))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_dst_email ON %s(email)", dstTableName))
	require.NoError(s.t, err)

	// Compare indexes
	srcSchemaTable, err := utils.ParseSchemaTable(srcTableName)
	require.NoError(s.t, err)
	dstSchemaTable, err := utils.ParseSchemaTable(dstTableName)
	require.NoError(s.t, err)

	addedIndexes, droppedIndexes, err := s.connector.CompareAndGenerateIndexDeltas(
		s.t.Context(), srcSchemaTable, dstSchemaTable)
	require.NoError(s.t, err)

	// Should have one added index (idx_src_name) and zero dropped (both have idx_email)
	require.Len(s.t, addedIndexes, 1, "should have one added index")
	require.Len(s.t, droppedIndexes, 0, "should have no dropped indexes")
}

func (s PostgresSchemaObjectsTestSuite) TestMultiColumnIndex() {
	tableName := s.schema + ".test_multi_col_index"
	
	// Create table with multi-column index
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			first_name VARCHAR(100),
			last_name VARCHAR(100),
			email VARCHAR(255)
		)`, tableName))
	require.NoError(s.t, err)

	// Create multi-column index
	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_name ON %s(last_name, first_name)", tableName))
	require.NoError(s.t, err)

	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	
	indexes, err := s.connector.GetIndexesForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	require.NotEmpty(s.t, indexes)
	
	// Verify multi-column index
	var found bool
	for _, idx := range indexes {
		if idx.IndexName == "idx_name" {
			require.Contains(s.t, idx.IndexDef, "last_name")
			require.Contains(s.t, idx.IndexDef, "first_name")
			found = true
		}
	}
	require.True(s.t, found, "should find multi-column index")
}

func (s PostgresSchemaObjectsTestSuite) TestPartialIndex() {
	tableName := s.schema + ".test_partial_index"
	
	// Create table with partial index (WHERE clause)
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			status VARCHAR(50),
			email VARCHAR(255)
		)`, tableName))
	require.NoError(s.t, err)

	// Create partial index with WHERE clause
	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_active_email ON %s(email) WHERE status = 'active'", tableName))
	require.NoError(s.t, err)

	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	
	indexes, err := s.connector.GetIndexesForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	
	// Verify partial index includes WHERE clause
	var found bool
	for _, idx := range indexes {
		if idx.IndexName == "idx_active_email" {
			require.Contains(s.t, idx.IndexDef, "WHERE")
			require.Contains(s.t, idx.IndexDef, "status")
			found = true
		}
	}
	require.True(s.t, found, "should find partial index with WHERE clause")
}

func (s PostgresSchemaObjectsTestSuite) TestExpressionIndex() {
	tableName := s.schema + ".test_expr_index"
	
	// Create table with expression index
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255)
		)`, tableName))
	require.NoError(s.t, err)

	// Create expression index (functional index)
	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_lower_email ON %s(LOWER(email))", tableName))
	require.NoError(s.t, err)

	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	
	indexes, err := s.connector.GetIndexesForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	
	// Verify expression index
	var found bool
	for _, idx := range indexes {
		if idx.IndexName == "idx_lower_email" {
			require.Contains(s.t, idx.IndexDef, "lower")
			found = true
		}
	}
	require.True(s.t, found, "should find expression index")
}

func (s PostgresSchemaObjectsTestSuite) TestMultipleTriggers() {
	tableName := s.schema + ".test_multi_triggers"
	
	// Create table
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			data TEXT,
			created_at TIMESTAMP,
			updated_at TIMESTAMP
		)`, tableName))
	require.NoError(s.t, err)

	// Create trigger functions
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.set_created_at()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.created_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.schema))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.set_updated_at()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.schema))
	require.NoError(s.t, err)

	// Create multiple triggers
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TRIGGER trg_created
		BEFORE INSERT ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s.set_created_at()`, tableName, s.schema))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TRIGGER trg_updated
		BEFORE UPDATE ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s.set_updated_at()`, tableName, s.schema))
	require.NoError(s.t, err)

	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	
	triggers, err := s.connector.GetTriggersForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	require.Len(s.t, triggers, 2, "should have 2 triggers")
}

func (s PostgresSchemaObjectsTestSuite) TestCompositeIndexAndTrigger() {
	srcTableName := s.schema + ".test_composite_src"
	dstTableName := s.schema + ".test_composite_dst"
	
	// Create source table with both indexes and triggers
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255),
			status VARCHAR(50),
			updated_at TIMESTAMP
		)`, srcTableName))
	require.NoError(s.t, err)

	// Add index
	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_email_status ON %s(email, status)", srcTableName))
	require.NoError(s.t, err)

	// Add trigger
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.update_timestamp_fn()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.schema))
	require.NoError(s.t, err)

	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TRIGGER trg_timestamp
		BEFORE UPDATE ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s.update_timestamp_fn()`, srcTableName, s.schema))
	require.NoError(s.t, err)

	// Get definitions
	schemaTable, err := utils.ParseSchemaTable(srcTableName)
	require.NoError(s.t, err)
	
	indexes, err := s.connector.GetIndexesForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	
	triggers, err := s.connector.GetTriggersForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)

	// Create destination and migrate both
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255),
			status VARCHAR(50),
			updated_at TIMESTAMP
		)`, dstTableName))
	require.NoError(s.t, err)

	// Drop source objects
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf("DROP INDEX %s.idx_email_status", s.schema))
	require.NoError(s.t, err)
	_, err = s.connector.conn.Exec(s.t.Context(), fmt.Sprintf("DROP TRIGGER trg_timestamp ON %s", srcTableName))
	require.NoError(s.t, err)

	// Replay both indexes and triggers
	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "composite_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName:  srcTableName,
		DstTableName:  dstTableName,
		AddedIndexes:  indexes,
		AddedTriggers: triggers,
	}}))

	// Verify both were created
	var indexCount, triggerCount int
	err = s.connector.conn.QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT COUNT(*) FROM pg_indexes 
			WHERE schemaname = '%s' AND tablename = 'test_composite_dst' 
			AND indexname = 'idx_email_status'`, s.schema)).Scan(&indexCount)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, indexCount)

	err = s.connector.conn.QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT COUNT(*) FROM pg_trigger t
			JOIN pg_class c ON t.tgrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = '%s' AND c.relname = 'test_composite_dst'
			AND t.tgname = 'trg_timestamp'`, s.schema)).Scan(&triggerCount)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, triggerCount)
}

func (s PostgresSchemaObjectsTestSuite) TestBTreeAndHashIndexTypes() {
	tableName := s.schema + ".test_index_types"
	
	// Create table
	_, err := s.connector.conn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			email VARCHAR(255),
			code INT
		)`, tableName))
	require.NoError(s.t, err)

	// Create BTree index (default)
	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_btree ON %s USING btree(email)", tableName))
	require.NoError(s.t, err)

	// Create Hash index
	_, err = s.connector.conn.Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_hash ON %s USING hash(code)", tableName))
	require.NoError(s.t, err)

	schemaTable, err := utils.ParseSchemaTable(tableName)
	require.NoError(s.t, err)
	
	indexes, err := s.connector.GetIndexesForTable(s.t.Context(), schemaTable)
	require.NoError(s.t, err)
	require.Len(s.t, indexes, 2, "should have 2 indexes")

	// Verify different index types
	hasbtree := false
	hashash := false
	for _, idx := range indexes {
		if strings.Contains(idx.IndexDef, "btree") {
			hasbtree = true
		}
		if strings.Contains(idx.IndexDef, "hash") {
			hashash = true
		}
	}
	require.True(s.t, hasbtree, "should have btree index")
	require.True(s.t, hashash, "should have hash index")
}

// Run all tests
func TestPostgresSchemaObjects(t *testing.T) {
	suite := SetupSchemaObjectsSuite(t)
	
	t.Run("TestAddIndex", func(t *testing.T) { suite.TestAddIndex() })
	t.Run("TestAddUniqueIndex", func(t *testing.T) { suite.TestAddUniqueIndex() })
	t.Run("TestAddTrigger", func(t *testing.T) { suite.TestAddTrigger() })
	t.Run("TestDropIndex", func(t *testing.T) { suite.TestDropIndex() })
	t.Run("TestDropTrigger", func(t *testing.T) { suite.TestDropTrigger() })
	t.Run("TestCompareIndexes", func(t *testing.T) { suite.TestCompareIndexes() })
	t.Run("TestMultiColumnIndex", func(t *testing.T) { suite.TestMultiColumnIndex() })
	t.Run("TestPartialIndex", func(t *testing.T) { suite.TestPartialIndex() })
	t.Run("TestExpressionIndex", func(t *testing.T) { suite.TestExpressionIndex() })
	t.Run("TestMultipleTriggers", func(t *testing.T) { suite.TestMultipleTriggers() })
	t.Run("TestCompositeIndexAndTrigger", func(t *testing.T) { suite.TestCompositeIndexAndTrigger() })
	t.Run("TestBTreeAndHashIndexTypes", func(t *testing.T) { suite.TestBTreeAndHashIndexTypes() })
}
