package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Test_Schema_Migration_Indexes tests that indexes are migrated from source to destination
func (s PeerFlowE2ETestSuitePG) Test_Schema_Migration_Indexes() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_schema_idx")
	dstTableName := s.attachSchemaSuffix("test_schema_idx_dst")

	// Create table with indexes on source
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2),
			category VARCHAR(50),
			created_at TIMESTAMP DEFAULT NOW()
		);
	`, srcTableName))
	require.NoError(s.t, err)

	// Create multiple indexes on source
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE INDEX idx_name_%s ON %s(name);
		CREATE INDEX idx_price_%s ON %s(price);
		CREATE INDEX idx_category_name_%s ON %s(category, name);
	`, s.suffix, srcTableName, s.suffix, srcTableName, s.suffix, srcTableName))
	require.NoError(s.t, err)

	// Insert some test data
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (name, price, category) VALUES
		('Product A', 99.99, 'PeerDB'),
		('Product B', 49.99, 'Databases'),
		('Product C', 149.99, 'DBaaS');
	`, srcTableName))
	require.NoError(s.t, err)

	// Create mirror
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_idx_migration"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	s.t.Log("Waiting for initial snapshot to complete")
	EnvWaitFor(s.t, env, 2*time.Minute, "normalize rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,name,price,category") == nil
	})

	// Verify indexes were migrated to destination
	rows, err := s.Conn().Query(s.t.Context(), fmt.Sprintf(`
		SELECT indexname
		FROM pg_indexes
		WHERE schemaname = 'e2e_test_%s'
		AND tablename = 'test_schema_idx_dst'
		AND indexname NOT LIKE '%%pkey'
		ORDER BY indexname
	`, s.suffix))
	require.NoError(s.t, err)
	defer rows.Close()

	indexes := []string{}
	for rows.Next() {
		var indexName string
		err := rows.Scan(&indexName)
		require.NoError(s.t, err)
		indexes = append(indexes, indexName)
	}

	s.t.Logf("Found indexes on destination: %v", indexes)

	// Verify all three indexes were created
	require.Contains(s.t, indexes, fmt.Sprintf("idx_name_%s", s.suffix))
	require.Contains(s.t, indexes, fmt.Sprintf("idx_price_%s", s.suffix))
	require.Contains(s.t, indexes, fmt.Sprintf("idx_category_name_%s", s.suffix))
	require.GreaterOrEqual(s.t, len(indexes), 3, "Expected at least 3 indexes on destination")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_Schema_Migration_Triggers tests that triggers are migrated from source to destination
func (s PeerFlowE2ETestSuitePG) Test_Schema_Migration_Triggers() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_schema_trig")
	dstTableName := s.attachSchemaSuffix("test_schema_trig_dst")

	// Create table on source
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			updated_at TIMESTAMP,
			update_count INTEGER DEFAULT 0
		);
	`, srcTableName))
	require.NoError(s.t, err)

	// Create trigger function
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION update_timestamp_%s()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			NEW.update_count = OLD.update_count + 1;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`, s.suffix))
	require.NoError(s.t, err)

	// Create trigger on source table
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TRIGGER trg_update_%s
		BEFORE UPDATE ON %s
		FOR EACH ROW
		EXECUTE FUNCTION update_timestamp_%s();
	`, s.suffix, srcTableName, s.suffix))
	require.NoError(s.t, err)

	// Insert test data
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (name) VALUES ('Test Item');
	`, srcTableName))
	require.NoError(s.t, err)

	// Create mirror
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_trig_migration"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	s.t.Log("Waiting for initial snapshot to complete")
	EnvWaitFor(s.t, env, 2*time.Minute, "normalize rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,name") == nil
	})

	// Verify trigger exists on destination
	var triggerExists bool
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1
			FROM pg_trigger t
			JOIN pg_class c ON t.tgrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = 'e2e_test_%s'
			AND c.relname = 'test_schema_trig_dst'
			AND t.tgname = 'trg_update_%s'
		)
	`, s.suffix, s.suffix)).Scan(&triggerExists)
	require.NoError(s.t, err)
	require.True(s.t, triggerExists, "Trigger should exist on destination table")

	s.t.Log("Trigger successfully migrated to destination")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_Schema_Migration_Constraints tests that constraints are migrated from source to destination
func (s PeerFlowE2ETestSuitePG) Test_Schema_Migration_Constraints() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_schema_const")
	dstTableName := s.attachSchemaSuffix("test_schema_const_dst")

	// Create table with constraints on source
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2),
			quantity INTEGER,
			email VARCHAR(255)
		);
	`, srcTableName))
	require.NoError(s.t, err)

	// Add various constraints
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT chk_price_%s CHECK (price >= 0),
		ADD CONSTRAINT chk_quantity_%s CHECK (quantity >= 0),
		ADD CONSTRAINT chk_name_%s CHECK (length(name) > 0);
	`, srcTableName, s.suffix, s.suffix, s.suffix))
	require.NoError(s.t, err)

	// Insert valid test data
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (name, price, quantity, email) VALUES
		('Product A', 99.99, 10, 'test@example.com'),
		('Product B', 49.99, 5, 'test2@example.com');
	`, srcTableName))
	require.NoError(s.t, err)

	// Create mirror
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_const_migration"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	s.t.Log("Waiting for initial snapshot to complete")
	EnvWaitFor(s.t, env, 2*time.Minute, "normalize rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,name,price,quantity,email") == nil
	})

	// Verify constraints were migrated to destination
	rows, err := s.Conn().Query(s.t.Context(), fmt.Sprintf(`
		SELECT conname
		FROM pg_constraint
		WHERE conrelid = 'e2e_test_%s.test_schema_const_dst'::regclass
		AND contype = 'c'
		ORDER BY conname
	`, s.suffix))
	require.NoError(s.t, err)
	defer rows.Close()

	constraints := []string{}
	for rows.Next() {
		var constraintName string
		err := rows.Scan(&constraintName)
		require.NoError(s.t, err)
		constraints = append(constraints, constraintName)
	}

	s.t.Logf("Found constraints on destination: %v", constraints)

	// Verify all three check constraints were created
	require.Contains(s.t, constraints, fmt.Sprintf("chk_price_%s", s.suffix))
	require.Contains(s.t, constraints, fmt.Sprintf("chk_quantity_%s", s.suffix))
	require.Contains(s.t, constraints, fmt.Sprintf("chk_name_%s", s.suffix))
	require.GreaterOrEqual(s.t, len(constraints), 3, "Expected at least 3 constraints on destination")

	s.t.Log("Constraints successfully migrated to destination")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_Schema_Migration_Combined tests that indexes, triggers, and constraints all work together
func (s PeerFlowE2ETestSuitePG) Test_Schema_Migration_Combined() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_schema_combined")
	dstTableName := s.attachSchemaSuffix("test_schema_combined_dst")

	// Create table with all schema objects on source
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			price DECIMAL(10,2),
			updated_at TIMESTAMP
		);
	`, srcTableName))
	require.NoError(s.t, err)

	// Add index
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE INDEX idx_combined_name_%s ON %s(name);
	`, s.suffix, srcTableName))
	require.NoError(s.t, err)

	// Add constraint
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		ALTER TABLE %s ADD CONSTRAINT chk_combined_price_%s CHECK (price > 0);
	`, srcTableName, s.suffix))
	require.NoError(s.t, err)

	// Add trigger
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION update_combined_%s()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER trg_combined_%s
		BEFORE UPDATE ON %s
		FOR EACH ROW
		EXECUTE FUNCTION update_combined_%s();
	`, s.suffix, s.suffix, srcTableName, s.suffix))
	require.NoError(s.t, err)

	// Insert test data
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (name, price) VALUES ('Combined Test', 99.99);
	`, srcTableName))
	require.NoError(s.t, err)

	// Create mirror
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_combined_migration"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	s.t.Log("Waiting for initial snapshot to complete")
	EnvWaitFor(s.t, env, 2*time.Minute, "normalize rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,name,price") == nil
	})

	// Verify index exists
	var indexExists bool
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1 FROM pg_indexes
			WHERE schemaname = 'e2e_test_%s'
			AND tablename = 'test_schema_combined_dst'
			AND indexname = 'idx_combined_name_%s'
		)
	`, s.suffix, s.suffix)).Scan(&indexExists)
	require.NoError(s.t, err)
	require.True(s.t, indexExists, "Index should exist on destination")

	// Verify constraint exists
	var constraintExists bool
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1 FROM pg_constraint
			WHERE conrelid = 'e2e_test_%s.test_schema_combined_dst'::regclass
			AND conname = 'chk_combined_price_%s'
		)
	`, s.suffix, s.suffix)).Scan(&constraintExists)
	require.NoError(s.t, err)
	require.True(s.t, constraintExists, "Constraint should exist on destination")

	// Verify trigger exists
	var triggerExists bool
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1
			FROM pg_trigger t
			JOIN pg_class c ON t.tgrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = 'e2e_test_%s'
			AND c.relname = 'test_schema_combined_dst'
			AND t.tgname = 'trg_combined_%s'
		)
	`, s.suffix, s.suffix)).Scan(&triggerExists)
	require.NoError(s.t, err)
	require.True(s.t, triggerExists, "Trigger should exist on destination")

	s.t.Log("All schema objects (index, constraint, trigger) successfully migrated!")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_Schema_Migration_InitialSnapshotOnly tests schema migration for initial snapshot only mirrors
func (s PeerFlowE2ETestSuitePG) Test_Schema_Migration_InitialSnapshotOnly() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_schema_snapshot")
	dstTableName := s.attachSchemaSuffix("test_schema_snapshot_dst")

	// Create table with indexes on source
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			data VARCHAR(100)
		);
		CREATE INDEX idx_snapshot_%s ON %s(data);
	`, srcTableName, s.suffix, srcTableName))
	require.NoError(s.t, err)

	// Insert test data
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (data) VALUES ('Snapshot Test');
	`, srcTableName))
	require.NoError(s.t, err)

	// Create initial snapshot only mirror
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_snapshot_migration"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.InitialSnapshotOnly = true

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	s.t.Log("Waiting for snapshot-only mirror to complete")
	EnvWaitFor(s.t, env, 2*time.Minute, "snapshot complete", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,data") == nil
	})

	// Verify index was migrated even for snapshot-only
	var indexExists bool
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1 FROM pg_indexes
			WHERE schemaname = 'e2e_test_%s'
			AND tablename = 'test_schema_snapshot_dst'
			AND indexname = 'idx_snapshot_%s'
		)
	`, s.suffix, s.suffix)).Scan(&indexExists)
	require.NoError(s.t, err)
	require.True(s.t, indexExists, "Index should be migrated for snapshot-only mirrors")

	s.t.Log("Schema migration works for initial snapshot only mirrors!")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_Schema_Migration_Functions_And_Triggers tests that functions and triggers are migrated together
func (s PeerFlowE2ETestSuitePG) Test_Schema_Migration_Functions_And_Triggers() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_func_trigger_src")
	dstTableName := s.attachSchemaSuffix("test_func_trigger_dst")

	// Create source table with function and trigger
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			value INTEGER,
			updated_at TIMESTAMP
		);

		-- Create function
		CREATE OR REPLACE FUNCTION update_timestamp_%s()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		-- Create trigger that uses the function
		CREATE TRIGGER trg_update_ts_%s
		BEFORE UPDATE ON %s
		FOR EACH ROW
		EXECUTE FUNCTION update_timestamp_%s();

		INSERT INTO %s (value) VALUES (10), (20);
	`, srcTableName, s.suffix, s.suffix, srcTableName, s.suffix, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_func_trigger_mirror"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial snapshot to complete
	EnvWaitFor(s.t, env, 2*time.Minute, "snapshot complete", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,value") == nil
	})

	// Verify function exists on destination
	var functionExists bool
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1
			FROM pg_proc p
			JOIN pg_namespace n ON p.pronamespace = n.oid
			WHERE n.nspname = 'e2e_test_%s'
			AND p.proname = 'update_timestamp_%s'
		)
	`, s.suffix, s.suffix)).Scan(&functionExists)
	require.NoError(s.t, err)
	require.True(s.t, functionExists, "Function should exist on destination")

	// Verify trigger exists on destination
	var triggerExists bool
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1
			FROM pg_trigger t
			JOIN pg_class c ON t.tgrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = 'e2e_test_%s'
			AND c.relname = 'test_func_trigger_dst'
			AND t.tgname = 'trg_update_ts_%s'
		)
	`, s.suffix, s.suffix)).Scan(&triggerExists)
	require.NoError(s.t, err)
	require.True(s.t, triggerExists, "Trigger should exist on destination")

	s.t.Log("Functions and triggers successfully migrated together!")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
