package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Test_Schema_Objects_Migration_PG tests migration of indexes and triggers from Postgres to Postgres
func (s PeerFlowE2ETestSuitePG) Test_Schema_Objects_Migration_PG() {
	srcTableName := s.attachSchemaSuffix("test_schema_objects_src")
	dstTableName := s.attachSchemaSuffix("test_schema_objects_dst")

	// Create source table with indexes and triggers
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			email VARCHAR(255),
			name VARCHAR(255),
			created_at TIMESTAMP,
			updated_at TIMESTAMP
		);
	`, srcTableName))
	require.NoError(s.t, err)

	// Create indexes on source table
	_, err = s.Conn().Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_email ON %s(email)", srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), 
		fmt.Sprintf("CREATE UNIQUE INDEX idx_unique_email ON %s(email)", srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_name ON %s(name)", srcTableName))
	require.NoError(s.t, err)

	// Create trigger function
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION e2e_test_%s.update_timestamp()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.suffix))
	require.NoError(s.t, err)

	// Create trigger on source table
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TRIGGER trg_update_timestamp
		BEFORE UPDATE ON %s
		FOR EACH ROW
		EXECUTE FUNCTION e2e_test_%s.update_timestamp()`, srcTableName, s.suffix))
	require.NoError(s.t, err)

	// Set up flow
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_schema_objects_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)

	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Insert test data
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(email, name, created_at, updated_at) 
		VALUES ('test@example.com', 'Test User', NOW(), NOW())
	`, srcTableName))
	EnvNoError(s.t, env, err)

	s.t.Log("Inserted initial row into the source table")

	// Wait for initial sync
	EnvWaitFor(s.t, env, 2*time.Minute, "normalize initial data", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,email,name") == nil
	})

	// Verify indexes were created on destination
	var indexCount int
	err = s.Conn().QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT COUNT(*) FROM pg_indexes 
			WHERE schemaname = 'e2e_test_%s' 
			AND tablename = 'test_schema_objects_dst'
			AND indexname IN ('idx_email', 'idx_unique_email', 'idx_name')`, s.suffix)).Scan(&indexCount)
	require.NoError(s.t, err)
	require.Equal(s.t, 3, indexCount, "All indexes should be created on destination")

	// Verify trigger was created on destination
	var triggerCount int
	err = s.Conn().QueryRow(s.t.Context(), 
		fmt.Sprintf(`SELECT COUNT(*) FROM pg_trigger t
			JOIN pg_class c ON t.tgrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = 'e2e_test_%s' 
			AND c.relname = 'test_schema_objects_dst'
			AND t.tgname = 'trg_update_timestamp'`, s.suffix)).Scan(&triggerCount)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, triggerCount, "Trigger should be created on destination")

	// Add a new index on source
	_, err = s.Conn().Exec(s.t.Context(), 
		fmt.Sprintf("CREATE INDEX idx_created_at ON %s(created_at)", srcTableName))
	require.NoError(s.t, err)
	s.t.Log("Added new index idx_created_at to source table")

	// Insert another row to trigger schema delta detection
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(email, name, created_at, updated_at) 
		VALUES ('test2@example.com', 'Test User 2', NOW(), NOW())
	`, srcTableName))
	EnvNoError(s.t, env, err)

	// Wait for new index to be replicated
	EnvWaitFor(s.t, env, 2*time.Minute, "replicate new index", func() bool {
		var newIndexCount int
		err := s.Conn().QueryRow(s.t.Context(), 
			fmt.Sprintf(`SELECT COUNT(*) FROM pg_indexes 
				WHERE schemaname = 'e2e_test_%s' 
				AND tablename = 'test_schema_objects_dst'
				AND indexname = 'idx_created_at'`, s.suffix)).Scan(&newIndexCount)
		return err == nil && newIndexCount == 1
	})

	s.t.Log("New index successfully replicated to destination")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
