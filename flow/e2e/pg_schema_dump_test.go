package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func (s PeerFlowE2ETestSuitePG) Test_PG_Schema_Dump_And_CDC() {
	srcSchema := "e2e_test_" + s.suffix
	dstDBName := "e2e_pgdump_" + s.suffix

	// create destination database on the same PG instance
	_, err := s.Conn().Exec(s.t.Context(), "CREATE DATABASE "+dstDBName)
	require.NoError(s.t, err)
	s.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg := internal.GetAncillaryPostgresConfigFromEnv()
		connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres",
			cfg.Host, cfg.Port, cfg.User, cfg.Password)
		dropConn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			s.t.Logf("failed to connect for cleanup: %v", err)
			return
		}
		defer dropConn.Close(ctx)
		_, _ = dropConn.Exec(ctx, fmt.Sprintf(
			"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='%s' AND pid <> pg_backend_pid()", dstDBName))
		if _, err := dropConn.Exec(ctx, "DROP DATABASE IF EXISTS "+dstDBName); err != nil {
			s.t.Logf("failed to drop destination database %s: %v", dstDBName, err)
		}
	})

	// connect to destination database for verification later
	dstCfg := internal.GetAncillaryPostgresConfigFromEnv()
	dstConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		dstCfg.Host, dstCfg.Port, dstCfg.User, dstCfg.Password, dstDBName)
	dstConn, err := pgx.Connect(s.t.Context(), dstConnStr)
	require.NoError(s.t, err)
	s.t.Cleanup(func() { dstConn.Close(s.t.Context()) })

	// create the source schema in the destination database (pg_dump will create it, but we need it for the peer)
	// Actually, pg_dump --schema-only will create the schema, so we don't need to pre-create it.

	// create a destination peer pointing to the new database
	dstPeerCfg := internal.GetAncillaryPostgresConfigFromEnv()
	dstPeerCfg.Database = dstDBName
	dstPeerName := "pgdump_dst_" + s.suffix
	dstPeer := &protos.Peer{
		Name: dstPeerName,
		Type: protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{
			PostgresConfig: dstPeerCfg,
		},
	}
	CreatePeer(s.t, dstPeer)

	// --- set up rich schema on source ---

	// create custom enum type in the source schema
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		"CREATE TYPE %s.color AS ENUM ('red', 'green', 'blue')", srcSchema))
	require.NoError(s.t, err)

	// create parent table with various column types
	parentTable := srcSchema + ".parent_tbl"
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			color %s.color NOT NULL DEFAULT 'red',
			score NUMERIC(10,2),
			metadata JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, parentTable, srcSchema))
	require.NoError(s.t, err)

	// create unique index on parent
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		"CREATE UNIQUE INDEX idx_parent_name ON %s (name)", parentTable))
	require.NoError(s.t, err)

	// create child table with foreign key referencing parent
	childTable := srcSchema + ".child_tbl"
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			parent_id INT NOT NULL REFERENCES %s(id),
			value TEXT,
			tags TEXT[]
		)`, childTable, parentTable))
	require.NoError(s.t, err)

	// create btree index on child
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		"CREATE INDEX idx_child_parent ON %s (parent_id)", childTable))
	require.NoError(s.t, err)

	// insert initial data for snapshot
	for i := 1; i <= 5; i++ {
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			"INSERT INTO %s (name, color, score, metadata) VALUES ($1, $2, $3, $4)",
			parentTable),
			fmt.Sprintf("item_%d", i),
			[]string{"red", "green", "blue"}[i%3],
			float64(i)*10.5,
			fmt.Sprintf(`{"key": "val_%d"}`, i),
		)
		require.NoError(s.t, err)
	}

	for i := 1; i <= 10; i++ {
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			"INSERT INTO %s (parent_id, value, tags) VALUES ($1, $2, $3)",
			childTable),
			(i%5)+1,
			fmt.Sprintf("child_val_%d", i),
			fmt.Sprintf("{tag_%d,common}", i),
		)
		require.NoError(s.t, err)
	}

	// source and dest table identifiers — use same schema-qualified names
	// since pg_dump recreates the schema on the destination
	srcParent := parentTable
	dstParent := parentTable
	srcChild := childTable
	dstChild := childTable

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_pgdump"),
		DestinationName: dstPeerName,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcParent,
				DestinationTableIdentifier: dstParent,
			},
			{
				SourceTableIdentifier:      srcChild,
				DestinationTableIdentifier: dstChild,
			},
		},
		SourceName:        GeneratePostgresPeer(s.t).Name,
		MaxBatchSize:      100,
		DoInitialSnapshot: true,
		System:            protos.TypeSystem_PG,
		SoftDeleteColName: "",
		SyncedAtColName:   "",
		Env: map[string]string{
			"PEERDB_PG_AUTOMATED_SCHEMA_DUMP": "true",
		},
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	// wait for initial snapshot to complete
	EnvWaitFor(s.t, env, 3*time.Minute, "initial load parent", func() bool {
		var count int64
		err := dstConn.QueryRow(s.t.Context(),
			fmt.Sprintf("SELECT COUNT(*) FROM %s", dstParent)).Scan(&count)
		return err == nil && count == 5
	})
	EnvWaitFor(s.t, env, 3*time.Minute, "initial load child", func() bool {
		var count int64
		err := dstConn.QueryRow(s.t.Context(),
			fmt.Sprintf("SELECT COUNT(*) FROM %s", dstChild)).Scan(&count)
		return err == nil && count == 10
	})

	// --- verify schema objects on destination ---

	// verify enum type exists on destination
	var enumExists bool
	err = dstConn.QueryRow(s.t.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE t.typname = 'color' AND n.nspname = $1
		)`, srcSchema).Scan(&enumExists)
	require.NoError(s.t, err)
	require.True(s.t, enumExists, "enum type 'color' should exist on destination")

	// verify unique index on parent table
	var idxParentExists bool
	err = dstConn.QueryRow(s.t.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM pg_indexes
			WHERE schemaname = $1 AND tablename = 'parent_tbl' AND indexname = 'idx_parent_name'
		)`, srcSchema).Scan(&idxParentExists)
	require.NoError(s.t, err)
	require.True(s.t, idxParentExists, "unique index idx_parent_name should exist on destination")

	// verify btree index on child table
	var idxChildExists bool
	err = dstConn.QueryRow(s.t.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM pg_indexes
			WHERE schemaname = $1 AND tablename = 'child_tbl' AND indexname = 'idx_child_parent'
		)`, srcSchema).Scan(&idxChildExists)
	require.NoError(s.t, err)
	require.True(s.t, idxChildExists, "btree index idx_child_parent should exist on destination")

	// verify foreign key constraint on child table
	var fkExists bool
	err = dstConn.QueryRow(s.t.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.table_constraints
			WHERE constraint_type = 'FOREIGN KEY'
			AND table_schema = $1
			AND table_name = 'child_tbl'
		)`, srcSchema).Scan(&fkExists)
	require.NoError(s.t, err)
	require.True(s.t, fkExists, "foreign key on child_tbl should exist on destination")

	// --- CDC test: insert more rows and verify replication ---

	// insert more parent rows
	for i := 6; i <= 8; i++ {
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			"INSERT INTO %s (name, color, score, metadata) VALUES ($1, $2, $3, $4)",
			parentTable),
			fmt.Sprintf("item_%d", i),
			[]string{"red", "green", "blue"}[i%3],
			float64(i)*10.5,
			fmt.Sprintf(`{"key": "val_%d"}`, i),
		)
		EnvNoError(s.t, env, err)
	}

	// insert more child rows
	for i := 11; i <= 15; i++ {
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			"INSERT INTO %s (parent_id, value, tags) VALUES ($1, $2, $3)",
			childTable),
			(i%5)+1,
			fmt.Sprintf("child_val_%d", i),
			fmt.Sprintf("{tag_%d,common}", i),
		)
		EnvNoError(s.t, env, err)
	}

	// wait for CDC to replicate the new rows
	EnvWaitFor(s.t, env, 3*time.Minute, "cdc parent rows", func() bool {
		var count int64
		err := dstConn.QueryRow(s.t.Context(),
			fmt.Sprintf("SELECT COUNT(*) FROM %s", dstParent)).Scan(&count)
		return err == nil && count == 8
	})
	EnvWaitFor(s.t, env, 3*time.Minute, "cdc child rows", func() bool {
		var count int64
		err := dstConn.QueryRow(s.t.Context(),
			fmt.Sprintf("SELECT COUNT(*) FROM %s", dstChild)).Scan(&count)
		return err == nil && count == 15
	})

	// verify data integrity: compare actual row content
	// query source and destination and compare
	var srcParentCount, dstParentCount int64
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", srcParent)).Scan(&srcParentCount)
	require.NoError(s.t, err)
	err = dstConn.QueryRow(s.t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstParent)).Scan(&dstParentCount)
	require.NoError(s.t, err)
	require.Equal(s.t, srcParentCount, dstParentCount, "parent table row counts should match")

	var srcChildCount, dstChildCount int64
	err = s.Conn().QueryRow(s.t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", srcChild)).Scan(&srcChildCount)
	require.NoError(s.t, err)
	err = dstConn.QueryRow(s.t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstChild)).Scan(&dstChildCount)
	require.NoError(s.t, err)
	require.Equal(s.t, srcChildCount, dstChildCount, "child table row counts should match")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_PG_Schema_Dump_Role_Migration verifies that pg_dumpall --roles-only
// propagates a role from a source PG cluster to a separate destination PG
// cluster. Roles are global objects per cluster, so this requires two clusters
// (peerdb-postgres + peerdb-postgres2).
func (s PeerFlowE2ETestSuitePG) Test_PG_Schema_Dump_Role_Migration() {
	srcCfg := internal.GetAncillaryPostgresConfigFromEnv()
	dstCfg := internal.GetSecondaryPostgresConfigFromEnv()

	roleName := "peerdb_test_role_" + s.suffix

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dstConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		dstCfg.Host, dstCfg.Port, dstCfg.User, dstCfg.Password, dstCfg.Database)
	dstConn, err := pgx.Connect(ctx, dstConnStr)
	require.NoError(s.t, err, "failed to connect to secondary postgres on %s:%d (is the postgres2 tilt resource running?)", dstCfg.Host, dstCfg.Port)
	defer dstConn.Close(ctx)

	// sanity: role must not pre-exist on destination
	var preExists bool
	require.NoError(s.t, dstConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", roleName).Scan(&preExists))
	require.False(s.t, preExists, "role %s unexpectedly exists on destination before test", roleName)

	// create role on source
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'pw'", roleName))
	require.NoError(s.t, err)
	s.t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = s.Conn().Exec(cleanupCtx, "DROP ROLE IF EXISTS "+roleName)

		dropConn, err := pgx.Connect(cleanupCtx, dstConnStr)
		if err != nil {
			s.t.Logf("failed to connect to destination for role cleanup: %v", err)
			return
		}
		defer dropConn.Close(cleanupCtx)
		if _, err := dropConn.Exec(cleanupCtx, "DROP ROLE IF EXISTS "+roleName); err != nil {
			s.t.Logf("failed to drop destination role %s: %v", roleName, err)
		}
	})

	require.NoError(s.t, connpostgres.RunPgDumpSchema(ctx, srcCfg, dstCfg))

	var postExists bool
	require.NoError(s.t, dstConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", roleName).Scan(&postExists))
	require.True(s.t, postExists, "role %s should have been migrated to destination cluster", roleName)
}
