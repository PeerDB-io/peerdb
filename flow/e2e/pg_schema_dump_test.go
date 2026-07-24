package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// setupDedicatedPgDumpSource creates a fresh database on the primary PG instance
// to serve as the source for schema-dump tests. pg_dump dumps a whole database,
// so sharing the source DB with other parallel tests caused the dump to include
// every concurrent test's e2e_test_<suffix> schema, blowing past the workflow
// SETUP timeout. A dedicated source DB keeps the dump scoped to this test.
//
// Returns a connection to the new DB, the registered source peer name, and the
// schema name to use within it. All resources are cleaned up via t.Cleanup.
func setupDedicatedPgDumpSource(t *testing.T, suffix string) (*pgx.Conn, string, string) {
	t.Helper()

	srcCfg := internal.GetAncillaryPostgresConfigFromEnv()
	srcDBName := "e2e_pgdump_src_" + suffix
	srcSchema := "e2e_test_" + suffix

	bootstrapStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		srcCfg.Host, srcCfg.Port, srcCfg.User, srcCfg.Password, srcCfg.Database)
	bootstrap, err := pgx.Connect(t.Context(), bootstrapStr)
	require.NoError(t, err)
	_, err = bootstrap.Exec(t.Context(), "CREATE DATABASE "+srcDBName)
	require.NoError(t, err)
	bootstrap.Close(t.Context())

	srcConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		srcCfg.Host, srcCfg.Port, srcCfg.User, srcCfg.Password, srcDBName)
	srcConn, err := pgx.Connect(t.Context(), srcConnStr)
	require.NoError(t, err)
	_, err = srcConn.Exec(t.Context(), "CREATE SCHEMA "+srcSchema)
	require.NoError(t, err)

	srcPeerCfg := internal.GetAncillaryPostgresConfigFromEnv()
	srcPeerCfg.Database = srcDBName
	srcPeerName := "pgdump_src_" + suffix
	CreatePeer(t, &protos.Peer{
		Name:   srcPeerName,
		Type:   protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{PostgresConfig: srcPeerCfg},
	})

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srcConn.Close(ctx)
		dropConn, err := pgx.Connect(ctx, bootstrapStr)
		if err != nil {
			t.Logf("failed to connect for source DB cleanup: %v", err)
			return
		}
		defer dropConn.Close(ctx)
		if _, err := dropConn.Exec(ctx, "DROP DATABASE IF EXISTS "+srcDBName+" WITH (FORCE)"); err != nil {
			t.Logf("failed to drop source database %s: %v", srcDBName, err)
		}
	})

	return srcConn, srcPeerName, srcSchema
}

func (s PeerFlowE2ETestSuitePG) Test_PG_Schema_Dump_And_CDC() {
	srcConn, srcPeerName, srcSchema := setupDedicatedPgDumpSource(s.t, s.suffix)

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
		if _, err := dropConn.Exec(ctx, "DROP DATABASE IF EXISTS "+dstDBName+" WITH (FORCE)"); err != nil {
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
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(
		"CREATE TYPE %s.color AS ENUM ('red', 'green', 'blue')", srcSchema))
	require.NoError(s.t, err)

	// create parent table with various column types
	parentTable := srcSchema + ".parent_tbl"
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(`
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
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(
		"CREATE UNIQUE INDEX idx_parent_name ON %s (name)", parentTable))
	require.NoError(s.t, err)

	// create child table with foreign key referencing parent
	childTable := srcSchema + ".child_tbl"
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			parent_id INT NOT NULL REFERENCES %s(id),
			value TEXT,
			tags TEXT[]
		)`, childTable, parentTable))
	require.NoError(s.t, err)

	// create btree index on child
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(
		"CREATE INDEX idx_child_parent ON %s (parent_id)", childTable))
	require.NoError(s.t, err)

	// insert initial data for snapshot
	for i := 1; i <= 5; i++ {
		_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(
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
		_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(
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
		SourceName:        srcPeerName,
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
			"SELECT COUNT(*) FROM "+dstParent).Scan(&count)
		return err == nil && count == 5
	})
	EnvWaitFor(s.t, env, 3*time.Minute, "initial load child", func() bool {
		var count int64
		err := dstConn.QueryRow(s.t.Context(),
			"SELECT COUNT(*) FROM "+dstChild).Scan(&count)
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
		_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(
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
		_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(
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
			"SELECT COUNT(*) FROM "+dstParent).Scan(&count)
		return err == nil && count == 8
	})
	EnvWaitFor(s.t, env, 3*time.Minute, "cdc child rows", func() bool {
		var count int64
		err := dstConn.QueryRow(s.t.Context(),
			"SELECT COUNT(*) FROM "+dstChild).Scan(&count)
		return err == nil && count == 15
	})

	// verify data integrity: compare actual row content
	// query source and destination and compare
	var srcParentCount, dstParentCount int64
	err = srcConn.QueryRow(s.t.Context(), "SELECT COUNT(*) FROM "+srcParent).Scan(&srcParentCount)
	require.NoError(s.t, err)
	err = dstConn.QueryRow(s.t.Context(), "SELECT COUNT(*) FROM "+dstParent).Scan(&dstParentCount)
	require.NoError(s.t, err)
	require.Equal(s.t, srcParentCount, dstParentCount, "parent table row counts should match")

	var srcChildCount, dstChildCount int64
	err = srcConn.QueryRow(s.t.Context(), "SELECT COUNT(*) FROM "+srcChild).Scan(&srcChildCount)
	require.NoError(s.t, err)
	err = dstConn.QueryRow(s.t.Context(), "SELECT COUNT(*) FROM "+dstChild).Scan(&dstChildCount)
	require.NoError(s.t, err)
	require.Equal(s.t, srcChildCount, dstChildCount, "child table row counts should match")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_PG_Schema_Dump_No_Owner_No_Privileges verifies that the schema dump does
// not emit owner or grant statements that reference roles. We create a role on
// the source, give it ownership and grants on a table, then dump into a
// secondary cluster where that role does NOT exist. With --no-owner and
// --no-privileges the dump must succeed; without them it would fail on
// ALTER TABLE ... OWNER TO <missing_role> / GRANT ... TO <missing_role>.
//
// Also verifies initial load + CDC into the dumped table on the destination
// cluster, so we know the table is usable end-to-end.
func (s PeerFlowE2ETestSuitePG) Test_PG_Schema_Dump_No_Owner_No_Privileges() {
	srcConn, srcPeerName, srcSchema := setupDedicatedPgDumpSource(s.t, s.suffix)

	dstCfg := internal.GetSecondaryPostgresConfigFromEnv()

	roleName := "peerdb_owner_role_" + s.suffix
	tableName := "owned_tbl"
	qualified := fmt.Sprintf("%s.%s", srcSchema, tableName)

	// destination connection (for assertions + role-absence sanity)
	dstConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		dstCfg.Host, dstCfg.Port, dstCfg.User, dstCfg.Password, dstCfg.Database)
	dstConn, err := pgx.Connect(s.t.Context(), dstConnStr)
	require.NoError(s.t, err, "failed to connect to secondary postgres on %s:%d (postgres2 tilt resource running?)",
		dstCfg.Host, dstCfg.Port)
	s.t.Cleanup(func() { dstConn.Close(s.t.Context()) })

	// sanity: role must not exist on destination
	var roleExistsOnDst bool
	require.NoError(s.t, dstConn.QueryRow(s.t.Context(),
		"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", roleName).Scan(&roleExistsOnDst))
	require.False(s.t, roleExistsOnDst, "role %s unexpectedly exists on destination", roleName)

	// create role + owned/granted table on source with seed rows.
	// the role is cluster-wide; the table lives in the dedicated source DB
	// and will go away when that DB is dropped during cleanup.
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'pw'", roleName))
	require.NoError(s.t, err)
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf(
		"CREATE TABLE %s (id SERIAL PRIMARY KEY, val TEXT)", qualified))
	require.NoError(s.t, err)
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qualified, roleName))
	require.NoError(s.t, err)
	_, err = srcConn.Exec(s.t.Context(), fmt.Sprintf("GRANT SELECT, INSERT ON %s TO %s", qualified, roleName))
	require.NoError(s.t, err)

	for i := 1; i <= 5; i++ {
		_, err = srcConn.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s (val) VALUES ($1)", qualified),
			fmt.Sprintf("snap_%d", i))
		require.NoError(s.t, err)
	}

	// role is cluster-wide so it outlives the dedicated source DB; drop it
	// against the shared source connection after the source DB is gone.
	s.t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = s.Conn().Exec(cleanupCtx, "DROP ROLE IF EXISTS "+roleName)

		dropConn, err := pgx.Connect(cleanupCtx, dstConnStr)
		if err != nil {
			s.t.Logf("failed to connect to destination for cleanup: %v", err)
			return
		}
		defer dropConn.Close(cleanupCtx)
		_, _ = dropConn.Exec(cleanupCtx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", srcSchema))
	})

	// register destination peer pointing at postgres2
	dstPeerName := "pgdump_noowner_dst_" + s.suffix
	CreatePeer(s.t, &protos.Peer{
		Name:   dstPeerName,
		Type:   protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{PostgresConfig: dstCfg},
	})

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_pgdump_noowner"),
		DestinationName: dstPeerName,
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      qualified,
			DestinationTableIdentifier: qualified,
		}},
		SourceName:        srcPeerName,
		MaxBatchSize:      100,
		DoInitialSnapshot: true,
		System:            protos.TypeSystem_PG,
		Env: map[string]string{
			"PEERDB_PG_AUTOMATED_SCHEMA_DUMP": "true",
		},
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	// initial load: pg_dump must succeed despite missing owner/grantee role,
	// then snapshot copies the 5 seed rows.
	EnvWaitFor(s.t, env, 3*time.Minute, "initial load owned_tbl", func() bool {
		var count int64
		err := dstConn.QueryRow(s.t.Context(),
			"SELECT COUNT(*) FROM "+qualified).Scan(&count)
		return err == nil && count == 5
	})

	// CDC: insert more rows on source and wait for them on dst
	for i := 6; i <= 10; i++ {
		_, err = srcConn.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s (val) VALUES ($1)", qualified),
			fmt.Sprintf("cdc_%d", i))
		EnvNoError(s.t, env, err)
	}

	EnvWaitFor(s.t, env, 3*time.Minute, "cdc owned_tbl", func() bool {
		var count int64
		err := dstConn.QueryRow(s.t.Context(),
			"SELECT COUNT(*) FROM "+qualified).Scan(&count)
		return err == nil && count == 10
	})

	// owner on dst should be the connecting user, not the (missing) source role
	var dstOwner string
	require.NoError(s.t, dstConn.QueryRow(s.t.Context(),
		"SELECT tableowner FROM pg_tables WHERE schemaname=$1 AND tablename=$2",
		srcSchema, tableName).Scan(&dstOwner))
	require.NotEqual(s.t, roleName, dstOwner, "destination table should not be owned by the source-only role")

	// no grants should reference the missing role on dst
	var grantCount int
	require.NoError(s.t, dstConn.QueryRow(s.t.Context(),
		"SELECT COUNT(*) FROM information_schema.table_privileges WHERE table_schema=$1 AND table_name=$2 AND grantee=$3",
		srcSchema, tableName, roleName).Scan(&grantCount))
	require.Zero(s.t, grantCount, "no privileges should be granted to the source-only role on destination")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
