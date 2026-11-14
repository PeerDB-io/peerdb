//go:build tilt

package connpostgres

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestCheckSourceTablesChildTables(t *testing.T) {
	schema, conn, _ := setupTestSchema(t)
	role := "childgrants_" + strings.ToLower(common.RandomString(6))
	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE ROLE %[1]s WITH LOGIN PASSWORD 'childgrants';
		GRANT USAGE ON SCHEMA %[2]s TO %[1]s;`, role, schema))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf("DROP OWNED BY %[1]s; DROP ROLE %[1]s", role))
	})

	cfg := proto.CloneOf(internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	cfg.User = role
	cfg.Password = "childgrants"
	connector, err := NewPostgresConnector(t.Context(), nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { connector.Close() })

	setup := func(t *testing.T, setupSQL string) {
		t.Helper()
		_, err := conn.Exec(t.Context(), strings.ReplaceAll(setupSQL, "{schema}", schema))
		require.NoError(t, err)
	}
	snapshotMapping := func(table string) *protos.TableMapping {
		return &protos.TableMapping{SourceTableIdentifier: schema + "." + table}
	}
	ctidSnapshot := func(table string) map[string]struct{} {
		return map[string]struct{}{schema + "." + table: {}}
	}

	t.Run("readable", func(t *testing.T) {
		setup(t, `
			CREATE TABLE {schema}.readable (id INT NOT NULL, created_at DATE NOT NULL) PARTITION BY RANGE (created_at);
			CREATE TABLE {schema}.readable_2024 PARTITION OF {schema}.readable
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01') PARTITION BY RANGE (created_at);
			CREATE TABLE {schema}.readable_2024h1 PARTITION OF {schema}.readable_2024
				FOR VALUES FROM ('2024-01-01') TO ('2024-07-01');
			GRANT SELECT ON {schema}.readable, {schema}.readable_2024h1 TO `+role)

		require.NoError(t, connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "readable"}},
			[]*protos.TableMapping{snapshotMapping("readable")}, "", true, ctidSnapshot("readable")))
	})

	t.Run("unreadable partition", func(t *testing.T) {
		setup(t, `
			CREATE TABLE {schema}.unreadable (id INT NOT NULL, created_at DATE NOT NULL) PARTITION BY RANGE (created_at);
			CREATE TABLE {schema}.unreadable_2023 PARTITION OF {schema}.unreadable
				FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
			CREATE TABLE {schema}.unreadable_2024 PARTITION OF {schema}.unreadable
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
			GRANT SELECT ON {schema}.unreadable, {schema}.unreadable_2023 TO `+role)

		err := connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "unreadable"}},
			[]*protos.TableMapping{snapshotMapping("unreadable")}, "", true, ctidSnapshot("unreadable"))
		require.ErrorContains(t, err, fmt.Sprintf("%[1]s.unreadable (%[1]s.unreadable_2024)", schema))
	})

	t.Run("child in a schema without usage", func(t *testing.T) {
		setup(t, `
			CREATE SCHEMA {schema}_other;
			CREATE TABLE {schema}.other_parent (id INT NOT NULL);
			CREATE TABLE {schema}_other.other_child () INHERITS ({schema}.other_parent);
			GRANT SELECT ON {schema}.other_parent, {schema}_other.other_child TO `+role)
		t.Cleanup(func() {
			_, _ = conn.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA %s_other CASCADE", schema))
		})

		err := connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "other_parent"}},
			[]*protos.TableMapping{snapshotMapping("other_parent")}, "", true, ctidSnapshot("other_parent"))
		require.ErrorContains(t, err, schema+"_other.other_child")
	})

	t.Run("column grants cover selected columns only", func(t *testing.T) {
		setup(t, `
			CREATE TABLE {schema}.columns (id INT NOT NULL, secret TEXT);
			CREATE TABLE {schema}.columns_archive () INHERITS ({schema}.columns);
			GRANT SELECT ON {schema}.columns TO `+role+`;
			GRANT SELECT (id) ON {schema}.columns_archive TO `+role)

		mapping := snapshotMapping("columns")
		mapping.Exclude = []string{"secret"}
		require.NoError(t, connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "columns"}},
			[]*protos.TableMapping{mapping}, "", true, ctidSnapshot("columns")))

		err := connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "columns"}},
			[]*protos.TableMapping{snapshotMapping("columns")}, "", true, ctidSnapshot("columns"))
		require.ErrorContains(t, err, schema+".columns_archive")
	})

	t.Run("only tables in the ctid snapshot set are checked", func(t *testing.T) {
		setup(t, `
			CREATE TABLE {schema}.skipped (id INT NOT NULL);
			CREATE TABLE {schema}.skipped_archive () INHERITS ({schema}.skipped);
			GRANT SELECT ON {schema}.skipped TO `+role)

		require.NoError(t, connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "skipped"}},
			[]*protos.TableMapping{snapshotMapping("skipped")}, "", true, nil))
	})

	t.Run("ctid snapshot tables", func(t *testing.T) {
		customKey := snapshotMapping("custom_key")
		customKey.PartitionKey = "id"
		mappings := []*protos.TableMapping{snapshotMapping("default_key"), customKey}
		ctidOff := map[string]string{"PEERDB_POSTGRES_APPLY_CTID_BLOCK_PARTITIONING_OVERRIDE": "false"}
		for name, tc := range map[string]struct {
			cfg      *protos.FlowConnectionConfigsCore
			expected map[string]struct{}
		}{
			"no snapshot": {
				cfg: &protos.FlowConnectionConfigsCore{TableMappings: mappings},
			},
			"default key only": {
				cfg:      &protos.FlowConnectionConfigsCore{DoInitialSnapshot: true, TableMappings: mappings},
				expected: ctidSnapshot("default_key"),
			},
			"single partition": {
				cfg: &protos.FlowConnectionConfigsCore{
					DoInitialSnapshot: true, TableMappings: mappings, SnapshotNumPartitionsOverride: 1,
				},
				expected: map[string]struct{}{},
			},
			"ctid block partitioning is off": {
				cfg:      &protos.FlowConnectionConfigsCore{DoInitialSnapshot: true, TableMappings: mappings, Env: ctidOff},
				expected: map[string]struct{}{},
			},
			"partition override forces ctid block partitioning": {
				cfg: &protos.FlowConnectionConfigsCore{
					DoInitialSnapshot: true, TableMappings: mappings, Env: ctidOff, SnapshotNumPartitionsOverride: 4,
				},
				expected: ctidSnapshot("default_key"),
			},
		} {
			tables, err := connector.ctidSnapshotTables(t.Context(), tc.cfg)
			require.NoError(t, err, name)
			require.Equal(t, tc.expected, tables, name)
		}
	})
}

func TestCtidSnapshotTablesSkipsCompressedHypertables(t *testing.T) {
	ctr, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "timescale/timescaledb:2.31.0-pg17",
			// NO_TS_TUNE keeps timescaledb-tune from sizing memory to the host
			Env: map[string]string{"POSTGRES_PASSWORD": "postgres", "NO_TS_TUNE": "true"},
			Cmd: []string{
				"postgres",
				"-c", "shared_buffers=16MB", "-c", "max_connections=10",
				"-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off",
				"-c", "timescaledb.max_background_workers=1", "-c", "timescaledb.telemetry_level=off",
			},
			ExposedPorts: []string{"5432/tcp"},
			// TCP only accepts connections once the post-initdb server is up
			WaitingFor: wait.ForSQL("5432/tcp", "pgx", func(host string, port network.Port) string {
				return "postgres://postgres:postgres@" + net.JoinHostPort(host, port.Port()) + "/postgres"
			}).WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, ctr, testcontainers.StopTimeout(30*time.Second))
	require.NoError(t, err)
	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	mappedPort, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)
	port, err := strconv.ParseUint(mappedPort.Port(), 10, 32)
	require.NoError(t, err)

	connector, err := NewPostgresConnector(t.Context(), nil, &protos.PostgresConfig{
		Host: host, Port: uint32(port), User: "postgres", Password: "postgres", Database: "postgres",
	})
	require.NoError(t, err)
	t.Cleanup(func() { connector.Close() })

	_, err = connector.conn.Exec(t.Context(), `
		CREATE EXTENSION IF NOT EXISTS timescaledb;
		CREATE TABLE public.compressed (time TIMESTAMPTZ NOT NULL, value INT);
		SELECT create_hypertable('public.compressed', 'time');
		INSERT INTO public.compressed SELECT now() - make_interval(days => i), i FROM generate_series(1, 30) i;
		ALTER TABLE public.compressed SET (timescaledb.compress);
		SELECT compress_chunk(c) FROM show_chunks('public.compressed') c;
		CREATE TABLE public.uncompressed (time TIMESTAMPTZ NOT NULL, value INT);
		SELECT create_hypertable('public.uncompressed', 'time');
		INSERT INTO public.uncompressed SELECT now() - make_interval(days => i), i FROM generate_series(1, 30) i;`)
	require.NoError(t, err)

	tables, err := connector.ctidSnapshotTables(t.Context(), &protos.FlowConnectionConfigsCore{
		DoInitialSnapshot: true,
		TableMappings: []*protos.TableMapping{
			{SourceTableIdentifier: "public.compressed"},
			{SourceTableIdentifier: "public.uncompressed"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"public.uncompressed": {}}, tables)
}
