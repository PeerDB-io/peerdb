//go:build tilt

package connpostgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
			[]*protos.TableMapping{snapshotMapping("readable")}, "", true, true))
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
			[]*protos.TableMapping{snapshotMapping("unreadable")}, "", true, true)
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
			[]*protos.TableMapping{snapshotMapping("other_parent")}, "", true, true)
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
			[]*protos.TableMapping{mapping}, "", true, true))

		err := connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "columns"}},
			[]*protos.TableMapping{snapshotMapping("columns")}, "", true, true)
		require.ErrorContains(t, err, schema+".columns_archive")
	})

	t.Run("skipped for custom partition key and without snapshot", func(t *testing.T) {
		setup(t, `
			CREATE TABLE {schema}.skipped (id INT NOT NULL);
			CREATE TABLE {schema}.skipped_archive () INHERITS ({schema}.skipped);
			GRANT SELECT ON {schema}.skipped TO `+role)

		mapping := snapshotMapping("skipped")
		mapping.PartitionKey = "id"
		require.NoError(t, connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "skipped"}},
			[]*protos.TableMapping{mapping}, "", true, true))
		require.NoError(t, connector.CheckSourceTables(t.Context(),
			[]*common.QualifiedTable{{Namespace: schema, Table: "skipped"}},
			[]*protos.TableMapping{snapshotMapping("skipped")}, "", true, false))
	})
}
