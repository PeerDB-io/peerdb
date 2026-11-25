//go:build tilt

package connpostgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type resyncIndexDef struct {
	contype string
	def     string
}

func setupResyncIndexTest(t *testing.T) (*PostgresConnector, string) {
	t.Helper()
	connector, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	schema := "pgresyncidx_" + strings.ToLower(common.RandomString(8))
	_, err = connector.conn.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = connector.conn.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA %s CASCADE", schema))
		connector.Close()
	})
	return connector, schema
}

// getResyncIndexDefs returns index name -> (constraint type, definition with the table name normalized)
func getResyncIndexDefs(t *testing.T, connector *PostgresConnector, schema string, table string) map[string]resyncIndexDef {
	t.Helper()
	rows, err := connector.conn.Query(t.Context(), `
		SELECT ic.relname, coalesce(con.contype::text, ''), pg_get_indexdef(i.indexrelid)
		FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid AND con.conrelid = i.indrelid
		WHERE i.indrelid = ($1 || '.' || $2)::regclass`, schema, table)
	require.NoError(t, err)
	defer rows.Close()
	defs := make(map[string]resyncIndexDef)
	for rows.Next() {
		var name, contype, def string
		require.NoError(t, rows.Scan(&name, &contype, &def))
		def = strings.Replace(def, " INDEX "+name+" ON ", " INDEX ON ", 1)
		defs[name] = resyncIndexDef{contype: contype, def: strings.Replace(def, schema+"."+table+" ", "TABLE ", 1)}
	}
	require.NoError(t, rows.Err())
	return defs
}

func TestRenameTablesPreservesIndexes(t *testing.T) {
	// exclusion constraint on a scalar column, created once as concurrent CREATE EXTENSION can conflict
	connector, _ := setupResyncIndexTest(t)
	_, err := connector.conn.Exec(t.Context(), "CREATE EXTENSION IF NOT EXISTS btree_gist")
	require.NoError(t, err)

	for _, resyncHasPrimaryKey := range []bool{true, false} {
		t.Run(fmt.Sprintf("resync_pkey_%v", resyncHasPrimaryKey), func(t *testing.T) {
			t.Parallel()
			connector, schema := setupResyncIndexTest(t)

			_, err := connector.conn.Exec(t.Context(), fmt.Sprintf(`
				CREATE TABLE %[1]s.orders (
					id INT NOT NULL,
					customer_id INT,
					status TEXT,
					email TEXT,
					external_ref TEXT,
					meta JSONB,
					room INT,
					during INT4RANGE,
					CONSTRAINT orders_pk PRIMARY KEY (id),
					CONSTRAINT orders_external_ref_key UNIQUE (external_ref) DEFERRABLE INITIALLY DEFERRED,
					CONSTRAINT orders_no_overlap EXCLUDE USING gist (room WITH =, during WITH &&)
				);
				CREATE INDEX idx_orders_customer ON %[1]s.orders (customer_id);
				CREATE INDEX idx_orders_open ON %[1]s.orders (status) WHERE status <> 'closed';
				CREATE UNIQUE INDEX "Idx Orders Email" ON %[1]s.orders (lower(email));
				CREATE INDEX idx_orders_meta ON %[1]s.orders USING gin (meta);
				CREATE INDEX idx_orders_dropped ON %[1]s.orders (customer_id) WHERE false;
				INSERT INTO %[1]s.orders VALUES (1, 10, 'open', 'a@x', 'r1', '{}', 1, '[1,5)');
			`, schema))
			require.NoError(t, err)
			expected := getResyncIndexDefs(t, connector, schema, "orders")

			// _resync table as PeerDB creates it: columns and, unless replica identity full, a primary key
			pkey := ""
			if resyncHasPrimaryKey {
				pkey = ", PRIMARY KEY (id)"
			}
			_, err = connector.conn.Exec(t.Context(), fmt.Sprintf(`
				CREATE TABLE %[1]s.orders_resync (
					id INT NOT NULL, customer_id INT, status TEXT, email TEXT, external_ref TEXT,
					meta JSONB, room INT, during INT4RANGE%[2]s);
				INSERT INTO %[1]s.orders_resync VALUES (1, 10, 'open', 'a@x', 'r1', '{}', 1, '[1,5)'),
					(2, 20, 'closed', 'b@x', 'r2', '{}', 1, '[5,9)');
			`, schema, pkey))
			require.NoError(t, err)

			// an index that cannot be built on the _resync table must not fail the resync
			_, err = connector.conn.Exec(t.Context(), fmt.Sprintf(`
				ALTER TABLE %[1]s.orders ADD COLUMN dst_only INT;
				CREATE INDEX idx_orders_dst_only ON %[1]s.orders (dst_only);`, schema))
			require.NoError(t, err)

			// building twice must be idempotent, as the activity is retried
			_, err = connector.buildResyncIndexes(t.Context(),
				&common.QualifiedTable{Namespace: schema, Table: "orders_resync"},
				&common.QualifiedTable{Namespace: schema, Table: "orders"})
			require.NoError(t, err)

			// everything except an existing primary key is built before the swap transaction,
			// including the exclusion constraint, so the swap itself only renames
			// (with a _resync primary key, PeerDB's orders_resync_pkey stands in for the original one)
			prebuilt := getResyncIndexDefs(t, connector, schema, "orders_resync")
			require.Len(t, prebuilt, len(expected))
			var prebuiltExclusion int
			for name, def := range prebuilt {
				if def.contype == "x" {
					require.True(t, strings.HasPrefix(name, "_peerdb_resync_"), name)
					prebuiltExclusion++
				}
			}
			require.Equal(t, 1, prebuiltExclusion)

			_, err = connector.RenameTables(t.Context(), &protos.RenameTablesInput{
				FlowJobName: "test_resync_indexes",
				RenameTableOptions: []*protos.RenameTableOption{{
					CurrentName: schema + ".orders_resync",
					NewName:     schema + ".orders",
				}},
			}, nil)
			require.NoError(t, err)

			actual := getResyncIndexDefs(t, connector, schema, "orders")
			require.Equal(t, expected, actual)

			var count int
			require.NoError(t, connector.conn.QueryRow(t.Context(),
				fmt.Sprintf("SELECT count(*) FROM %s.orders", schema)).Scan(&count))
			require.Equal(t, 2, count)

			var leftovers int
			require.NoError(t, connector.conn.QueryRow(t.Context(),
				"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "+
					"WHERE n.nspname = $1 AND c.relname LIKE '\\_peerdb\\_resync\\_%'", schema).Scan(&leftovers))
			require.Zero(t, leftovers)
		})
	}
}

func TestRenameTablesPreservesIndexesWithSoftDelete(t *testing.T) {
	t.Parallel()
	connector, schema := setupResyncIndexTest(t)

	_, err := connector.conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %[1]s.items (id INT PRIMARY KEY, sku TEXT, "_PEERDB_IS_DELETED" BOOL DEFAULT FALSE);
		CREATE UNIQUE INDEX items_sku_key ON %[1]s.items (sku);
		CREATE INDEX items_live ON %[1]s.items (id) WHERE NOT "_PEERDB_IS_DELETED";
		INSERT INTO %[1]s.items VALUES (1, 'a', false), (2, 'b', false);
		CREATE TABLE %[1]s.items_resync (id INT PRIMARY KEY, sku TEXT, "_PEERDB_IS_DELETED" BOOL DEFAULT FALSE);
		INSERT INTO %[1]s.items_resync VALUES (1, 'a', false);
	`, schema))
	require.NoError(t, err)
	expected := getResyncIndexDefs(t, connector, schema, "items")

	resyncName := schema + ".items_resync"
	_, err = connector.RenameTables(t.Context(), &protos.RenameTablesInput{
		FlowJobName:       "test_resync_indexes_soft_delete",
		SoftDeleteColName: `_PEERDB_IS_DELETED`,
		RenameTableOptions: []*protos.RenameTableOption{{
			CurrentName: resyncName,
			NewName:     schema + ".items",
		}},
	}, map[string]*protos.TableSchema{resyncName: {
		PrimaryKeyColumns: []string{"id"},
		Columns:           []*protos.FieldDescription{{Name: "id"}, {Name: "sku"}},
	}})
	require.NoError(t, err)

	require.Equal(t, expected, getResyncIndexDefs(t, connector, schema, "items"))
	var deleted bool
	require.NoError(t, connector.conn.QueryRow(t.Context(),
		fmt.Sprintf(`SELECT "_PEERDB_IS_DELETED" FROM %s.items WHERE id = 2`, schema)).Scan(&deleted))
	require.True(t, deleted)
}

// A soft-deleted row carried over from the original table can conflict with a unique index, e.g. when
// its value was reused at the source after the delete. The resync must still complete: the
// conflicting index is skipped, every other index is preserved.
func TestRenameTablesSoftDeleteUniqueConflict(t *testing.T) {
	t.Parallel()
	connector, schema := setupResyncIndexTest(t)

	_, err := connector.conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %[1]s.items (id INT PRIMARY KEY, sku TEXT, "_PEERDB_IS_DELETED" BOOL DEFAULT FALSE);
		CREATE UNIQUE INDEX items_sku_key ON %[1]s.items (sku);
		CREATE INDEX items_live ON %[1]s.items (id) WHERE NOT "_PEERDB_IS_DELETED";
		INSERT INTO %[1]s.items VALUES (1, 'a', false), (2, 'b', false);
		CREATE TABLE %[1]s.items_resync (id INT PRIMARY KEY, sku TEXT, "_PEERDB_IS_DELETED" BOOL DEFAULT FALSE);
		INSERT INTO %[1]s.items_resync VALUES (1, 'a', false), (3, 'b', false);
	`, schema))
	require.NoError(t, err)
	expected := getResyncIndexDefs(t, connector, schema, "items")
	delete(expected, "items_sku_key")

	resyncName := schema + ".items_resync"
	_, err = connector.RenameTables(t.Context(), &protos.RenameTablesInput{
		FlowJobName:       "test_resync_indexes_soft_delete_conflict",
		SoftDeleteColName: `_PEERDB_IS_DELETED`,
		RenameTableOptions: []*protos.RenameTableOption{{
			CurrentName: resyncName,
			NewName:     schema + ".items",
		}},
	}, map[string]*protos.TableSchema{resyncName: {
		PrimaryKeyColumns: []string{"id"},
		Columns:           []*protos.FieldDescription{{Name: "id"}, {Name: "sku"}},
	}})
	require.NoError(t, err)

	require.Equal(t, expected, getResyncIndexDefs(t, connector, schema, "items"))
	var deletedRows int
	require.NoError(t, connector.conn.QueryRow(t.Context(),
		fmt.Sprintf(`SELECT count(*) FROM %s.items WHERE id = 2 AND "_PEERDB_IS_DELETED"`, schema)).Scan(&deletedRows))
	require.Equal(t, 1, deletedRows)
}

// The soft-delete transfer commits before the swap. With replica identity full every column is
// compared and NULLs never match, so a retry after a failed swap must not transfer the rows again.
func TestRenameTablesSoftDeleteTransferRetry(t *testing.T) {
	t.Parallel()
	connector, schema := setupResyncIndexTest(t)

	_, err := connector.conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %[1]s.items (id INT, code TEXT, note TEXT, "_PEERDB_IS_DELETED" BOOL DEFAULT FALSE);
		CREATE UNIQUE INDEX items_code_key ON %[1]s.items (code);
		INSERT INTO %[1]s.items VALUES (1, 'a', 'x', false), (2, 'b', NULL, false);
		CREATE TABLE %[1]s.items_resync (id INT, code TEXT, note TEXT, "_PEERDB_IS_DELETED" BOOL DEFAULT FALSE);
		INSERT INTO %[1]s.items_resync VALUES (1, 'a', 'x', false);
		CREATE TABLE %[1]s.other_resync (id INT);
		CREATE VIEW %[1]s.other AS SELECT 1 AS id;
	`, schema))
	require.NoError(t, err)
	expected := getResyncIndexDefs(t, connector, schema, "items")

	itemsName := schema + ".items_resync"
	itemsOption := &protos.RenameTableOption{CurrentName: itemsName, NewName: schema + ".items"}
	schemas := map[string]*protos.TableSchema{
		itemsName: {
			PrimaryKeyColumns:     []string{"id", "code", "note"},
			IsReplicaIdentityFull: true,
			Columns:               []*protos.FieldDescription{{Name: "id"}, {Name: "code"}, {Name: "note"}},
		},
		schema + ".other_resync": {Columns: []*protos.FieldDescription{{Name: "id"}}},
	}

	// first attempt: the soft-delete transfer and index builds commit, then the swap fails
	_, err = connector.RenameTables(t.Context(), &protos.RenameTablesInput{
		FlowJobName:       "test_resync_soft_delete_retry",
		SoftDeleteColName: `_PEERDB_IS_DELETED`,
		RenameTableOptions: []*protos.RenameTableOption{
			itemsOption,
			{CurrentName: schema + ".other_resync", NewName: schema + ".other"},
		},
	}, schemas)
	require.Error(t, err)

	// retry
	_, err = connector.RenameTables(t.Context(), &protos.RenameTablesInput{
		FlowJobName:        "test_resync_soft_delete_retry",
		SoftDeleteColName:  `_PEERDB_IS_DELETED`,
		RenameTableOptions: []*protos.RenameTableOption{itemsOption},
	}, schemas)
	require.NoError(t, err)

	require.Equal(t, expected, getResyncIndexDefs(t, connector, schema, "items"))
	var deletedRows int
	require.NoError(t, connector.conn.QueryRow(t.Context(),
		fmt.Sprintf(`SELECT count(*) FROM %s.items WHERE id = 2 AND "_PEERDB_IS_DELETED"`, schema)).Scan(&deletedRows))
	require.Equal(t, 1, deletedRows)
}

func TestRenameTablesPrimaryKeyCases(t *testing.T) {
	for _, tc := range []struct {
		name string
		// DDL of the original and _resync tables, and rows of the _resync table
		original, resync string
		// expected indexes after the swap, keyed by name
		expected map[string]resyncIndexDef
	}{
		{
			// a deferrable primary key keeps its name, deferrability cannot be restored on an existing one
			name:     "deferrable",
			original: "id INT, CONSTRAINT items_pk PRIMARY KEY (id) DEFERRABLE",
			resync:   "id INT PRIMARY KEY); INSERT INTO %[1]s.items_resync VALUES (1",
			expected: map[string]resyncIndexDef{
				"items_pk": {contype: "p", def: "CREATE UNIQUE INDEX ON TABLE USING btree (id)"},
			},
		},
		{
			// the primary key cannot be attached because of NULLs, the index is kept under its name
			name:     "attach_fails",
			original: "id INT, CONSTRAINT items_pk PRIMARY KEY (id)",
			resync:   "id INT); INSERT INTO %[1]s.items_resync VALUES (NULL",
			expected: map[string]resyncIndexDef{
				"items_pk": {contype: "", def: "CREATE UNIQUE INDEX ON TABLE USING btree (id)"},
			},
		},
		{
			// PeerDB created a different primary key, the original one is kept as a unique index
			name:     "different",
			original: "id INT, code TEXT, CONSTRAINT items_pk PRIMARY KEY (id, code)",
			resync:   "id INT PRIMARY KEY, code TEXT); INSERT INTO %[1]s.items_resync VALUES (1, 'a'",
			expected: map[string]resyncIndexDef{
				"items_resync_pkey": {contype: "p", def: "CREATE UNIQUE INDEX ON TABLE USING btree (id)"},
				"items_pk":          {contype: "", def: "CREATE UNIQUE INDEX ON TABLE USING btree (id, code)"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			connector, schema := setupResyncIndexTest(t)
			_, err := connector.conn.Exec(t.Context(), fmt.Sprintf(
				"CREATE TABLE %[1]s.items ("+tc.original+"); CREATE TABLE %[1]s.items_resync ("+tc.resync+")", schema))
			require.NoError(t, err)

			_, err = connector.RenameTables(t.Context(), &protos.RenameTablesInput{
				FlowJobName: "test_resync_pkey_" + tc.name,
				RenameTableOptions: []*protos.RenameTableOption{{
					CurrentName: schema + ".items_resync",
					NewName:     schema + ".items",
				}},
			}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, getResyncIndexDefs(t, connector, schema, "items"))
		})
	}
}
