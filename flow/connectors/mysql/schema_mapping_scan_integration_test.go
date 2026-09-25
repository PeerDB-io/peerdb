//go:build tilt

package connmysql

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
)

const (
	scanTestSchemas         = 10
	scanTestTablesPerSchema = 20
	scanTestTablesRequested = 5
	// A correct implementation opens no tables at all. A joined one opens every table on the
	// instance, so the budget only needs to sit below the 200 seeded here.
	scanTestOpenBudget = 100
)

// openedTables returns how many tables conn's session has opened so far.
func openedTables(t *testing.T, conn *client.Conn) int64 {
	t.Helper()
	// Session scope keeps other tests from skewing the count, so the caller reuses one
	// connection for both readings and the measured call; the connector's Execute may retry
	// onto a different one. Opened_table_definitions goes quiet once table_definition_cache
	// exceeds the table count.
	rs, err := conn.Execute("SHOW SESSION STATUS LIKE 'Opened_tables'")
	require.NoError(t, err)
	require.Len(t, rs.Values, 1)
	raw, err := rs.GetString(0, 1)
	require.NoError(t, err)
	n, err := strconv.ParseInt(raw, 10, 64)
	require.NoError(t, err)
	return n
}

// TestIntegrationSchemaMappingDoesNotScanInstance asserts that GetSchemaMapping's work is
// proportional to the tables requested, not to the tables on the server.
func TestIntegrationSchemaMappingDoesNotScanInstance(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "mysql"},
		{name: "mariadb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			connector := newTestConnector(t, ctx, tc.name)

			seedStart := time.Now()
			var target string
			for i := range scanTestSchemas {
				db := testDBName(fmt.Sprintf("scan%02d", i))
				createTestDB(t, ctx, connector, db)
				for j := range scanTestTablesPerSchema {
					_, err := connector.Execute(ctx, fmt.Sprintf(
						"CREATE TABLE `%s`.t%02d (id INT PRIMARY KEY, payload VARCHAR(32))", db, j))
					require.NoError(t, err)
				}
				if i == 0 {
					target = db
				}
			}
			seeded := scanTestSchemas * scanTestTablesPerSchema
			t.Logf("seeded %d tables across %d schemas in %s", seeded, scanTestSchemas,
				time.Since(seedStart).Round(time.Millisecond))

			requested := make([]string, 0, scanTestTablesRequested)
			for j := range scanTestTablesRequested {
				requested = append(requested, fmt.Sprintf("t%02d", j))
			}

			conn := connector.Conn()
			before := openedTables(t, conn)
			start := time.Now()
			got, err := mysql_validation.GetSchemaMapping(conn, target, requested)
			elapsed := time.Since(start)
			require.NoError(t, err)
			delta := openedTables(t, conn) - before

			require.Len(t, got.Columns, scanTestTablesRequested)
			require.Equal(t, []string{"id", "payload"},
				got.Columns[common.QualifiedTable{Namespace: target, Table: "t00"}])

			// Elapsed is logged but never asserted, since it varies with machine load and cache
			// warmth. On a 12,000 table instance requesting 50, this reads 50 definitions and
			// opens 0 tables, against 12,118 and 12,120 for the joined form.
			t.Logf("requested %d tables of %d seeded: Opened_tables +%d (budget %d), took %s",
				scanTestTablesRequested, seeded, delta, scanTestOpenBudget,
				elapsed.Round(time.Millisecond))

			require.Lessf(t, delta, int64(scanTestOpenBudget),
				"opened %d tables for %d requested: the instance is being scanned, which means a"+
					" metadata join was reintroduced", delta, scanTestTablesRequested)
		})
	}
}
