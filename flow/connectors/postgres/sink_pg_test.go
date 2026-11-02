//go:build tilt

package connpostgres

import (
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TestExecuteQueryWithTxReturnsProbeError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector, schemaName := setupDB(t, "probe_error")
	defer connector.Close()
	defer teardownDB(t, connector.conn, schemaName)

	table := common.QuoteIdentifier(schemaName) + ".t"
	_, err := connector.conn.Exec(ctx, "CREATE TABLE "+table+"(id int PRIMARY KEY)")
	require.NoError(t, err)

	qe, err := connector.NewQRepQueryExecutor(ctx, nil, shared.InternalVersion_Latest, "test flow", "test part")
	require.NoError(t, err)

	_, writer := NewPgCopyPipe()
	_, _, err = qe.ExecuteQueryIntoSink(ctx, writer, "SELECT id FROM "+table+" WHERE id = 1 / 0")
	require.ErrorContains(t, err, "division by zero")
}

func TestExecuteQueryWithTxSnapshotColumns(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector, schemaName := setupDB(t, "snapshot_columns")
	defer connector.Close()
	defer teardownDB(t, connector.conn, schemaName)

	table := common.QuoteIdentifier(schemaName) + ".t"
	for _, stmt := range []string{
		"CREATE TABLE " + table + "(id int PRIMARY KEY, v text)",
		"INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')",
	} {
		_, err := connector.conn.Exec(ctx, stmt)
		require.NoError(t, err)
	}

	exporter, err := NewPostgresConnector(ctx, nil, internal.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(t, err)
	defer exporter.Close()
	tx, err := exporter.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback(ctx)) }()
	var snapshot string
	require.NoError(t, tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot))

	for _, stmt := range []string{
		"ALTER TABLE " + table + " ADD COLUMN extra int",
		"ALTER TABLE " + table + " RENAME COLUMN v TO w",
		"INSERT INTO " + table + " VALUES (3, 'c', 3)",
	} {
		_, err := connector.conn.Exec(ctx, stmt)
		require.NoError(t, err)
	}

	qe, err := connector.NewQRepQueryExecutorSnapshot(
		ctx, nil, shared.InternalVersion_Latest, snapshot, "test flow", "test part", nil, nil)
	require.NoError(t, err)

	reader, writer := NewPgCopyPipe()
	var data []byte
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		data, _ = io.ReadAll(reader)
	}()
	_, _, err = qe.ExecuteQueryIntoSink(ctx, writer, "SELECT * FROM "+table)
	writer.Close(err)
	<-drained
	require.NoError(t, err)

	cols, err := reader.GetColumnNames()
	require.NoError(t, err)
	require.Equal(t, []string{"id", "v"}, cols)
	require.Equal(t, "1\ta\n2\tb\n", string(data))
}
