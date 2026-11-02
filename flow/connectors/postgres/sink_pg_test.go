//go:build tilt

package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

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
