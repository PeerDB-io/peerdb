package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

func BenchmarkQRepQueryExecutor(b *testing.B) {
	query := "SELECT * FROM bench.large_table"

	ctx := b.Context()
	connector, err := NewPostgresConnector(ctx, nil, internal.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(b, err, "error while creating connector")
	defer connector.Close()

	// Create a new QRepQueryExecutor instance
	qe, err := connector.NewQRepQueryExecutor(ctx, "test flow", "test part")
	require.NoError(b, err, "error while creating QRepQueryExecutor")

	// Run the benchmark
	for b.Loop() {
		// Execute the query and process the rows
		_, err := qe.ExecuteAndProcessQuery(ctx, query)
		require.NoError(b, err, "error while executing query")
	}
}
