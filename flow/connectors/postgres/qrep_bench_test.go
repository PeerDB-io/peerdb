package connpostgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

func BenchmarkQRepQueryExecutor(b *testing.B) {
	query := "SELECT * FROM bench.large_table"

	ctx := context.Background()
	connector, err := NewPostgresConnector(ctx, nil, peerdbenv.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(b, err, "error while creating connector")
	defer connector.Close()

	// Create a new QRepQueryExecutor instance
	qe, err := connector.NewQRepQueryExecutor(ctx, "test flow", "test part")
	require.NoError(b, err, "error while creating QRepQueryExecutor")

	// Run the benchmark
	b.ResetTimer()
	for i := range b.N {
		// log the iteration
		b.Logf("iteration %d", i)

		// Execute the query and process the rows
		_, err := qe.ExecuteAndProcessQuery(ctx, query)
		require.NoError(b, err, "error while executing query")
	}
}
