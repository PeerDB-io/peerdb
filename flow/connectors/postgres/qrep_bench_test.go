package connpostgres

import (
	"context"
	"testing"

	"github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
)

func BenchmarkQRepQueryExecutor(b *testing.B) {
	query := "SELECT * FROM bench.large_table"

	ctx := context.Background()
	connector, err := NewPostgresConnector(ctx, utils.GetCatalogPostgresConfigFromEnv())
	if err != nil {
		b.Fatalf("failed to create connection: %v", err)
	}
	defer connector.Close()

	// Create a new QRepQueryExecutor instance
	qe := connector.NewQRepQueryExecutor("test flow", "test part")

	// Run the benchmark
	b.ResetTimer()
	for i := range b.N {
		// log the iteration
		b.Logf("iteration %d", i)

		// Execute the query and process the rows
		_, err := qe.ExecuteAndProcessQuery(ctx, query)
		if err != nil {
			b.Fatalf("failed to execute query: %v", err)
		}
	}
}
