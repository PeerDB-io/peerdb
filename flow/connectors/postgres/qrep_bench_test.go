package connpostgres

import (
	"context"
	"testing"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func BenchmarkQRepQueryExecutor(b *testing.B) {
	query := "SELECT * FROM bench.large_table"

	ctx := context.Background()
	connector, err := NewPostgresConnector(ctx,
		&protos.PostgresConfig{
			Host:     "localhost",
			Port:     7132,
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
		})
	if err != nil {
		b.Fatalf("failed to create connection: %v", err)
	}
	defer connector.Close()

	// Create a new QRepQueryExecutor instance
	qe := connector.NewQRepQueryExecutor("test flow", "test part")

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// log the iteration
		b.Logf("iteration %d", i)

		// Execute the query and process the rows
		_, err := qe.ExecuteAndProcessQuery(ctx, query)
		if err != nil {
			b.Fatalf("failed to execute query: %v", err)
		}
	}
}
