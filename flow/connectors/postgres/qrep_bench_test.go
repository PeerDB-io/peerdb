package connpostgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func BenchmarkQRepQueryExecutor(b *testing.B) {
	connectionString := "postgres://postgres:postgres@localhost:7132/postgres"
	query := "SELECT * FROM bench.large_table"

	ctx := context.Background()

	// Create a separate connection for non-replication queries
	conn, err := pgx.Connect(ctx, connectionString)
	if err != nil {
		b.Fatalf("failed to create connection: %v", err)
	}
	defer conn.Close(context.Background())

	// Create a new QRepQueryExecutor instance
	qe := NewQRepQueryExecutor(conn, context.Background(), "test flow", "test part")

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
