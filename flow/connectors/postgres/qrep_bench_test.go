package connpostgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func BenchmarkQRepQueryExecutor(b *testing.B) {
	connectionString := "postgres://postgres:postgres@localhost:7132/postgres"
	query := "SELECT * FROM bench.large_table"

	ctx := context.Background()

	// Create a separate connection pool for non-replication queries
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		b.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Create a new QRepQueryExecutor instance
	qe := NewQRepQueryExecutor(pool, context.Background(), false)

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// log the iteration
		b.Logf("iteration %d", i)

		// Execute the query and process the rows
		_, err := qe.ExecuteAndProcessQuery(query)
		if err != nil {
			b.Fatalf("failed to execute query: %v", err)
		}
	}
}
