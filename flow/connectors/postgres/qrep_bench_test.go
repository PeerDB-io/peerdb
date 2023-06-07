package connpostgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func BenchmarkQRepQueryExecutor(b *testing.B) {
	connectionString := "postgres://postgres:postgres@localhost:7132/postgres"
	query := "SELECT * FROM bench.large_table LIMIT 1000000"

	ctx := context.Background()

	// Create a separate connection pool for non-replication queries
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		b.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Create a new QRepQueryExecutor instance
	qe := NewQRepQueryExecutor(pool, context.Background())

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// log the iteration
		b.Logf("iteration %d", i)

		// Execute the query and process the rows
		rows, err := qe.ExecuteQuery(query)
		if err != nil {
			b.Fatalf("failed to execute query: %v", err)
		}
		defer rows.Close()

		fieldDescriptions := rows.FieldDescriptions()
		_, err = qe.ProcessRows(rows, fieldDescriptions)
		if err != nil {
			b.Fatalf("failed to process rows: %v", err)
		}
	}
}
