package connpostgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/profile"
)

func BenchmarkQRepQueryExecutor(b *testing.B) {
	connectionString := "postgres://postgres:postgres@localhost:7132/postgres"
	query := "SELECT * FROM e2e_test.test_table"

	ctx := context.Background()

	// Create a separate connection pool for non-replication queries
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		b.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Create a new QRepQueryExecutor instance
	qe := NewQRepQueryExecutor(pool, context.Background())

	defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
