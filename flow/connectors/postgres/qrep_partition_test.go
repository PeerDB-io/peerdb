package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type testCase struct {
	name                  string
	config                *protos.QRepConfig
	last                  *protos.QRepPartition
	want                  []*protos.QRepPartition
	expectedNumPartitions int
	wantErr               bool
}

func newTestCaseForNumRows(schema string, name string, rows uint32, expectedNum int) *testCase {
	schemaQualifiedTable := schema + ".test"
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE "from" >= {{.start}} AND "from" < {{.end}}`,
		schemaQualifiedTable)
	return &testCase{
		name: name,
		config: &protos.QRepConfig{
			FlowJobName:         "test_flow_job",
			NumRowsPerPartition: rows,
			Query:               query,
			WatermarkTable:      schemaQualifiedTable,
			WatermarkColumn:     "from",
		},
		want:                  []*protos.QRepPartition{},
		expectedNumPartitions: expectedNum,
	}
}

func newTestCaseForCTID(schema string, name string, rows uint32, expectedNum int) *testCase {
	schemaQualifiedTable := schema + ".test"
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE "from" >= {{.start}} AND "from" < {{.end}}`,
		schemaQualifiedTable)
	return &testCase{
		name: name,
		config: &protos.QRepConfig{
			FlowJobName:         "test_flow_job",
			NumRowsPerPartition: rows,
			Query:               query,
			WatermarkTable:      schemaQualifiedTable,
			WatermarkColumn:     "ctid",
		},
		want:                  []*protos.QRepPartition{},
		expectedNumPartitions: expectedNum,
	}
}

func TestGetQRepPartitions(t *testing.T) {
	connStr := peerdbenv.GetCatalogConnectionStringFromEnv(context.Background())

	// Setup the DB
	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	tunnel, err := NewSSHTunnel(context.Background(), nil)
	if err != nil {
		t.Fatalf("Failed to create tunnel: %v", err)
	}
	defer tunnel.Close()

	conn, err := tunnel.NewPostgresConnFromConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close(context.Background())

	//nolint:gosec // Generate a random schema name, number has no cryptographic significance
	rndUint := rand.Uint64()
	schemaName := fmt.Sprintf("test_%d", rndUint)

	// Create the schema
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create the table in the new schema
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.test (
			id SERIAL PRIMARY KEY,
			value INT NOT NULL,
			"from" TIMESTAMP NOT NULL
		)
	`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// from 2010 Jan 1 10:00 AM UTC to 2010 Jan 30 10:00 AM UTC
	numRows := prepareTestData(t, conn, schemaName)

	// Define the test cases
	testCases := []*testCase{
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 1 partition if num_rows_per_partition is size of table",
			uint32(numRows),
			1,
		),
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 2 partitions if num_rows_per_partition is half the size of table",
			uint32(numRows)/2,
			2,
		),
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 3 partitions if num_rows_per_partition is 1/3 the size of table",
			uint32(numRows)/3,
			3,
		),
		// this is 5 partitions 30 rows and 7 rows per partition, would be 7, 7, 7, 7, 2
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 5 partitions if num_rows_per_partition is 1/4 the size of table",
			uint32(numRows)/4,
			5,
		),
		newTestCaseForCTID(
			schemaName,
			"ensure all rows are in 1 partition if num_rows_per_partition is size of table",
			uint32(numRows),
			1,
		),
		newTestCaseForCTID(
			schemaName,
			"ensure all rows are in 2 partitions if num_rows_per_partition is half the size of table",
			uint32(numRows)/2,
			2,
		),
		newTestCaseForCTID(
			schemaName,
			"ensure all rows are in 3 partitions if num_rows_per_partition is 1/3 the size of table",
			uint32(numRows)/3,
			3,
		),
		// this is 5 partitions 30 rows and 7 rows per partition, would be 7, 7, 7, 7, 2
		newTestCaseForCTID(
			schemaName,
			"ensure all rows are in 5 partitions if num_rows_per_partition is 1/4 the size of table",
			uint32(numRows)/4,
			5,
		),
	}

	// Run the test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &PostgresConnector{
				connStr: connStr,
				config:  &protos.PostgresConfig{},
				conn:    conn,
				logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testGetQRepPartitions"))),
			}

			got, err := c.GetQRepPartitions(context.Background(), tc.config, tc.last)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetQRepPartitions() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			// If the expected number of partitions is set, just check that
			// the number of partitions is equal to the expected number of
			// partitions, we don't care about the actual partition ranges
			// for now, but ideally we should check that the partition ranges
			// are correct as well.
			if tc.expectedNumPartitions != 0 {
				assert.Len(t, got, tc.expectedNumPartitions)
				return
			}

			expected := tc.want
			assert.Equal(t, len(expected), len(got))

			for i, val := range expected {
				er := val.Range.Range.(*protos.PartitionRange_TimestampRange).TimestampRange
				gotr := got[i].Range.Range.(*protos.PartitionRange_TimestampRange).TimestampRange
				assert.Equal(t, er.Start.AsTime(), gotr.Start.AsTime())
				assert.Equal(t, er.End.AsTime(), gotr.End.AsTime())
			}
		})
	}

	// Drop the schema at the end
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, schemaName))
	if err != nil {
		t.Fatalf("Failed to drop schema: %v", err)
	}
}

// returns the number of rows inserted
func prepareTestData(t *testing.T, pool *pgx.Conn, schema string) int {
	t.Helper()

	// Define the start and end times
	startTime := time.Date(2010, time.January, 1, 10, 0, 0, 0, time.UTC)
	endTime := time.Date(2010, time.January, 31, 10, 0, 0, 0, time.UTC)

	times := 0
	for tm := startTime; tm.Before(endTime); tm = tm.Add(24 * time.Hour) {
		times += 1
		_, err := pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s.test (value, "from") VALUES ($1, $2)
		`, schema), times, tm)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	return times
}
