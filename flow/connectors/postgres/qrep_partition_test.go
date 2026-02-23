package connpostgres

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type testCase struct {
	name                  string
	config                *protos.QRepConfig
	last                  *protos.QRepPartition
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
			InitialCopyOnly:     true,
		},
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
			WatermarkColumn:     ctidColumnName,
		},
		expectedNumPartitions: expectedNum,
	}
}

func TestGetQRepPartitions(t *testing.T) {
	t.Parallel()
	connStr := internal.GetCatalogConnectionStringFromEnv(t.Context())

	// Setup the DB
	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	tunnel, err := utils.NewSSHTunnel(t.Context(), nil)
	if err != nil {
		t.Fatalf("Failed to create tunnel: %v", err)
	}
	defer tunnel.Close()

	conn, err := NewPostgresConnFromConfig(t.Context(), config, "", nil, tunnel)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close(t.Context())

	//nolint:gosec // Generate a random schema name, number has no cryptographic significance
	schemaName := fmt.Sprintf("test_%d", rand.Uint64())

	// Create the schema
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create the table in the new schema
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.test (
			id SERIAL PRIMARY KEY,
			value INT NOT NULL,
			"from" TIMESTAMP
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
		// NTILE(5) groups 12 nulls into bucket 1 along with some timestamps, producing 4 distinct timestamp ranges
		// + 1 explicit null partition = 5 total (4 timestamp + 1 null)
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 5 partitions if num_rows_per_partition is 1/4 the size of table",
			uint32(numRows)/4,
			4,
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
		// this is 5 partitions 33 rows and 7 rows per partition, would be 8, 8, 8, 8, 1
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
				Config:  &protos.PostgresConfig{},
				conn:    conn,
				logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testGetQRepPartitions"))),
			}

			got, err := c.GetQRepPartitions(t.Context(), tc.config, tc.last)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetQRepPartitions() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			if tc.expectedNumPartitions != 0 {
				expected := tc.expectedNumPartitions
				if tc.expectedNumPartitions > 1 && tc.config.WatermarkColumn != ctidColumnName {
					expected = expected + 1 // account for null partition when partitioning by non-ctid column
				}
				assert.Len(t, got, expected)
				return
			}
		})
	}

	// Drop the schema at the end
	if _, err := conn.Exec(t.Context(), fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, schemaName)); err != nil {
		t.Fatalf("Failed to drop schema: %v", err)
	}
}

// returns the number of rows inserted
func prepareTestData(t *testing.T, pool *pgx.Conn, schema string) int {
	t.Helper()

	// Define the start and end times
	startTime := time.Date(2010, time.January, 1, 10, 0, 0, 0, time.UTC)
	endTime := time.Date(2010, time.January, 31, 10, 0, 0, 0, time.UTC)

	rowsCount := 0
	for tm := startTime; tm.Before(endTime); tm = tm.Add(24 * time.Hour) {
		rowsCount += 1
		_, err := pool.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s.test (value, "from") VALUES ($1, $2)
		`, schema), rowsCount, tm)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// add some rows with null "from" value to ensure they get partitioned correctly as well
	for i := range 12 {
		rowsCount += 1
		_, err := pool.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s.test (value, "from") VALUES ($1, NULL)
		`, schema), rowsCount+i+1)
		if err != nil {
			t.Fatalf("Failed to insert test data with null from value: %v", err)
		}
	}

	return rowsCount
}
