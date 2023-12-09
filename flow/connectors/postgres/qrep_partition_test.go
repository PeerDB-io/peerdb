package connpostgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	util "github.com/PeerDB-io/peer-flow/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
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
	schemaQualifiedTable := fmt.Sprintf("%s.test", schema)
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
	schemaQualifiedTable := fmt.Sprintf("%s.test", schema)
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
	// log.SetLevel(log.DebugLevel)

	const connStr = "postgres://postgres:postgres@localhost:7132/postgres"

	// Setup the DB
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("unable to connect to database: %v", err)
	}

	// Generate a random schema name
	rndUint, err := util.RandomUInt64()
	if err != nil {
		t.Fatalf("Failed to generate random uint: %v", err)
	}
	schemaName := fmt.Sprintf("test_%d", rndUint)

	// Create the schema
	_, err = pool.Exec(context.Background(), fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create the table in the new schema
	_, err = pool.Exec(context.Background(), fmt.Sprintf(`
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
	numRows := prepareTestData(t, pool, schemaName)

	secondsInADay := uint32(24 * time.Hour / time.Second)
	fmt.Printf("secondsInADay: %d\n", secondsInADay)

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
				ctx:     context.Background(),
				config:  &protos.PostgresConfig{},
				pool:    pool,
			}

			got, err := c.GetQRepPartitions(tc.config, tc.last)
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
				assert.Equal(t, tc.expectedNumPartitions, len(got))
				return
			}

			expected := tc.want
			assert.Equal(t, len(expected), len(got))

			for i := 0; i < len(expected); i++ {
				er := expected[i].Range.Range.(*protos.PartitionRange_TimestampRange).TimestampRange
				gotr := got[i].Range.Range.(*protos.PartitionRange_TimestampRange).TimestampRange
				assert.Equal(t, er.Start.AsTime(), gotr.Start.AsTime())
				assert.Equal(t, er.End.AsTime(), gotr.End.AsTime())
			}
		})
	}

	// Drop the schema at the end
	_, err = pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, schemaName))
	if err != nil {
		t.Fatalf("Failed to drop schema: %v", err)
	}
}

// returns the number of rows inserted
func prepareTestData(test *testing.T, pool *pgxpool.Pool, schema string) int {
	// Define the start and end times
	startTime := time.Date(2010, time.January, 1, 10, 0, 0, 0, time.UTC)
	endTime := time.Date(2010, time.January, 30, 10, 0, 0, 0, time.UTC)

	// Prepare the time range
	var times []time.Time
	for t := startTime; !t.After(endTime); t = t.Add(24 * time.Hour) {
		times = append(times, t)
	}

	// Insert the test data
	for i, t := range times {
		_, err := pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s.test (value, "from") VALUES ($1, $2)
		`, schema), i+1, t)
		if err != nil {
			test.Fatalf("Failed to insert test data: %v", err)
		}
	}

	return len(times)
}
