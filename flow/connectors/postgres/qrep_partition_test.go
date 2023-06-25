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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testCase struct {
	name    string
	config  *protos.QRepConfig
	last    *protos.QRepPartition
	want    []*protos.QRepPartition
	wantErr bool
}

func newTestCase(schema string, name string, duration uint32, wantErr bool) *testCase {
	schemaQualifiedTable := fmt.Sprintf("%s.test", schema)
	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE timestamp >= {{.start}} AND timestamp < {{.end}}",
		schemaQualifiedTable)
	return &testCase{
		name: name,
		config: &protos.QRepConfig{
			FlowJobName:          "test_flow_job",
			BatchDurationSeconds: duration,
			Query:                query,
			WatermarkTable:       schemaQualifiedTable,
			WatermarkColumn:      "timestamp",
		},
		want:    []*protos.QRepPartition{},
		wantErr: wantErr,
	}
}

func (tc *testCase) appendPartition(start time.Time, end time.Time) *testCase {
	tsRange := &protos.PartitionRange_TimestampRange{
		TimestampRange: &protos.TimestampPartitionRange{
			Start: timestamppb.New(start),
			End:   timestamppb.New(end),
		},
	}
	tc.want = append(tc.want, &protos.QRepPartition{
		PartitionId: "test_uuid",
		Range: &protos.PartitionRange{
			Range: tsRange,
		},
	})
	return tc
}

func (tc *testCase) appendPartitions(start, end time.Time, numPartitions int) *testCase {
	duration := end.Sub(start)
	partitionDuration := duration / time.Duration(numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitionStart := start.Add(time.Duration(i) * partitionDuration)
		partitionEnd := start.Add(time.Duration(i+1) * partitionDuration)
		tc.appendPartition(partitionStart, partitionEnd)
	}
	return tc
}

func TestGetQRepPartitions(t *testing.T) {
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
			timestamp TIMESTAMP NOT NULL
		)
	`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// from 2010 Jan 1 10:00 AM UTC to 2010 Jan 30 10:00 AM UTC
	prepareTestData(t, pool, schemaName)

	secondsInADay := uint32(24 * time.Hour / time.Second)
	fmt.Printf("secondsInADay: %d\n", secondsInADay)

	// Define the test cases
	testCases := []*testCase{
		newTestCase(
			schemaName,
			"ensure all days are in 1 partition",
			secondsInADay*100,
			false,
		).appendPartition(
			time.Date(2010, time.January, 1, 10, 0, 0, 0, time.UTC),
			time.Date(2010, time.January, 30, 10, 0, 0, 0, time.UTC),
		),
		newTestCase(
			schemaName,
			"ensure all days are in 30 partitions",
			secondsInADay,
			false,
		).appendPartitions(
			time.Date(2010, time.January, 1, 10, 0, 0, 0, time.UTC),
			time.Date(2010, time.January, 30, 10, 0, 0, 0, time.UTC),
			29,
		),
		newTestCase(
			schemaName,
			"ensure all days are in 60 partitions",
			secondsInADay/2,
			false,
		).appendPartitions(
			time.Date(2010, time.January, 1, 10, 0, 0, 0, time.UTC),
			time.Date(2010, time.January, 30, 10, 0, 0, 0, time.UTC),
			58,
		),
		newTestCase(
			schemaName,
			"test for error condition with batch size 0",
			0,
			true,
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

func prepareTestData(test *testing.T, pool *pgxpool.Pool, schema string) {
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
			INSERT INTO %s.test (value, timestamp) VALUES ($1, $2)
		`, schema), i+1, t)
		if err != nil {
			test.Fatalf("Failed to insert test data: %v", err)
		}
	}
}
