package connpostgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

func setupDB(t *testing.T) (*PostgresConnector, string) {
	t.Helper()

	connector, err := NewPostgresConnector(context.Background(),
		nil, peerdbenv.GetCatalogPostgresConfigFromEnv(context.Background()))
	require.NoError(t, err, "error while creating connector")

	// Create unique schema name using current time
	schemaName := fmt.Sprintf("schema_%d", time.Now().Unix())

	// Create the schema
	_, err = connector.conn.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA %s;", schemaName))
	require.NoError(t, err, "error while creating schema")

	return connector, schemaName
}

func teardownDB(t *testing.T, conn *pgx.Conn, schemaName string) {
	t.Helper()

	_, err := conn.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE;", schemaName))
	require.NoError(t, err, "error while dropping schema")
}

func TestExecuteAndProcessQuery(t *testing.T) {
	ctx := context.Background()
	connector, schemaName := setupDB(t)
	conn := connector.conn
	defer connector.Close()
	defer teardownDB(t, conn, schemaName)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.test(id SERIAL PRIMARY KEY, data TEXT);", schemaName)
	_, err := conn.Exec(ctx, query)
	require.NoError(t, err, "error while creating table")

	query = fmt.Sprintf("INSERT INTO %s.test(data) VALUES('testdata');", schemaName)
	_, err = conn.Exec(ctx, query)
	require.NoError(t, err, "error while inserting data")

	qe, err := connector.NewQRepQueryExecutor(ctx, "test flow", "test part")
	require.NoError(t, err, "error while creating QRepQueryExecutor")

	query = fmt.Sprintf("SELECT * FROM %s.test;", schemaName)
	batch, err := qe.ExecuteAndProcessQuery(context.Background(), query)
	require.NoError(t, err, "error while executing query")
	require.Len(t, batch.Records, 1, "expected 1 record")
	require.Equal(t, "testdata", batch.Records[0][1].Value(), "expected 'testdata'")
}

func TestAllDataTypes(t *testing.T) {
	ctx := context.Background()
	connector, schemaName := setupDB(t)
	conn := connector.conn
	defer conn.Close(ctx)
	defer teardownDB(t, conn, schemaName)

	// Create a table that contains every data type we want to test
	query := fmt.Sprintf(`
	CREATE TABLE %s.test(
		col_bool BOOLEAN,
		col_int4 INTEGER,
		col_int8 BIGINT,
		col_float4 REAL,
		col_float8 DOUBLE PRECISION,
		col_text TEXT,
		col_bytea BYTEA,
		col_json JSON,
		col_uuid UUID,
		col_timestamp TIMESTAMP,
		col_numeric NUMERIC,
		col_tz TIMESTAMP WITH TIME ZONE,
		col_tz2 TIME WITH TIME ZONE,
		col_tz3 TIME WITHOUT TIME ZONE,
		col_tz4 TIMESTAMP WITHOUT TIME ZONE,
		col_date DATE
	);`, schemaName)

	_, err := conn.Exec(ctx, query)
	require.NoError(t, err, "error while creating table")

	// Insert a row into the table
	query = fmt.Sprintf(`
	INSERT INTO %s.test(
		col_bool,
		col_int4,
		col_int8,
		col_float4,
		col_float8,
		col_text,
		col_bytea,
		col_json,
		col_uuid,
		col_timestamp,
		col_numeric,
		col_tz,
		col_tz2,
		col_tz3,
		col_tz4,
		col_date
	) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
		$12, $13, $14, $15, $16
		)`,
		schemaName)

	savedTime := time.Now().UTC()
	savedUUID := uuid.New()

	_, err = conn.Exec(
		context.Background(),
		query,
		true,               // col_bool
		int32(2),           // col_int4
		int64(3),           // col_int8
		float32(1.1),       // col_float4
		float64(2.2),       // col_float8
		"text",             // col_text
		[]byte("bytea"),    // col_bytea
		`{"key": "value"}`, // col_json
		savedUUID,          // col_uuid
		savedTime,          // col_timestamp
		"123.456",          // col_numeric
		savedTime,          // col_tz
		savedTime,          // col_tz2
		savedTime,          // col_tz3
		savedTime,          // col_tz4
		savedTime,          // col_date
	)
	require.NoError(t, err, "error while inserting into test table")

	qe, err := connector.NewQRepQueryExecutor(ctx, "test flow", "test part")
	require.NoError(t, err, "error while creating QRepQueryExecutor")
	// Select the row back out of the table
	query = fmt.Sprintf("SELECT * FROM %s.test;", schemaName)
	rows, err := qe.ExecuteQuery(context.Background(), query)
	require.NoError(t, err, "error while executing query")
	defer rows.Close()

	// Use rows.FieldDescriptions() to get field descriptions
	fieldDescriptions := rows.FieldDescriptions()

	batch, err := qe.ProcessRows(ctx, rows, fieldDescriptions)
	require.NoError(t, err, "error while processing rows")
	require.Len(t, batch.Records, 1, "expected 1 record")

	// Retrieve the results.
	record := batch.Records[0]

	expectedBool := true
	require.Equal(t, expectedBool, record[0].Value(), "expected true")

	expectedInt4 := int32(2)
	require.Equal(t, expectedInt4, record[1].Value(), "expected 2")

	expectedInt8 := int64(3)
	require.Equal(t, expectedInt8, record[2].Value(), "expected 3")

	expectedFloat4 := float32(1.1)
	if record[3].Value().(float32) != expectedFloat4 {
		t.Fatalf("expected %v, got %v", expectedFloat4, record[3].Value())
	}

	expectedFloat8 := float64(2.2)
	if record[4].Value().(float64) != expectedFloat8 {
		t.Fatalf("expected %v, got %v", expectedFloat8, record[4].Value())
	}

	expectedText := "text"
	require.Equal(t, expectedText, record[5].Value(), "expected 'text'")

	expectedBytea := []byte("bytea")
	require.Equal(t, expectedBytea, record[6].Value(), "expected 'bytea'")

	expectedJSON := `{"key":"value"}`
	require.Equal(t, expectedJSON, record[7].Value(), "expected '{\"key\":\"value\"}'")

	actualUUID := record[8].Value().(uuid.UUID)
	require.Equal(t, savedUUID[:], actualUUID[:], "expected savedUUID: %v", savedUUID)
	actualTime := record[9].Value().(time.Time)
	require.Equal(t, savedTime.Truncate(time.Second),
		actualTime.Truncate(time.Second), "expected savedTime: %v", savedTime)

	expectedNumeric := "123.456"
	actualNumeric := record[10].Value().(decimal.Decimal).String()
	require.Equal(t, expectedNumeric, actualNumeric, "expected 123.456")
}
