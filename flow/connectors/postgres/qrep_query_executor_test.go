package connpostgres

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func setupDB(t *testing.T) (*pgx.Conn, string) {
	t.Helper()

	config, err := pgx.ParseConfig("postgres://postgres:postgres@localhost:7132/postgres")
	if err != nil {
		t.Fatalf("unable to parse config: %v", err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("unable to connect to database: %v", err)
	}

	// Create unique schema name using current time
	schemaName := fmt.Sprintf("schema_%d", time.Now().Unix())

	// Create the schema
	_, err = conn.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA %s;", schemaName))
	if err != nil {
		t.Fatalf("unable to create schema: %v", err)
	}

	return conn, schemaName
}

func teardownDB(t *testing.T, conn *pgx.Conn, schemaName string) {
	t.Helper()

	_, err := conn.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE;", schemaName))
	if err != nil {
		t.Fatalf("error while dropping schema: %v", err)
	}
}

func TestExecuteAndProcessQuery(t *testing.T) {
	conn, schemaName := setupDB(t)
	defer conn.Close(context.Background())

	defer teardownDB(t, conn, schemaName)

	ctx := context.Background()

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.test(id SERIAL PRIMARY KEY, data TEXT);", schemaName)
	_, err := conn.Exec(ctx, query)
	if err != nil {
		t.Fatalf("error while creating test table: %v", err)
	}

	query = fmt.Sprintf("INSERT INTO %s.test(data) VALUES('testdata');", schemaName)
	_, err = conn.Exec(ctx, query)
	if err != nil {
		t.Fatalf("error while inserting into test table: %v", err)
	}

	qe := NewQRepQueryExecutor(conn, ctx, "test flow", "test part")
	qe.SetTestEnv(true)

	query = fmt.Sprintf("SELECT * FROM %s.test;", schemaName)
	batch, err := qe.ExecuteAndProcessQuery(query)
	if err != nil {
		t.Fatalf("error while executing and processing query: %v", err)
	}

	if len(batch.Records) != 1 {
		t.Fatalf("expected 1 record, got %v", len(batch.Records))
	}

	if batch.Records[0][1].Value != "testdata" {
		t.Fatalf("expected 'testdata', got %v", batch.Records[0][0].Value)
	}
}

func TestAllDataTypes(t *testing.T) {
	conn, schemaName := setupDB(t)
	defer conn.Close(context.Background())

	// Call teardownDB function after test
	defer teardownDB(t, conn, schemaName)

	ctx := context.Background()

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
	if err != nil {
		t.Fatalf("error while creating test table: %v", err)
	}

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

	savedTime := time.Now()
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
		savedUUID.String(), // col_uuid
		savedTime,          // col_timestamp
		"123.456",          // col_numeric
		savedTime,          // col_tz
		savedTime,          // col_tz2
		savedTime,          // col_tz3
		savedTime,          // col_tz4
		savedTime,          // col_date
	)
	if err != nil {
		t.Fatalf("error while inserting into test table: %v", err)
	}

	qe := NewQRepQueryExecutor(conn, ctx, "test flow", "test part")
	// Select the row back out of the table
	query = fmt.Sprintf("SELECT * FROM %s.test;", schemaName)
	rows, err := qe.ExecuteQuery(query)
	if err != nil {
		t.Fatalf("error while executing query: %v", err)
	}
	defer rows.Close()

	// Use rows.FieldDescriptions() to get field descriptions
	fieldDescriptions := rows.FieldDescriptions()

	batch, err := qe.ProcessRows(rows, fieldDescriptions)
	if err != nil {
		t.Fatalf("failed to process rows: %v", err)
	}

	if len(batch.Records) != 1 {
		t.Fatalf("expected 1 record, got %v", len(batch.Records))
	}

	// Retrieve the results.
	record := batch.Records[0]

	expectedBool := true
	if record[0].Value.(bool) != expectedBool {
		t.Fatalf("expected %v, got %v", expectedBool, record[0].Value)
	}

	expectedInt4 := int32(2)
	if record[1].Value.(int32) != expectedInt4 {
		t.Fatalf("expected %v, got %v", expectedInt4, record[1].Value)
	}

	expectedInt8 := int64(3)
	if record[2].Value.(int64) != expectedInt8 {
		t.Fatalf("expected %v, got %v", expectedInt8, record[2].Value)
	}

	expectedFloat4 := float32(1.1)
	if record[3].Value.(float32) != expectedFloat4 {
		t.Fatalf("expected %v, got %v", expectedFloat4, record[3].Value)
	}

	expectedFloat8 := float64(2.2)
	if record[4].Value.(float64) != expectedFloat8 {
		t.Fatalf("expected %v, got %v", expectedFloat8, record[4].Value)
	}

	expectedText := "text"
	if record[5].Value.(string) != expectedText {
		t.Fatalf("expected %v, got %v", expectedText, record[5].Value)
	}

	expectedBytea := []byte("bytea")
	if !bytes.Equal(record[6].Value.([]byte), expectedBytea) {
		t.Fatalf("expected %v, got %v", expectedBytea, record[6].Value)
	}

	expectedJSON := `{"key":"value"}`
	if record[7].Value.(string) != expectedJSON {
		t.Fatalf("expected %v, got %v", expectedJSON, record[7].Value)
	}

	actualUUID := record[8].Value.([16]uint8)
	if !bytes.Equal(actualUUID[:], savedUUID[:]) {
		t.Fatalf("expected %v, got %v", savedUUID, actualUUID)
	}

	expectedNumeric := "123.456"
	actualNumeric := record[10].Value.(*big.Rat).FloatString(3)
	if actualNumeric != expectedNumeric {
		t.Fatalf("expected %v, got %v", expectedNumeric, actualNumeric)
	}
}
