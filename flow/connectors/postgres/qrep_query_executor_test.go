package connpostgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func setupDB(t *testing.T) (*PostgresConnector, string) {
	t.Helper()

	connector, err := NewPostgresConnector(t.Context(),
		nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err, "error while creating connector")

	// Create unique schema name using current time
	schemaName := fmt.Sprintf("qrep_query_executor_%d", time.Now().Unix())

	// Create the schema
	_, err = connector.conn.Exec(t.Context(),
		"CREATE SCHEMA "+utils.QuoteIdentifier(schemaName))
	require.NoError(t, err, "error while creating schema")

	return connector, schemaName
}

func teardownDB(t *testing.T, conn *pgx.Conn, schemaName string) {
	t.Helper()

	_, err := conn.Exec(t.Context(),
		fmt.Sprintf("DROP SCHEMA %s CASCADE", utils.QuoteIdentifier(schemaName)))
	require.NoError(t, err, "error while dropping schema")
}

func TestExecuteAndProcessQuery(t *testing.T) {
	ctx := t.Context()
	connector, schemaName := setupDB(t)
	conn := connector.conn
	defer connector.Close()
	defer teardownDB(t, conn, schemaName)

	_, err := conn.Exec(ctx,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.test(id SERIAL PRIMARY KEY, data TEXT)", utils.QuoteIdentifier(schemaName)))
	require.NoError(t, err, "error while creating table")

	_, err = conn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s.test(data) VALUES ('testdata')", utils.QuoteIdentifier(schemaName)))
	require.NoError(t, err, "error while inserting data")

	qe, err := connector.NewQRepQueryExecutor(ctx, "test flow", "test part")
	require.NoError(t, err, "error while creating QRepQueryExecutor")

	batch, err := qe.ExecuteAndProcessQuery(t.Context(), fmt.Sprintf("SELECT * FROM %s.test", utils.QuoteIdentifier(schemaName)))
	require.NoError(t, err, "error while executing query")
	require.Len(t, batch.Records, 1, "expected 1 record")
	require.Equal(t, "testdata", batch.Records[0][1].Value(), "expected 'testdata'")
}

func TestSupportedDataTypes(t *testing.T) {
	ctx := t.Context()
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
	);`, utils.QuoteIdentifier(schemaName))

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
	) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, utils.QuoteIdentifier(schemaName))

	savedTime := time.Now().UTC()
	savedUUID := uuid.New()

	_, err = conn.Exec(
		t.Context(),
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
	batch, err := qe.ExecuteAndProcessQuery(t.Context(),
		fmt.Sprintf("SELECT * FROM %s.test", utils.QuoteIdentifier(schemaName)))
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

	require.JSONEq(t, `{"key":"value"}`, record[7].Value().(string), "expected '{\"key\":\"value\"}'")

	actualUUID := record[8].Value().(uuid.UUID)
	require.Equal(t, savedUUID[:], actualUUID[:], "expected savedUUID: %v", savedUUID)
	actualTime := record[9].Value().(time.Time)
	require.Equal(t, savedTime.Truncate(time.Second),
		actualTime.Truncate(time.Second), "expected savedTime: %v", savedTime)

	expectedNumeric := "123.456"
	actualNumeric := record[10].Value().(decimal.Decimal).String()
	require.Equal(t, expectedNumeric, actualNumeric, "expected 123.456")
}

func TestStringDataTypes(t *testing.T) {
	tests := []struct {
		Type          string
		Literal       string   // skipped if empty
		Expected      string   // skipped if empty
		ArrayLiteral  string   // skipped if empty
		ArrayExpected []string // skipped if empty
	}{
		{
			Type:          "text",
			Literal:       "'abc'",
			Expected:      "abc",
			ArrayLiteral:  "ARRAY['abc', 'def', NULL]",
			ArrayExpected: []string{"abc", "def", ""},
		},
		{
			Type:          "bytea",
			Literal:       "",
			Expected:      "",
			ArrayLiteral:  `'{"\\x012345", "\\x6789ab", NULL}'::bytea[]`,
			ArrayExpected: []string{"\x01\x23\x45", "\x67\x89\xab", ""},
		},
		{
			Type:          "bit(3)",
			Literal:       "b'101'",
			Expected:      "101",
			ArrayLiteral:  "ARRAY[b'101', b'111', NULL]",
			ArrayExpected: []string{"101", "111", ""},
		},
		{
			Type:          "varbit",
			Literal:       "b'101'",
			Expected:      "101",
			ArrayLiteral:  "ARRAY[b'1', b'101', NULL]",
			ArrayExpected: []string{"1", "101", ""},
		},
		{
			Type:          "xml",
			Literal:       "'<item>data</item>'::xml",
			Expected:      "<item>data</item>",
			ArrayLiteral:  `'{"<root><test>value</test></root>", "<item>data</item>", NULL}'::xml[]`,
			ArrayExpected: []string{"<root><test>value</test></root>", "<item>data</item>", ""},
		},
		{
			Type:          "time",
			Literal:       "",
			Expected:      "",
			ArrayLiteral:  `'{"12:30:45", "18:15:30", NULL}'::time[]`,
			ArrayExpected: []string{"12:30:45.000000", "18:15:30.000000", ""},
		},
		{
			Type:          "timetz",
			Literal:       "",
			Expected:      "",
			ArrayLiteral:  `'{"12:30:45+05", "18:15:30-08", NULL}'::timetz[]`,
			ArrayExpected: []string{"12:30:45+05", "18:15:30-08", ""},
		},
		{
			Type:          "interval",
			Literal:       "'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval",
			Expected:      "{\"minutes\":1,\"seconds\":2.22,\"days\":29,\"months\":2,\"years\":5,\"valid\":true}",
			ArrayLiteral:  `'{"1 day", "2 hours 30 minutes", NULL}'::interval[]`,
			ArrayExpected: []string{"{\"days\":1,\"valid\":true}", "{\"hours\":2,\"minutes\":30,\"valid\":true}", ""},
		},
		{
			Type:          "point",
			Literal:       "",
			Expected:      "",
			ArrayLiteral:  `'{"(1,2)", "(3,4)", NULL}'::point[]`,
			ArrayExpected: []string{"(1,2)", "(3,4)", ""},
		},
		{
			Type:          "lseg",
			Literal:       "'[(1,1),(2,2)]'::lseg",
			Expected:      "[(1,1),(2,2)]",
			ArrayLiteral:  `'{"[(1,1),(2,2)]", "[(3,3),(4,4)]", NULL}'::lseg[]`,
			ArrayExpected: []string{"[(1,1),(2,2)]", "[(3,3),(4,4)]", ""},
		},
		{
			Type:          "path",
			Literal:       "'((1,1),(2,2),(3,1))'::path",
			Expected:      "((1,1),(2,2),(3,1))",
			ArrayLiteral:  `'{"((1,1),(2,2),(3,1))", "((4,4),(5,5),(6,4))", NULL}'::path[]`,
			ArrayExpected: []string{"((1,1),(2,2),(3,1))", "((4,4),(5,5),(6,4))", ""},
		},
		{
			Type:          "box",
			Literal:       "'((1,1),(3,3))'::box",
			Expected:      "(3,3),(1,1)",
			ArrayLiteral:  `array['((1,1),(3,3))','((4,4),(6,6))', NULL]::box[]`,
			ArrayExpected: []string{"(3,3),(1,1)", "(6,6),(4,4)", ""},
		},
		{
			Type:          "polygon",
			Literal:       "'((1,1),(2,2),(3,1))'::polygon",
			Expected:      "((1,1),(2,2),(3,1))",
			ArrayLiteral:  `'{"((1,1),(2,2),(3,1))", "((4,4),(5,5),(6,4))", NULL}'::polygon[]`,
			ArrayExpected: []string{"((1,1),(2,2),(3,1))", "((4,4),(5,5),(6,4))", ""},
		},
		{
			Type:          "macaddr",
			Literal:       "",
			Expected:      "",
			ArrayLiteral:  `'{"08:00:2b:01:02:03", "08:00:2b:01:02:04", NULL}'::macaddr[]`,
			ArrayExpected: []string{"08:00:2b:01:02:03", "08:00:2b:01:02:04", ""},
		},
		{
			Type:          "cidr",
			Literal:       "",
			Expected:      "",
			ArrayLiteral:  `'{"192.168.1.0/24", "10.0.0.0/8", NULL}'::cidr[]`,
			ArrayExpected: []string{"192.168.1.0/24", "10.0.0.0/8", ""},
		},
		{
			Type:          "inet",
			Literal:       "",
			Expected:      "",
			ArrayLiteral:  `'{"192.168.1.1/32", "10.0.0.1/32", NULL}'::inet[]`,
			ArrayExpected: []string{"192.168.1.1/32", "10.0.0.1/32", ""},
		},
		{
			Type:          "int4range",
			Literal:       "'[1,100]'::int4range",
			Expected:      "[1,101)",
			ArrayLiteral:  `'{"[1,100]", "[200,300]", NULL}'::int4range[]`,
			ArrayExpected: []string{"[1,101)", "[200,301)", ""},
		},
		{
			Type:          "int4range",
			Literal:       "'(,)'::int4range",
			Expected:      "(,)",
			ArrayLiteral:  `'{"(,)", "(,)"}'::int4range[]`,
			ArrayExpected: []string{"(,)", "(,)"},
		},
		{
			Type:          "int8range",
			Literal:       "'[1,10000000000]'::int8range",
			Expected:      "[1,10000000001)",
			ArrayLiteral:  `'{"[1,10000000000]", "[20000000000,30000000000]", NULL}'::int8range[]`,
			ArrayExpected: []string{"[1,10000000001)", "[20000000000,30000000001)", ""},
		},
		{
			Type:          "int8range",
			Literal:       "'(,)'::int8range",
			Expected:      "(,)",
			ArrayLiteral:  `'{"(,)", "(,)"}'::int8range[]`,
			ArrayExpected: []string{"(,)", "(,)"},
		},
		{
			Type:          "numrange",
			Literal:       "'[1.5,99.9]'::numrange",
			Expected:      "[1.5,99.9]",
			ArrayLiteral:  `'{"[1.5,99.9]", "[200.1,300.8]", NULL}'::numrange[]`,
			ArrayExpected: []string{"[1.5,99.9]", "[200.1,300.8]", ""},
		},
		{
			Type:          "numrange",
			Literal:       "'(,)'::numrange",
			Expected:      "(,)",
			ArrayLiteral:  `'{"(,)", "(,)"}'::numrange[]`,
			ArrayExpected: []string{"(,)", "(,)"},
		},
		{
			Type:          "tsrange",
			Literal:       "'[2023-01-01 00:00:00,2023-12-31 23:59:59]'::tsrange",
			Expected:      "[2023-01-01 00:00:00,2023-12-31 23:59:59]",
			ArrayLiteral:  `'{"[2023-01-01 00:00:00,2023-12-31 23:59:59]", "[2024-01-01 00:00:00,2024-12-31 23:59:59]", NULL}'::tsrange[]`,
			ArrayExpected: []string{"[2023-01-01 00:00:00,2023-12-31 23:59:59]", "[2024-01-01 00:00:00,2024-12-31 23:59:59]", ""},
		},
		{
			Type:          "tsrange",
			Literal:       "'(,)'::tsrange",
			Expected:      "(,)",
			ArrayLiteral:  `'{"(,)", "(,)"}'::tsrange[]`,
			ArrayExpected: []string{"(,)", "(,)"},
		},
		{
			Type:     "tstzrange",
			Literal:  "'[2023-01-01 00:00:00-02,2023-12-31 23:59:59+00]'::tstzrange",
			Expected: "[2023-01-01 02:00:00Z,2023-12-31 23:59:59Z]",
			ArrayLiteral: `'{` +
				`"[2023-01-01 00:00:00-02,2023-12-31 23:59:59+00]",` +
				`"[2024-01-01 00:00:00-02,2024-12-31 23:59:59+00]",` +
				`NULL` +
				`}'::tstzrange[]`,
			ArrayExpected: []string{
				"[2023-01-01 02:00:00Z,2023-12-31 23:59:59Z]",
				"[2024-01-01 02:00:00Z,2024-12-31 23:59:59Z]",
				"",
			},
		},
		{
			Type:          "tstzrange",
			Literal:       "'(,)'::tstzrange",
			Expected:      "(,)",
			ArrayLiteral:  `'{"(,)", "(,)"}'::tstzrange[]`,
			ArrayExpected: []string{"(,)", "(,)"},
		},
		{
			Type:          "daterange",
			Literal:       "'[2023-01-01,2023-12-31]'::daterange",
			Expected:      "[2023-01-01,2024-01-01)",
			ArrayLiteral:  `'{"[2023-01-01,2023-12-31]", "[2024-01-01,2024-12-31]", NULL}'::daterange[]`,
			ArrayExpected: []string{"[2023-01-01,2024-01-01)", "[2024-01-01,2025-01-01)", ""},
		},
		{
			Type:          "daterange",
			Literal:       "'(,)'::daterange",
			Expected:      "(,)",
			ArrayLiteral:  `'{"(,)", "(,)"}'::daterange[]`,
			ArrayExpected: []string{"(,)", "(,)"},
		},
		{
			Type:          "int4multirange",
			Literal:       "'{[1,10],[20,30]}'::int4multirange",
			Expected:      "{[1,11),[20,31)}",
			ArrayLiteral:  `'{"{[1,10],[20,30]}", "{[100,110],[120,130]}", NULL}'::int4multirange[]`,
			ArrayExpected: []string{"{[1,11),[20,31)}", "{[100,111),[120,131)}", ""},
		},
		{
			Type:          "int4multirange",
			Literal:       "'{(,10],[20,)}'::int4multirange",
			Expected:      "{(,11),[20,)}",
			ArrayLiteral:  `'{"{(,10],[20,)}", "{(,110],[120,)}"}'::int4multirange[]`,
			ArrayExpected: []string{"{(,11),[20,)}", "{(,111),[120,)}"},
		},
		{
			Type:     "int8multirange",
			Literal:  "'{[1,10000000000],[20000000000,30000000000]}'::int8multirange",
			Expected: "{[1,10000000001),[20000000000,30000000001)}",
			ArrayLiteral: `'{` +
				`"{[1,10000000000],[20000000000,30000000000]}",` +
				`"{[40000000000,50000000000],[60000000000,70000000000]}",` +
				`NULL` +
				`}'::int8multirange[]`,
			ArrayExpected: []string{
				"{[1,10000000001),[20000000000,30000000001)}",
				"{[40000000000,50000000001),[60000000000,70000000001)}",
				"",
			},
		},
		{
			Type:          "int8multirange",
			Literal:       "'{(,10000000000],[20000000000,)}'::int8multirange",
			Expected:      "{(,10000000001),[20000000000,)}",
			ArrayLiteral:  `'{"{(,10000000000],[20000000000,)}", "{(,50000000000],[60000000000,)}"}'::int8multirange[]`,
			ArrayExpected: []string{"{(,10000000001),[20000000000,)}", "{(,50000000001),[60000000000,)}"},
		},
		{
			Type:     "nummultirange",
			Literal:  "'{[1.1,10.9],[20.1,30.9]}'::nummultirange",
			Expected: "{[1.1,10.9],[20.1,30.9]}",
			ArrayLiteral: `'{` +
				`"{[1.1,10.9],[20.1,30.9]}",` +
				`"{[100.1,110.9],[120.1,130.9]}",` +
				`NULL` +
				`}'::nummultirange[]`,
			ArrayExpected: []string{
				"{[1.1,10.9],[20.1,30.9]}",
				"{[100.1,110.9],[120.1,130.9]}",
				"",
			},
		},
		{
			Type:          "nummultirange",
			Literal:       "'{(,10.9],[20.1,)}'::nummultirange",
			Expected:      "{(,10.9],[20.1,)}",
			ArrayLiteral:  `'{"{(,10.9],[20.1,)}", "{(,110.9],[120.1,)}"}'::nummultirange[]`,
			ArrayExpected: []string{"{(,10.9],[20.1,)}", "{(,110.9],[120.1,)}"},
		},
		{
			Type:     "tsmultirange",
			Literal:  "'{[2023-01-01 00:00:00,2023-01-31 23:59:59],[2023-03-01 00:00:00,2023-03-31 23:59:59]}'::tsmultirange",
			Expected: "{[2023-01-01 00:00:00,2023-01-31 23:59:59],[2023-03-01 00:00:00,2023-03-31 23:59:59]}",
			ArrayLiteral: `'{` +
				`"{[2023-01-01 00:00:00,2023-01-31 23:59:59],[2023-03-01 00:00:00,2023-03-31 23:59:59]}",` +
				`"{[2024-01-01 00:00:00,2024-01-31 23:59:59],[2024-03-01 00:00:00,2024-03-31 23:59:59]}",` +
				`NULL` +
				`}'::tsmultirange[]`,
			ArrayExpected: []string{
				"{[\"2023-01-01 00:00:00\",\"2023-01-31 23:59:59\"],[\"2023-03-01 00:00:00\",\"2023-03-31 23:59:59\"]}",
				"{[\"2024-01-01 00:00:00\",\"2024-01-31 23:59:59\"],[\"2024-03-01 00:00:00\",\"2024-03-31 23:59:59\"]}",
				"",
			},
		},
		{
			Type:     "tsmultirange",
			Literal:  "'{(,2023-01-31 23:59:59],[2023-03-01 00:00:00,)}'::tsmultirange",
			Expected: "{(,2023-01-31 23:59:59],[2023-03-01 00:00:00,)}",
			ArrayLiteral: `'{` +
				`"{(,2023-01-31 23:59:59],[2023-03-01 00:00:00,)}",` +
				`"{(,2024-01-31 23:59:59],[2024-03-01 00:00:00,)}"` +
				`}'::tsmultirange[]`,
			ArrayExpected: []string{
				"{(,\"2023-01-31 23:59:59\"],[\"2023-03-01 00:00:00\",)}",
				"{(,\"2024-01-31 23:59:59\"],[\"2024-03-01 00:00:00\",)}",
			},
		},
		{
			Type:     "tstzmultirange",
			Literal:  "'{[2023-01-01 00:00:00-02,2023-01-31 23:59:59+00],[2023-03-01 00:00:00-02,2023-03-31 23:59:59+00]}'::tstzmultirange",
			Expected: "{[2023-01-01 02:00:00Z,2023-01-31 23:59:59Z],[2023-03-01 02:00:00Z,2023-03-31 23:59:59Z]}",
			ArrayLiteral: `'{"{[2023-01-01 00:00:00-02,2023-01-31 23:59:59+00],[2023-03-01 00:00:00-02,2023-03-31 23:59:59+00]}",` +
				`"{[2024-01-01 00:00:00-02,2024-01-31 23:59:59+00],[2024-03-01 00:00:00-02,2024-03-31 23:59:59+00]}",` +
				`NULL` +
				`}'::tstzmultirange[]`,
			ArrayExpected: []string{
				"{[\"2023-01-01 02:00:00+00\",\"2023-01-31 23:59:59+00\"],[\"2023-03-01 02:00:00+00\",\"2023-03-31 23:59:59+00\"]}",
				"{[\"2024-01-01 02:00:00+00\",\"2024-01-31 23:59:59+00\"],[\"2024-03-01 02:00:00+00\",\"2024-03-31 23:59:59+00\"]}",
				"",
			},
		},
		{
			Type:     "tstzmultirange",
			Literal:  "'{(,2023-01-31 23:59:59+00],[2023-03-01 00:00:00-02,)}'::tstzmultirange",
			Expected: "{(,2023-01-31 23:59:59Z],[2023-03-01 02:00:00Z,)}",
			ArrayLiteral: `'{` +
				`"{(,2023-01-31 23:59:59+00],[2023-03-01 00:00:00-02,)}",` +
				`"{(,2024-01-31 23:59:59+00],[2024-03-01 00:00:00-02,)}"` +
				`}'::tstzmultirange[]`,
			ArrayExpected: []string{
				"{(,\"2023-01-31 23:59:59+00\"],[\"2023-03-01 02:00:00+00\",)}",
				"{(,\"2024-01-31 23:59:59+00\"],[\"2024-03-01 02:00:00+00\",)}",
			},
		},
		{
			Type:     "datemultirange",
			Literal:  "'{[2023-01-01,2023-01-31],[2023-03-01,2023-03-31]}'::datemultirange",
			Expected: "{[2023-01-01,2023-02-01),[2023-03-01,2023-04-01)}",
			ArrayLiteral: `'{` +
				`"{[2023-01-01,2023-01-31],[2023-03-01,2023-03-31]}",` +
				`"{[2024-01-01,2024-01-31],[2024-03-01,2024-03-31]}",` +
				`NULL` +
				`}'::datemultirange[]`,
			ArrayExpected: []string{
				"{[2023-01-01,2023-02-01),[2023-03-01,2023-04-01)}",
				"{[2024-01-01,2024-02-01),[2024-03-01,2024-04-01)}",
				"",
			},
		},
		{
			Type:          "datemultirange",
			Literal:       "'{(,2023-01-31],[2023-03-01,)}'::datemultirange",
			Expected:      "{(,2023-02-01),[2023-03-01,)}",
			ArrayLiteral:  `'{"{(,2023-01-31],[2023-03-01,)}","{(,2024-01-31],[2024-03-01,)}"}'::datemultirange[]`,
			ArrayExpected: []string{"{(,2023-02-01),[2023-03-01,)}", "{(,2024-02-01),[2024-03-01,)}"},
		},
	}

	ctx := t.Context()
	connector, schemaName := setupDB(t)
	conn := connector.conn
	defer conn.Close(ctx)
	defer teardownDB(t, conn, schemaName)

	for _, tc := range tests {
		t.Run(tc.Type, func(t *testing.T) {
			query := fmt.Sprintf(
				"CREATE TABLE %s.test_strings(col %s, col_arr %s[])",
				utils.QuoteIdentifier(schemaName), tc.Type, tc.Type,
			)
			_, err := conn.Exec(ctx, query)
			require.NoError(t, err)

			defer func() {
				query := fmt.Sprintf(
					"DROP TABLE %s.test_strings", utils.QuoteIdentifier(schemaName),
				)
				_, err := conn.Exec(ctx, query)
				require.NoError(t, err)
			}()

			literal := tc.Literal
			if literal == "" {
				literal = "null"
			}
			arrayLiteral := tc.ArrayLiteral
			if arrayLiteral == "" {
				arrayLiteral = "null"
			}
			query = fmt.Sprintf(
				"INSERT INTO %s.test_strings(col, col_arr) VALUES (%s, %s)",
				utils.QuoteIdentifier(schemaName), literal, arrayLiteral,
			)
			_, err = conn.Exec(ctx, query)
			require.NoError(t, err)

			qe, err := connector.NewQRepQueryExecutor(ctx, "test flow", "test part")
			require.NoError(t, err)
			// Select the row back out of the table
			batch, err := qe.ExecuteAndProcessQuery(t.Context(),
				fmt.Sprintf("SELECT * FROM %s.test_strings", utils.QuoteIdentifier(schemaName)))
			require.NoError(t, err)
			require.Len(t, batch.Records, 1)

			// Retrieve the results.
			record := batch.Records[0]

			if tc.Expected != "" {
				str, ok := record[0].Value().(string)
				require.True(t, ok)
				require.Equal(t, tc.Expected, str)
			}
			if tc.ArrayExpected != nil {
				strs, ok := record[1].Value().([]string)
				require.True(t, ok)
				require.Len(t, strs, len(tc.ArrayExpected))
				for i, expected := range tc.ArrayExpected {
					require.Equal(t, expected, strs[i])
				}
			}
		})
	}
}
