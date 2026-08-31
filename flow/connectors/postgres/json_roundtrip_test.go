package connpostgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// jsonRoundtripCase is one JSON document stored in Postgres and read back out.
// want is the document we expect to arrive at the destination; it differs from
// input only where a number is out of float64 range and becomes a string.
//
// The json[]/jsonb[] columns get input plus a trailing SQL NULL element by
// default. Cases that exist to exercise the array splicing itself override that
// with elems/wantArr.
type jsonRoundtripCase struct {
	name    string
	input   string
	want    string
	elems   []any
	wantArr string
}

// arrayInput is the value stored in the json[]/jsonb[] columns.
func (tc jsonRoundtripCase) arrayInput() []any {
	if tc.elems != nil {
		return tc.elems
	}
	return []any{tc.input, nil}
}

// arrayWant is the JSON array expected back from those columns.
func (tc jsonRoundtripCase) arrayWant() string {
	if tc.wantArr != "" {
		return tc.wantArr
	}
	return "[" + tc.want + ",null]"
}

func jsonRoundtripCases() []jsonRoundtripCase {
	big := "1" + strings.Repeat("0", 1000)

	return []jsonRoundtripCase{
		// Objects.
		{name: "object", input: `{"a":1,"b":"two","c":true,"d":null}`, want: `{"a":1,"b":"two","c":true,"d":null}`},
		{name: "empty object", input: `{}`, want: `{}`},
		{name: "nested object", input: `{"a":{"b":{"c":{"d":1}}}}`, want: `{"a":{"b":{"c":{"d":1}}}}`},
		{name: "object with array values", input: `{"xs":[1,2,3],"ys":[]}`, want: `{"xs":[1,2,3],"ys":[]}`},
		{
			name:  "object with overflowing number",
			input: `{"small":1,"big":` + big + `}`,
			want:  `{"small":1,"big":"` + big + `"}`,
		},
		{name: "object with unicode and escapes", input: `{"k":"日本語 🎉 \"q\" \\ \n"}`, want: `{"k":"日本語 🎉 \"q\" \\ \n"}`},
		{name: "object with empty key", input: `{"":"empty"}`, want: `{"":"empty"}`},
		{name: "object with numeric string", input: `{"a":"42"}`, want: `{"a":"42"}`},

		// Arrays. These are JSON arrays stored in a single json/jsonb column,
		// which is a different thing from a Postgres json[] column below.
		{name: "array of scalars", input: `[1,2.5,"three",true,null]`, want: `[1,2.5,"three",true,null]`},
		{name: "empty array", input: `[]`, want: `[]`},
		{name: "array of objects", input: `[{"a":1},{"a":2}]`, want: `[{"a":1},{"a":2}]`},
		{name: "nested arrays", input: `[[1,[2,[3]]],[]]`, want: `[[1,[2,[3]]],[]]`},
		{
			name:  "array with overflowing number",
			input: `[1,` + big + `,2]`,
			want:  `[1,"` + big + `",2]`,
		},
		{name: "array of nulls", input: `[null,null]`, want: `[null,null]`},

		// Top-level scalars are valid JSON documents too.
		{name: "top level number", input: `1`, want: `1`},
		{name: "top level overflow", input: big, want: `"` + big + `"`},
		{name: "top level string", input: `"hi"`, want: `"hi"`},
		{name: "top level true", input: `true`, want: `true`},
		{name: "top level json null", input: `null`, want: `null`},

		// Array-column shapes. Each element is a separate document spliced into
		// one JSON array, and a SQL NULL element has to keep its slot for the
		// result to stay well-formed.
		{name: "empty array column", input: `{"a":1}`, want: `{"a":1}`, elems: []any{}, wantArr: `[]`},
		{
			name: "single element array column", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{`{"a":1}`}, wantArr: `[{"a":1}]`,
		},
		{
			name: "multi element array column", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{`{"a":1}`, `{"b":2}`}, wantArr: `[{"a":1},{"b":2}]`,
		},
		{
			name: "nested arrays as elements", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{`[1,2]`, `[[3]]`}, wantArr: `[[1,2],[[3]]]`,
		},
		{
			name: "mixed kinds as elements", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{`{"a":1}`, `[1,2]`, `"s"`, `7`, `true`}, wantArr: `[{"a":1},[1,2],"s",7,true]`,
		},
		{
			name: "leading sql null element", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{nil, `{"a":1}`}, wantArr: `[null,{"a":1}]`,
		},
		{
			name: "all sql null elements", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{nil, nil}, wantArr: `[null,null]`,
		},
		// A JSON null element and a SQL NULL element are different inputs that
		// both have to serialise as null.
		{
			name: "json null element", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{`null`, `{"a":1}`}, wantArr: `[null,{"a":1}]`,
		},
		{
			name: "overflowing number in element", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{`{"big":` + big + `}`, `{"a":1}`}, wantArr: `[{"big":"` + big + `"},{"a":1}]`,
		},
		{
			name: "unicode in element", input: `{"a":1}`, want: `{"a":1}`,
			elems: []any{`{"k":"日本語 🎉"}`}, wantArr: `[{"k":"日本語 🎉"}]`,
		},
	}
}

// TestJSONRoundtripQRep stores each document in json, jsonb, json[] and jsonb[]
// columns, reads the whole set back through the QRep executor in one query, and
// checks the values that reach the destination. This covers mapRowToQRecord plus
// parseFieldFromPostgresOID.
func TestJSONRoundtripQRep(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector, schemaName := setupDB(t, "json_roundtrip_qrep")
	conn := connector.conn
	defer connector.Close()
	defer teardownDB(t, conn, schemaName)

	_, err := conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.t(id INT PRIMARY KEY, col_json JSON, col_jsonb JSONB, col_json_arr JSON[], col_jsonb_arr JSONB[])",
		common.QuoteIdentifier(schemaName)))
	require.NoError(t, err, "error while creating table")

	cases := jsonRoundtripCases()
	for i, tc := range cases {
		_, err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s.t(id, col_json, col_jsonb, col_json_arr, col_jsonb_arr) VALUES($1,$2,$3,$4,$5)",
			common.QuoteIdentifier(schemaName)),
			i, tc.input, tc.input, tc.arrayInput(), tc.arrayInput())
		require.NoErrorf(t, err, "error inserting %s", tc.name)
	}

	env := map[string]string{"PEERDB_POSTGRES_FAST_PROCESS_JSON_COLUMNS": "true"}
	qe, err := connector.NewQRepQueryExecutor(ctx, env, nil, shared.InternalVersion_Latest, "test flow", "test part")
	require.NoError(t, err, "error while creating QRepQueryExecutor")

	batch, err := qe.ExecuteAndProcessQuery(ctx, fmt.Sprintf(
		"SELECT col_json, col_jsonb, col_json_arr, col_jsonb_arr FROM %s.t ORDER BY id",
		common.QuoteIdentifier(schemaName)))
	require.NoError(t, err, "error while processing rows")
	require.Len(t, batch.Records, len(cases), "expected one record per case")

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// jsonb is normalised by Postgres (key order, whitespace, number
			// spelling), so every column is compared semantically.
			for idx, col := range []struct {
				name string
				want string
			}{
				{"col_json", tc.want},
				{"col_jsonb", tc.want},
				{"col_json_arr", tc.arrayWant()},
				{"col_jsonb_arr", tc.arrayWant()},
			} {
				got, ok := batch.Records[i][idx].Value().(string)
				require.Truef(t, ok, "%s should decode to a string, got %T", col.name, batch.Records[i][idx].Value())
				require.JSONEq(t, col.want, got, col.name)
			}
		})
	}
}

// TestJSONRoundtripCDCDecode drives the same documents through the CDC decoder.
// decodeColumnData is fed the text-format wire bytes Postgres would put in a
// logical replication tuple, for json, jsonb and their array types.
func TestJSONRoundtripCDCDecode(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector, schemaName := setupDB(t, "json_roundtrip_cdc")
	conn := connector.conn
	defer connector.Close()
	defer teardownDB(t, conn, schemaName)

	p := &PostgresCDCSource{
		PostgresConnector:      connector,
		otelManager:            &otel_metrics.OtelManager{},
		internalVersion:        shared.InternalVersion_Latest,
		fastProcessJsonColumns: true,
	}

	_, err := conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.t(id INT PRIMARY KEY, col_json JSON, col_jsonb JSONB, col_json_arr JSON[], col_jsonb_arr JSONB[])",
		common.QuoteIdentifier(schemaName)))
	require.NoError(t, err, "error while creating table")

	for i, tc := range jsonRoundtripCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, err := conn.Exec(ctx, fmt.Sprintf(
				"INSERT INTO %s.t(id, col_json, col_jsonb, col_json_arr, col_jsonb_arr) VALUES($1,$2,$3,$4,$5)",
				common.QuoteIdentifier(schemaName)),
				i, tc.input, tc.input, tc.arrayInput(), tc.arrayInput())
			require.NoError(t, err, "error inserting")

			// Read the row back in the text wire format that logical decoding uses.
			rows, err := conn.Query(ctx, fmt.Sprintf(
				"SELECT col_json, col_jsonb, col_json_arr, col_jsonb_arr FROM %s.t WHERE id = $1",
				common.QuoteIdentifier(schemaName)),
				pgx.QueryResultFormats{pgtype.TextFormatCode}, i)
			require.NoError(t, err, "error querying raw values")
			defer rows.Close()
			require.True(t, rows.Next(), "expected a row")

			raw := rows.RawValues()
			fds := rows.FieldDescriptions()
			decoded := make([]string, len(fds))
			for col := range fds {
				qv, err := p.decodeColumnData(
					raw[col], fds[col].DataTypeOID, fds[col].TypeModifier, pgtype.TextFormatCode, nil,
					shared.InternalVersion_Latest,
				)
				require.NoErrorf(t, err, "decodeColumnData for %s", fds[col].Name)
				str, ok := qv.Value().(string)
				require.Truef(t, ok, "%s should decode to a string, got %T", fds[col].Name, qv.Value())
				decoded[col] = str
			}
			rows.Close()
			require.NoError(t, rows.Err())

			require.JSONEq(t, tc.want, decoded[0], "col_json")
			require.JSONEq(t, tc.want, decoded[1], "col_jsonb")

			require.JSONEq(t, tc.arrayWant(), decoded[2], "col_json_arr")
			require.JSONEq(t, tc.arrayWant(), decoded[3], "col_jsonb_arr")
		})
	}
}
