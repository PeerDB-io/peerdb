package connpostgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func newTestCDCSource(t *testing.T) *PostgresCDCSource {
	t.Helper()
	return &PostgresCDCSource{
		PostgresConnector: &PostgresConnector{},
		otelManager:       &otel_metrics.OtelManager{},
	}
}

func TestProcessMessageInvalidMessage(t *testing.T) {
	t.Parallel()

	p := newTestCDCSource(t)
	batch := model.NewCDCStream[model.RecordItems](0)

	xld := pglogrepl.XLogData{
		WALStart:     pglogrepl.LSN(0x01),
		ServerWALEnd: pglogrepl.LSN(0x02),
		// 'S' is a v2 Stream Start tag — not handled by pglogrepl.Parse (v1)
		WALData: []byte{'S', 0, 1, 0, 1 /*arbitrary bytes*/},
	}

	rec, err := processMessage(t.Context(), p, batch, xld, xld.WALStart, 0, qProcessor{}, map[string]struct{}{})
	require.Nil(t, rec)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error parsing logical message (msgType=\"S\", walStart=0/1)")
}

// TestDefaultExprFromPostgresMissingValue feeds in scalar values extracted from attmissingval; want
// is empty for the types and values we decline to translate.
func TestDefaultExprFromPostgresMissingValue(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value string
		qkind types.QValueKind
		want  string
	}{
		{name: "int", value: "5", qkind: types.QValueKindInt32, want: "5"},
		{name: "int negative", value: "-1", qkind: types.QValueKindInt32, want: "-1"},
		{name: "int max", value: "2147483647", qkind: types.QValueKindInt32, want: "2147483647"},
		{name: "smallint negative", value: "-32768", qkind: types.QValueKindInt16, want: "-32768"},
		{name: "bigint", value: "9223372036854775807", qkind: types.QValueKindInt64, want: "9223372036854775807"},
		{name: "oid", value: "42", qkind: types.QValueKindUInt32, want: "42"},

		{name: "numeric", value: "1.50", qkind: types.QValueKindNumeric, want: "1.50"},
		{name: "double", value: "2.5", qkind: types.QValueKindFloat64, want: "2.5"},
		{name: "real negative", value: "-1.5", qkind: types.QValueKindFloat32, want: "-1.5"},
		{name: "double exponent", value: "1e-05", qkind: types.QValueKindFloat64, want: "1e-05"},

		{name: "bool true", value: "true", qkind: types.QValueKindBoolean, want: "true"},
		{name: "bool false", value: "false", qkind: types.QValueKindBoolean, want: "false"},

		// strings, requoted with SQL doubling for ClickHouse
		{name: "text", value: "hello", qkind: types.QValueKindString, want: "'hello'"},
		{name: "text empty", value: "", qkind: types.QValueKindString, want: "''"},
		{name: "text with quote", value: "it's", qkind: types.QValueKindString, want: "'it''s'"},
		{name: "text with backslash n", value: "\\n", qkind: types.QValueKindString, want: "'\\\\n'"},
		{name: "text with backslash", value: "\\", qkind: types.QValueKindString, want: "'\\\\'"},
		{name: "text with two backslashes", value: "\\\\", qkind: types.QValueKindString, want: "'\\\\\\\\'"},
		{
			name: "text with mixed escapes", value: "\\n\\\\'", qkind: types.QValueKindString,
			want: "'\\\\n\\\\\\\\'''",
		},
		{name: "text with cast text", value: "a::b", qkind: types.QValueKindString, want: "'a::b'"},
		{name: "qchar", value: "x", qkind: types.QValueKindQChar, want: "'x'"},
		{name: "enum", value: "ok", qkind: types.QValueKindEnum, want: "'ok'"},
		{
			name: "uuid", value: "00000000-0000-0000-0000-000000000001", qkind: types.QValueKindUUID,
			want: "'00000000-0000-0000-0000-000000000001'",
		},
		{name: "jsonb", value: `{"a": 1}`, qkind: types.QValueKindJSONB, want: `'{"a": 1}'`},
		{
			name: "hstore canonicalized", value: `{"z": "<&>", "a": null}`,
			qkind: types.QValueKindHStore, want: `'{"a":null,"z":"\\u003c\\u0026\\u003e"}'`,
		},
		{name: "inet", value: "10.0.0.1", qkind: types.QValueKindINET, want: "'10.0.0.1'"},
		{name: "date", value: "2020-01-02", qkind: types.QValueKindDate, want: "'2020-01-02'"},
		{
			name: "timestamp", value: "2020-01-02T03:04:05.678",
			qkind: types.QValueKindTimestamp, want: "'2020-01-02 03:04:05.678'",
		},
		{
			name: "timestamptz", value: "2020-01-02T01:04:05+00:00",
			qkind: types.QValueKindTimestampTZ, want: "'2020-01-02 01:04:05'",
		},
		{name: "text with backslash b", value: `a\b`, qkind: types.QValueKindString, want: "'a\\\\b'"},
		{name: "text with newline", value: "a\nb", qkind: types.QValueKindString, want: "'a\nb'"},

		// declined: types whose text form does not carry over
		{name: "array", value: "[1,2]", qkind: types.QValueKindArrayInt32},
		{name: "bytea", value: `\x0001`, qkind: types.QValueKindBytes},
		{name: "hstore non-string value", value: `{"a": 1}`, qkind: types.QValueKindHStore},
		{name: "interval", value: "1 day", qkind: types.QValueKindInterval},
		{name: "time", value: "13:14:15", qkind: types.QValueKindTime},
		{name: "point", value: "(1,2)", qkind: types.QValueKindPoint},
		{name: "numeric nan", value: "NaN", qkind: types.QValueKindNumeric},
		{name: "double infinity", value: "Infinity", qkind: types.QValueKindFloat64},
		{
			name: "timestamptz non utc offset", value: "2020-01-02T01:04:05+02:00",
			qkind: types.QValueKindTimestampTZ,
		},
		// declined: value of the wrong shape for the column
		{name: "quoted bool", value: "'true'", qkind: types.QValueKindBoolean},
		{name: "text for number", value: "abc", qkind: types.QValueKindInt32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			literal, ok := defaultExprFromPostgresMissingValue(tc.value, tc.qkind)
			require.Equal(t, tc.want != "", ok)
			require.Equal(t, tc.want, literal)
		})
	}
}
