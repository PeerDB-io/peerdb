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

// TestDefaultExprFromPostgresDefault feeds in what pg_get_expr renders for a DEFAULT; want is empty
// for the ones we decline to translate.
func TestDefaultExprFromPostgresDefault(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		rendered string
		qkind    types.QValueKind
		want     string
	}{
		// integers, which pg quotes once a sign or enough width is involved
		{name: "int", rendered: "5", qkind: types.QValueKindInt32, want: "5"},
		{name: "int negative", rendered: "'-1'::integer", qkind: types.QValueKindInt32, want: "-1"},
		{name: "int parenthesized", rendered: "(-1)", qkind: types.QValueKindInt32, want: "-1"},
		{name: "int max", rendered: "2147483647", qkind: types.QValueKindInt32, want: "2147483647"},
		// a smallint default is labelled with the type of the constant pg folded, not the column's
		{name: "smallint negative", rendered: "'-32768'::integer", qkind: types.QValueKindInt16, want: "-32768"},
		{name: "bigint", rendered: "'9223372036854775807'::bigint", qkind: types.QValueKindInt64, want: "9223372036854775807"},
		{name: "oid", rendered: "42", qkind: types.QValueKindUInt32, want: "42"},

		// scale is whatever pg rendered, so DECIMAL(10,2) DEFAULT 1.50 keeps its trailing zero
		{name: "numeric", rendered: "1.50", qkind: types.QValueKindNumeric, want: "1.50"},
		{name: "double", rendered: "2.5", qkind: types.QValueKindFloat64, want: "2.5"},
		{name: "real negative", rendered: "'-1.5'::numeric", qkind: types.QValueKindFloat32, want: "-1.5"},
		{name: "double exponent", rendered: "1e-05", qkind: types.QValueKindFloat64, want: "1e-05"},

		{name: "bool true", rendered: "true", qkind: types.QValueKindBoolean, want: "true"},
		{name: "bool false", rendered: "false", qkind: types.QValueKindBoolean, want: "false"},

		// strings, requoted with SQL doubling so the literal is dialect neutral
		{name: "text", rendered: "'hello'::text", qkind: types.QValueKindString, want: "'hello'"},
		{name: "text empty", rendered: "''::text", qkind: types.QValueKindString, want: "''"},
		{name: "text with quote", rendered: "'it''s'::text", qkind: types.QValueKindString, want: "'it''s'"},
		{name: "text with cast inside", rendered: "'a::b'::text", qkind: types.QValueKindString, want: "'a::b'"},
		{name: "varchar", rendered: "'abc'::character varying", qkind: types.QValueKindString, want: "'abc'"},
		{name: "bpchar", rendered: "'ab'::bpchar", qkind: types.QValueKindString, want: "'ab'"},
		{name: "qchar", rendered: `'x'::"char"`, qkind: types.QValueKindQChar, want: "'x'"},
		{name: "enum", rendered: "'ok'::mood", qkind: types.QValueKindEnum, want: "'ok'"},
		{name: "enum schema qualified", rendered: `'ok'::public."Mood"`, qkind: types.QValueKindEnum, want: "'ok'"},
		{
			name: "uuid", rendered: "'00000000-0000-0000-0000-000000000001'::uuid", qkind: types.QValueKindUUID,
			want: "'00000000-0000-0000-0000-000000000001'",
		},
		{name: "jsonb", rendered: `'{"a": 1}'::jsonb`, qkind: types.QValueKindJSONB, want: `'{"a": 1}'`},
		{name: "inet", rendered: "'10.0.0.1'::inet", qkind: types.QValueKindINET, want: "'10.0.0.1'"},
		{name: "date", rendered: "'2020-01-02'::date", qkind: types.QValueKindDate, want: "'2020-01-02'"},
		{
			name: "timestamp", rendered: "'2020-01-02 03:04:05.678'::timestamp without time zone",
			qkind: types.QValueKindTimestamp, want: "'2020-01-02 03:04:05.678'",
		},
		// pg renders the offset with timezone=UTC, which our connections set; the destination column
		// has no zone of its own
		{
			name: "timestamptz", rendered: "'2020-01-02 01:04:05+00'::timestamp with time zone",
			qkind: types.QValueKindTimestampTZ, want: "'2020-01-02 01:04:05'",
		},

		// declined: not a constant
		{name: "now", rendered: "now()", qkind: types.QValueKindTimestamp},
		{name: "current timestamp", rendered: "CURRENT_TIMESTAMP", qkind: types.QValueKindTimestamp},
		{name: "current user", rendered: "CURRENT_USER", qkind: types.QValueKindString},
		{name: "sequence", rendered: "nextval('t_id_seq'::regclass)", qkind: types.QValueKindInt32},
		{name: "arithmetic", rendered: "(2 + 3)", qkind: types.QValueKindInt32},
		{name: "generated expression", rendered: "(id * 2)", qkind: types.QValueKindInt32},
		{name: "concatenation", rendered: "('a'::text || 'b'::text)", qkind: types.QValueKindString},
		{name: "concatenation unparenthesized", rendered: "'a'::text || 'b'::text", qkind: types.QValueKindString},
		{name: "function of constant", rendered: "upper('a'::text)", qkind: types.QValueKindString},
		{name: "chained cast", rendered: "'5'::text::integer", qkind: types.QValueKindInt32},
		{name: "null", rendered: "NULL::integer", qkind: types.QValueKindInt32},
		{name: "unterminated literal", rendered: "'oops", qkind: types.QValueKindString},

		// declined: a constant whose text form does not carry over
		{name: "array", rendered: "'{1,2}'::integer[]", qkind: types.QValueKindArrayInt32},
		{name: "bytea", rendered: `'\x0001'::bytea`, qkind: types.QValueKindBytes},
		{name: "interval", rendered: "'1 day'::interval", qkind: types.QValueKindInterval},
		{name: "time", rendered: "'13:14:15'::time without time zone", qkind: types.QValueKindTime},
		{name: "point", rendered: "'(1,2)'::point", qkind: types.QValueKindPoint},
		{name: "numeric nan", rendered: "'NaN'::numeric", qkind: types.QValueKindNumeric},
		{name: "double infinity", rendered: "'Infinity'::double precision", qkind: types.QValueKindFloat64},
		{
			name: "timestamptz non utc offset", rendered: "'2020-01-02 01:04:05+02'::timestamp with time zone",
			qkind: types.QValueKindTimestampTZ,
		},
		// backslashes and control characters escape differently across dialects
		{name: "text with backslash", rendered: `'a\b'::text`, qkind: types.QValueKindString},
		{name: "text with newline", rendered: "'a\nb'::text", qkind: types.QValueKindString},

		// declined: constant of the wrong shape for the column
		{name: "quoted bool", rendered: "'true'::boolean", qkind: types.QValueKindBoolean},
		{name: "money", rendered: "1.99", qkind: types.QValueKindString},
		{name: "number for text", rendered: "5", qkind: types.QValueKindString},
		{name: "text for number", rendered: "'abc'::text", qkind: types.QValueKindInt32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			literal, ok := defaultExprFromPostgresDefault(tc.rendered, tc.qkind)
			require.Equal(t, tc.want != "", ok)
			require.Equal(t, tc.want, literal)
		})
	}
}
