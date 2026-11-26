package clickhouse

import (
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var validColumnTypes = []string{
	"String",
	"JSON",
	"Nullable(String)",
	"Nullable(Bool)",
	"DateTime64(9)",
	"Decimal(38,10)",
	"FixedString(16)",
	"LowCardinality(Nullable(String))",
	"Array(Nullable(Int64))",
	"Array(JSON)",
	"Map(String,Array(Int64))",
}

var invalidColumnTypes = []string{
	"",
	" ",
	" String",
	"String ",
	"Decimal(10, 2)",
	"Nullable ( String )",
	"Int64 NOT NULL",
	"String CODEC(ZSTD)",
	"Nullable(String)DEFAULT(hostName())",
	"Array(String)DEFAULT(1)",
	"Map(String,Int64)MATERIALIZED(1)",
	"JSON, extra String DEFAULT 'x'",
	"JSON) ENGINE = MergeTree ORDER BY _id --",
	"Nullable(String",
	"Nullable(String))",
	"Nullable()",
	"Decimal(10,)",
	"Array(,String)",
	"DateTime64(6, 'UTC')",
	"Enum8('a' = 1)",
	"Tuple(a Int64)",
	"JSON(max_dynamic_paths=10)",
	"1",
	"Int64;",
	"DEFAULT(rand(1))",
	"default(1)",
	"MATERIALIZED(toString(1))",
	"ALIAS(1)",
	"EPHEMERAL(1)",
	"AUTO_INCREMENT",
	"CONSTRAINT",
	"Nullable(DEFAULT(1))",
}

var validExpressions = []string{
	"created_at",
	"toYYYYMM(created_at)",
	"toYYYYMM(created_at)  ",
	"toYYYYMM(assumeNotNull(captured_at))",
	"toDate(created_at)",
	"intDiv(block_number, 1000000)",
	"customer_id % 4",
	"num%2,val",
	"toStartOfInterval(ts, INTERVAL 1 DAY)",
	"cityHash64(id) - 1.5 * 2 / 3",
	"rand()",
	"tuple()",
	"`Weird Column`",
	`toYYYYMM("CamelCase")`,
	"`select`",
	"selected_at",
	"withdrawn_at",
}

var invalidExpressions = []string{
	"id) SETTINGS index_granularity = 1 --",
	"id) TTL toDate(ts + INTERVAL 1 DAY",
	"id) FROM (SELECT 1 AS id) UNION ALL SELECT hostName() --",
	"id)",
	"(id",
	")(",
	"id -- comment",
	"id /* comment */",
	"id # comment",
	"id; DROP TABLE t",
	"$$($$) TTL toDate(ts) + toIntervalDay($$)$$",
	"toTimeZone(ts, 'UTC')",
	"throwIf(1, 'x')",
	"(SELECT 1)",
	"(select 1)",
	"WITH 1 AS x SELECT x",
	"x IN other_db.t",
	"x AS y",
	"toInt64(x as Int64)",
	"`unterminated",
	`"unterminated`,
	"`a\\`b`",
	"x::Int64",
	"[1, 2]",
	"x = 1",
	"{param:String}",
	"x ? 1 : 2",
	"toYYYYMM(créated)",
}

func TestValidateColumnType(t *testing.T) {
	for _, columnType := range validColumnTypes {
		require.NoError(t, ValidateColumnType(columnType), columnType)
	}
	for _, columnType := range invalidColumnTypes {
		err := ValidateColumnType(columnType)
		var fragmentErr *InvalidFragmentError
		require.ErrorAs(t, err, &fragmentErr, columnType)
		require.Equal(t, "column type", fragmentErr.Kind)
	}
}

func TestValidateExpression(t *testing.T) {
	for _, expression := range validExpressions {
		require.NoError(t, ValidateExpression(expression), expression)
	}
	for _, expression := range invalidExpressions {
		err := ValidateExpression(expression)
		var fragmentErr *InvalidFragmentError
		require.ErrorAs(t, err, &fragmentErr, expression)
		require.Equal(t, "expression", fragmentErr.Kind)
	}
}

func FuzzValidateColumnType(f *testing.F) {
	for _, columnType := range append(validColumnTypes, invalidColumnTypes...) {
		f.Add(columnType)
	}
	r := fragmentRand(1)
	for range 200 {
		f.Add(randomFragment(r))
	}
	f.Fuzz(func(t *testing.T, columnType string) {
		err := ValidateColumnType(columnType)
		if err != nil {
			var fragmentErr *InvalidFragmentError
			require.ErrorAs(t, err, &fragmentErr)
			return
		}
		// Accepted types have no whitespace, nest, and can't be extended.
		require.False(t, strings.ContainsAny(columnType, " \t\n\r"), columnType)
		require.NoError(t, ValidateColumnType("Nullable("+columnType+")"))
		require.NoError(t, ValidateColumnType("Map(String,"+columnType+")"))
		for _, suffix := range []string{" DEFAULT 1", " DEFAULT(1)", " CODEC(ZSTD)", ")", ", y String", " --", ";"} {
			require.Error(t, ValidateColumnType(columnType+suffix), suffix)
		}
	})
}

func FuzzValidateExpression(f *testing.F) {
	for _, expression := range append(validExpressions, invalidExpressions...) {
		f.Add(expression)
	}
	r := fragmentRand(3)
	for range 200 {
		f.Add(randomFragment(r))
	}
	f.Fuzz(func(t *testing.T, expression string) {
		err := ValidateExpression(expression)
		if err != nil {
			var fragmentErr *InvalidFragmentError
			require.ErrorAs(t, err, &fragmentErr)
			return
		}
		// Outside quoted names: balanced parentheses, and no comments, separators or strings.
		unquoted := quotedNameRegex.ReplaceAllString(expression, "x")
		depth := 0
		for _, c := range unquoted {
			switch c {
			case '(':
				depth++
			case ')':
				depth--
				require.GreaterOrEqual(t, depth, 0, unquoted)
			}
		}
		require.Zero(t, depth, unquoted)
		for _, forbidden := range []string{"--", "/*", "#", ";", "'", "$", "\\", "`", `"`} {
			require.NotContains(t, unquoted, forbidden)
		}

		// Accepted expressions nest, and nothing after them can escape the parentheses.
		require.NoError(t, ValidateExpression("("+expression+")"))
		require.NoError(t, ValidateExpression("f("+expression+", 1)"))
		for _, suffix := range []string{")", ") --", " -- x", " /* x", " # x", ";", " 'x'", " $$x$$", " (SELECT 1)"} {
			require.Error(t, ValidateExpression(expression+suffix), suffix)
		}
	})
}

// quotedNameRegex matches quoted names as ValidateExpression accepts them, without backslashes:
// `my col` and "MyCol" match, `a\`b` doesn't.
var quotedNameRegex = regexp.MustCompile("`[^`\\\\]*`|\"[^\"\\\\]*\"")

// fragmentRand is seeded so generated fragments are the same on every run.
func fragmentRand(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // Reproducible test input, not used for security.
}

// fragmentTokens mix valid syntax with syntax the validators must reject.
var fragmentTokens = []string{
	"id", "ts", "String", "Int64", "Nullable", "Array", "Map", "Decimal", "toYYYYMM", "intDiv",
	"(", ")", "(", ")", ",", ",", " ", " ", "+", "-", "*", "/", "%", "1", "10", "2.5",
	"SETTINGS", "TTL", "ENGINE", "ORDER", "BY", "PRIMARY", "KEY", "DEFAULT", "CODEC", "MATERIALIZED",
	"INTERVAL", "DAY", "Null", "MergeTree", "AS", "FROM", "UNION", "ALL", "SELECT", "WITH",
	"`x`", "`", "\"y\"", "\"", "'x'", "'", "\\", "$$", "#", "--", "/*", "*/", ";", "=", ".", "::", "{", "}", "?", "é",
	")TTL(", ") TTL ", ")ORDER BY(", ")PRIMARY KEY(", ")SAMPLE BY(", ")SETTINGS(", "),", "),(", "DEFAULT(", ")DEFAULT(",
	"ALIAS", "EPHEMERAL", "AUTO_INCREMENT", "CONSTRAINT", "COMMENT", "STATISTICS", "NULL", "NOT", "default",
}

func randomFragment(r *rand.Rand) string {
	var b strings.Builder
	for n := 1 + r.IntN(12); n > 0; n-- {
		b.WriteString(fragmentTokens[r.IntN(len(fragmentTokens))])
	}
	return b.String()
}
