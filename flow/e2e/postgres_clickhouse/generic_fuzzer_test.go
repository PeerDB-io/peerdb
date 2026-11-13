//go:build tilt

package postgres_clickhouse

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestGenericFuzzerCH_PG(t *testing.T) {
	e2eshared.RunSuite(t, e2e.SetupGenericFuzzer(e2e.SetupClickHouseSuite(t, false, func(t *testing.T) (*e2e.PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchfz_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupPostgres(t, suffix)
		return source, suffix, err
	}), e2e.GenericFuzzerConfig{Columns: pgFuzzColumns}))
}

// Dates and timestamps stay within what ClickHouse Date32 and DateTime64 can hold.
var (
	pgFuzzMinTime = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	pgFuzzMaxTime = time.Date(2261, 12, 31, 23, 59, 59, 999999000, time.UTC)
)

var pgFuzzColumns = map[string]e2e.FuzzColumn{
	"c_int2": {SourceType: "INT2 NOT NULL", Generate: func(r *rand.Rand) string {
		return edgeOr(r, []string{"-32768", "32767", "0"}, func() string { return strconv.Itoa(r.IntN(1<<16) - 1<<15) })
	}},
	"c_int4": {SourceType: "INT4", Generate: func(r *rand.Rand) string {
		return orNull(r, edgeOr(r, []string{"-2147483648", "2147483647", "0"}, func() string {
			return strconv.FormatInt(int64(int32(r.Uint32())), 10)
		}))
	}},
	"c_int8": {SourceType: "INT8 NOT NULL", Generate: func(r *rand.Rand) string {
		return edgeOr(r, []string{"-9223372036854775808", "9223372036854775807", "0"}, func() string {
			return strconv.FormatInt(int64(r.Uint64()), 10)
		})
	}},
	"c_float4": {SourceType: "FLOAT4", Generate: func(r *rand.Rand) string {
		return orNull(r, edgeOr(r, []string{"0", "-0", "3.4028235e+38", "-3.4028235e+38", "1e-45"}, func() string {
			f := math.Float32frombits(r.Uint32())
			for math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
				f = math.Float32frombits(r.Uint32())
			}
			return strconv.FormatFloat(float64(f), 'g', -1, 32)
		}))
	}},
	"c_float8": {SourceType: "FLOAT8 NOT NULL", Generate: func(r *rand.Rand) string {
		return edgeOr(r, []string{"0", "-0", "1.7976931348623157e+308", "-1.7976931348623157e+308", "5e-324"}, func() string {
			f := math.Float64frombits(r.Uint64())
			for math.IsNaN(f) || math.IsInf(f, 0) {
				f = math.Float64frombits(r.Uint64())
			}
			return strconv.FormatFloat(f, 'g', -1, 64)
		})
	}},
	"c_numeric": {SourceType: "NUMERIC(20,6)", Generate: func(r *rand.Rand) string {
		return orNull(r, edgeOr(r, []string{"0", "99999999999999.999999", "-99999999999999.999999", "0.000001"}, func() string {
			sign := ""
			if r.IntN(2) == 0 {
				sign = "-"
			}
			return fmt.Sprintf("%s%d.%06d", sign, r.Int64N(1e14), r.IntN(1e6))
		}))
	}},
	"c_bool": {SourceType: "BOOL", Generate: func(r *rand.Rand) string {
		return orNull(r, strconv.FormatBool(r.IntN(2) == 0))
	}},
	"c_text": {SourceType: "TEXT", Generate: func(r *rand.Rand) string {
		return orNull(r, textLiteral(randomText(r)))
	}},
	"c_uuid": {SourceType: "UUID NOT NULL", Generate: func(r *rand.Rand) string {
		var u uuid.UUID
		for i := range u {
			u[i] = byte(r.Uint32())
		}
		return "'" + u.String() + "'"
	}},
	"c_date": {SourceType: "DATE NOT NULL", Generate: func(r *rand.Rand) string {
		return edgeOr(r, []string{"'1900-01-01'", "'1970-01-01'", "'2261-12-31'"}, func() string {
			return "'" + randomTime(r).Format(time.DateOnly) + "'"
		})
	}},
	"c_timestamp": {SourceType: "TIMESTAMP(6)", Generate: func(r *rand.Rand) string {
		return orNull(r, edgeOr(r, []string{"'1900-01-01 00:00:00'", "'1970-01-01 00:00:00'"}, func() string {
			return "'" + randomTime(r).Format("2006-01-02 15:04:05.999999") + "'"
		}))
	}},
	"c_timestamptz": {SourceType: "TIMESTAMPTZ(6) NOT NULL", Generate: func(r *rand.Rand) string {
		return "'" + randomTime(r).Format("2006-01-02 15:04:05.999999") + "+00'"
	}},
	"c_int4_array": {SourceType: "INT4[] NOT NULL", Generate: func(r *rand.Rand) string {
		elems := make([]string, r.IntN(5))
		for i := range elems {
			elems[i] = strconv.FormatInt(int64(int32(r.Uint32())), 10)
		}
		return "'{" + strings.Join(elems, ",") + "}'"
	}},
	"c_text_array": {SourceType: "TEXT[] NOT NULL", Generate: func(r *rand.Rand) string {
		elems := make([]string, r.IntN(5))
		for i := range elems {
			elems[i] = textLiteral(randomText(r))
		}
		return "ARRAY[" + strings.Join(elems, ",") + "]::TEXT[]"
	}},
}

// edgeOr returns one of edges a quarter of the time, otherwise gen().
func edgeOr(r *rand.Rand, edges []string, gen func() string) string {
	if r.IntN(4) == 0 {
		return edges[r.IntN(len(edges))]
	}
	return gen()
}

// orNull returns NULL a quarter of the time, otherwise literal.
func orNull(r *rand.Rand, literal string) string {
	if r.IntN(4) == 0 {
		return "NULL"
	}
	return literal
}

// textRunes is the alphabet for randomText. It includes values that
// are more likely to trigger edge cases:
//   - abcXYZ019: plain ASCII letters in both cases and digits, as a baseline
//   - space, _ and -: separators that trimming or identifier handling may change
//   - ' and ": SQL and JSON quote characters; ' must be doubled in the source literal
//   - \: the escape character of JSON, ClickHouse strings and Postgres E” literals
//   - %: the LIKE wildcard and the printf verb prefix
//   - \n, \t and \r: control characters that JSON must escape and line-based formats split on
//   - é and ß: 2-byte UTF-8 runes; ß also changes length when upper-cased
//   - 漢字: 3-byte UTF-8 runes from the CJK range
//   - 🙂: a 4-byte UTF-8 rune outside the BMP, a surrogate pair in UTF-16
var textRunes = []rune("abcXYZ019 _-'\"\\%\n\t\réß漢字🙂")

func randomText(r *rand.Rand) string {
	var sb strings.Builder
	for range r.IntN(40) {
		if r.IntN(4) == 0 {
			sb.WriteRune(rune(1 + r.IntN(0xD7FF))) // any BMP code point below the surrogates, NUL excluded
		} else {
			sb.WriteRune(textRunes[r.IntN(len(textRunes))])
		}
	}
	return sb.String()
}

// textLiteral quotes s as a standard SQL string literal; with standard_conforming_strings on, only ' needs escaping.
func textLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func randomTime(r *rand.Rand) time.Time {
	span := pgFuzzMaxTime.UnixMicro() - pgFuzzMinTime.UnixMicro()
	return time.UnixMicro(pgFuzzMinTime.UnixMicro() + r.Int64N(span)).UTC()
}
