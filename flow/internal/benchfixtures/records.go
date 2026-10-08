package benchfixtures

import (
	"crypto/md5" //nolint:gosec // Deterministic benchmark data, not security.
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Preserve the supplied source types, name lengths, nullability and primary
// key shape without retaining production names. Values are always non-null.
var fields = []struct {
	kind     types.QValueKind
	nameLen  int
	nullable bool
}{
	{types.QValueKindInt64, 5, false},
	{types.QValueKindInt64, 5, false},
	{types.QValueKindInt64, 4, false},
	{types.QValueKindInt32, 11, false},
	{types.QValueKindInt32, 11, false},
	{types.QValueKindTimestamp, 17, false},
	{types.QValueKindInt16, 16, false},
	{types.QValueKindInt16, 6, false},
	{types.QValueKindFloat32, 9, false},
	{types.QValueKindInt32, 14, false},
	{types.QValueKindInt32, 15, false},
	{types.QValueKindInt32, 3, false},
	{types.QValueKindInt16, 3, false},
	{types.QValueKindInt16, 3, false},
	{types.QValueKindInt16, 3, false},
	{types.QValueKindInt16, 10, false},
	{types.QValueKindInt16, 12, true},
	{types.QValueKindInt16, 11, true},
	{types.QValueKindInt16, 9, true},
	{types.QValueKindString, 6, true},
	{types.QValueKindJSON, 8, true},
	{types.QValueKindInt16, 10, true},
	{types.QValueKindInt16, 10, true},
	{types.QValueKindInt32, 9, true},
	{types.QValueKindInt16, 5, true},
	{types.QValueKindTimestamp, 10, false},
	{types.QValueKindTimestamp, 10, false},
}

func Names() []string {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = fmt.Sprintf("c%02d", i) + strings.Repeat("x", field.nameLen-3)
	}
	return names
}

// Record has the supplied schema with approximately 500-byte pgoutput events.
func Record(row int) model.RecordItems {
	r := model.NewRecordItems(len(fields))
	//nolint:gosec // Matches PostgreSQL md5() in the deterministic fixture.
	hash := fmt.Sprintf("%x", md5.Sum(fmt.Appendf(nil, "%d", row)))
	names := Names()
	for i, field := range fields {
		var value types.QValue
		switch field.kind {
		case types.QValueKindInt64:
			n := int64(row)*100003 + int64(i)
			if i == 0 {
				n = int64(row)
			}
			value = types.QValueInt64{Val: n}
		case types.QValueKindInt32:
			value = types.QValueInt32{Val: int32((row*17 + i) % 100000)}
		case types.QValueKindInt16:
			value = types.QValueInt16{Val: int16((row + i) % 100)}
		case types.QValueKindTimestamp:
			value = types.QValueTimestamp{Val: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC).Add(time.Duration(row) * time.Microsecond)}
		case types.QValueKindFloat32:
			value = types.QValueFloat32{Val: float32(row%1000) / 100}
		case types.QValueKindString:
			value = types.QValueString{Val: strings.Repeat(hash, 5)[:137] + "<>&雪"}
		case types.QValueKindJSON:
			value = types.QValueJSON{Val: fmt.Sprintf(`{"n": %d, "tag": "%s"}`, row%1000, hash[:8])}
		default:
			panic("unsupported benchmark field")
		}
		r.AddColumn(names[i], value)
	}
	return r
}

// Postgres describes identical values for a generate_series(... ) g.
func Postgres() ([]string, []string) {
	var columns, values []string
	names := Names()
	for i, field := range fields {
		var typ, expr string
		switch field.kind {
		case types.QValueKindInt64:
			typ, expr = "BIGINT", fmt.Sprintf("g::bigint*100003+%d", i)
			if i == 0 {
				expr = "g"
			}
		case types.QValueKindInt32:
			typ, expr = "INTEGER", fmt.Sprintf("(g::bigint*17+%d)%%100000", i)
		case types.QValueKindInt16:
			typ, expr = "SMALLINT", fmt.Sprintf("(g+%d)%%100", i)
		case types.QValueKindTimestamp:
			typ, expr = "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP '2026-10-08 00:00:00' + g * INTERVAL '1 microsecond'"
		case types.QValueKindFloat32:
			typ, expr = "REAL", "(g%1000)::real/100::real"
		case types.QValueKindString:
			typ, expr = "TEXT", "left(repeat(md5(g::text),5),137)||'<>&雪'"
		case types.QValueKindJSON:
			typ, expr = "JSONB", "jsonb_build_object('n',g%1000,'tag',left(md5(g::text),8))"
		default:
			panic("unsupported benchmark field")
		}
		if !field.nullable {
			typ += " NOT NULL"
		}
		columns = append(columns, names[i]+" "+typ)
		values = append(values, expr)
	}
	columns = append(columns, "PRIMARY KEY ("+names[0]+","+names[5]+")")
	return columns, values
}
