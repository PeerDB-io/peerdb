package connmysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TestQkindFromMysqlColumnTypeMariaDB covers the MariaDB-only types as they appear in
// information_schema (the snapshot/QRep path), which is how these columns are typed
// since the DDL parser rejects them.
func TestQkindFromMysqlColumnTypeMariaDB(t *testing.T) {
	for _, tc := range []struct {
		dataType string
		want     types.QValueKind
	}{
		{"uuid", types.QValueKindUUID},
		{"inet4", types.QValueKindINET},
		{"inet6", types.QValueKindINET},
	} {
		t.Run(tc.dataType, func(t *testing.T) {
			qkind, err := QkindFromMysqlColumnType(tc.dataType, true, shared.InternalVersion_Latest)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
		})
	}
}

func TestQkindFromMysqlColumnTypeCompressed(t *testing.T) {
	for _, ct := range []string{
		"varchar(100) /*M!100301 COMPRESSED*/",
		"text /*M!100301 COMPRESSED*/",
		"blob /*M!100301 COMPRESSED*/",
	} {
		t.Run(ct, func(t *testing.T) {
			_, err := QkindFromMysqlColumnType(ct, true, shared.InternalVersion_Latest)
			require.ErrorContains(t, err, "COMPRESSED")
		})
	}
}

func TestQkindFromMysqlType_Bit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint32
		want    types.QValueKind
	}{
		{"first version maps BIT to Int64", shared.InternalVersion_First, types.QValueKindInt64},
		{
			"version just before the gate maps BIT to Int64",
			shared.InternalVersion_MySQLConvertBitToUInt64 - 1,
			types.QValueKindInt64,
		},
		{
			"gating version maps BIT to UInt64",
			shared.InternalVersion_MySQLConvertBitToUInt64,
			types.QValueKindUInt64,
		},
		{"latest version maps BIT to UInt64", shared.InternalVersion_Latest, types.QValueKindUInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qkind, err := qkindFromMysqlType(mysql.MYSQL_TYPE_BIT, false, 0, tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
		})
	}
}

func TestQkindFromMysqlColumnType_Set(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata bool
		version  uint32
		want     types.QValueKind
	}{
		{"with metadata maps SET to String", true, shared.InternalVersion_Latest, types.QValueKindString},
		{"without metadata, before gate maps SET to String", false, shared.InternalVersion_MySQL5ConvertSetsToInts - 1, types.QValueKindString},
		{"without metadata, gate maps SET to Uint64Set", false, shared.InternalVersion_MySQL5ConvertSetsToInts, types.QValueKindUint64Set},
		{"without metadata, latest maps SET to Uint64Set", false, shared.InternalVersion_Latest, types.QValueKindUint64Set},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qkind, err := QkindFromMysqlColumnType("set('a','b','c')", tc.metadata, tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
		})
	}
}

func TestProcessTime(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	for _, ts := range []struct {
		out time.Duration
		in  string
	}{
		{time.Date(1970, 1, 1, 23, 30, 0, 500000000, time.UTC).Sub(epoch), "23:30.5"},
		{time.Date(1970, 2, 3, 8, 0, 1, 0, time.UTC).Sub(epoch), "800:0:1"},
		{time.Date(1969, 11, 28, 15, 59, 59, 0, time.UTC).Sub(epoch), "-800:0:1"},
		{time.Date(1969, 11, 28, 15, 59, 58, 900000000, time.UTC).Sub(epoch), "-800:0:1.1"},
		{time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC).Sub(epoch), "1."},
		{time.Date(1970, 1, 1, 0, 12, 34, 0, time.UTC).Sub(epoch), "1234"},
		{time.Date(1970, 1, 1, 3, 12, 34, 0, time.UTC).Sub(epoch), "31234"},
		{time.Date(1970, 1, 1, 0, 0, 1, 120000000, time.UTC).Sub(epoch), "1.12"},
		{time.Date(1970, 1, 1, 0, 0, 1, 12000000, time.UTC).Sub(epoch), "1.012"},
		{time.Date(1970, 1, 1, 0, 0, 1, 12300000, time.UTC).Sub(epoch), "1.0123"},
		{time.Date(1970, 1, 1, 0, 0, 1, 1230000, time.UTC).Sub(epoch), "1.00123"},
		{time.Date(1970, 1, 1, 0, 0, 1, 1000, time.UTC).Sub(epoch), "1.000001"},
		{time.Date(1970, 1, 1, 0, 0, 1, 200, time.UTC).Sub(epoch), "1.0000002"},
		{time.Date(1970, 1, 1, 0, 0, 1, 30, time.UTC).Sub(epoch), "1.00000003"},
		{time.Date(1970, 1, 1, 0, 0, 1, 4, time.UTC).Sub(epoch), "1.000000004"},
		{0, "123.aa"},
		{0, "hh:00:00"},
		{0, "00:mm:00"},
		{0, "00:00:ss"},
		{0, "hh:00"},
		{0, "00:mm"},
		{0, "ss"},
		{0, "mm00"},
		{0, "00ss"},
		{0, "hh0000"},
		{0, "00mm00"},
		{0, "0000ss"},
	} {
		tm, err := processTime(ts.in)
		if tm == 0 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, ts.out, tm)
	}
}

func TestCompactMySQLJSON(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		// MySQL server-rendered text: space after ':' and ','.
		{`{"a": 1, "b": 2}`, `{"a":1,"b":2}`},
		// DOUBLE keeps its trailing zero; it is not renumbered.
		{`{"x": 1.0}`, `{"x":1.0}`},
		// Object key order is preserved (not lexicographically sorted).
		{`{"b": 1, "a": 2}`, `{"b":1,"a":2}`},
		// Nested arrays/objects.
		{`{"arr": [1, 2, {"k": "v"}]}`, `{"arr":[1,2,{"k":"v"}]}`},
		// Whitespace inside string values is untouched.
		{`{"s": "a, b: c"}`, `{"s":"a, b: c"}`},
		{`{}`, `{}`},
		{`[]`, `[]`},
	} {
		require.Equal(t, tc.want, compactMySQLJSON([]byte(tc.in)), "in=%q", tc.in)
	}

	// Invalid JSON falls back to the original bytes rather than dropping the value.
	require.Equal(t, `not json`, compactMySQLJSON([]byte(`not json`)))
}
