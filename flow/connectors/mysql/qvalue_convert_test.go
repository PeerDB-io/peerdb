package connmysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
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

// TestDecodeMariaDBUUID verifies the binlog bytes are read in plain network byte order
// (no group reordering), matching MariaDB's wire format as confirmed by
// Test_MySQL_MariaDB_UUID_INET against a real server.
func TestDecodeMariaDBUUID(t *testing.T) {
	// canonical bytes, same order as the textual representation
	stored := []byte{
		0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26,
		0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24, 0xdb,
	}
	got, err := decodeMariaDBUUID(stored)
	require.NoError(t, err)
	require.Equal(t, "6ccd780c-baba-1026-9564-5b8c656024db", got.String())
	require.Equal(t, uuid.MustParse("6ccd780c-baba-1026-9564-5b8c656024db"), got)

	_, err = decodeMariaDBUUID([]byte{0x00, 0x01})
	require.Error(t, err)
}

func TestFormatMariaDBInet(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{"ipv4", []byte{192, 168, 0, 1}, "192.168.0.1"},
		{"ipv4 zero", []byte{0, 0, 0, 0}, "0.0.0.0"},
		{"ipv6 loopback", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, "::1"},
		{
			"ipv6 full",
			[]byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01},
			"2001:db8::1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := formatMariaDBInet(tc.data)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	_, err := formatMariaDBInet([]byte{1, 2, 3})
	require.Error(t, err)
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
