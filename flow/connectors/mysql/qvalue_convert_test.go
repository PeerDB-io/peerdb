package connmysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

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
		name                    string
		binlogMetadataSupported bool
		version                 uint32
		want                    types.QValueKind
	}{
		{
			name:                    "old version without metadata maps SET to string",
			binlogMetadataSupported: false,
			version:                 shared.InternalVersion_MySQL5ConvertSetsToInts - 1,
			want:                    types.QValueKindString,
		},
		{
			name:                    "gating version without metadata maps SET to Uint64Set",
			binlogMetadataSupported: false,
			version:                 shared.InternalVersion_MySQL5ConvertSetsToInts,
			want:                    types.QValueKindUint64Set,
		},
		{
			name:                    "latest version without metadata maps SET to Uint64Set",
			binlogMetadataSupported: false,
			version:                 shared.InternalVersion_Latest,
			want:                    types.QValueKindUint64Set,
		},
		{
			name:                    "metadata-supported server keeps SET as string labels",
			binlogMetadataSupported: true,
			version:                 shared.InternalVersion_Latest,
			want:                    types.QValueKindString,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qkind, err := QkindFromMysqlColumnType("set('a','b','c')", tc.binlogMetadataSupported, tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
		})
	}
}

func TestQValueFromMysqlFieldValue_Uint64Set(t *testing.T) {
	const maxUint64 = ^uint64(0)

	for _, value := range []uint64{5, maxUint64} {
		fieldValue := mysql.NewFieldValue(mysql.FieldValueTypeUnsigned, value, nil)

		qv, err := QValueFromMysqlFieldValue(types.QValueKindUint64Set, mysql.MYSQL_TYPE_SET, fieldValue)
		require.NoError(t, err)
		require.Equal(t, types.QValueUint64Set{Val: value}, qv)
	}
}

func TestQValueFromMysqlRowEvent_Uint64Set(t *testing.T) {
	tableMap := &replication.TableMapEvent{ColumnType: []byte{mysql.MYSQL_TYPE_SET}}
	coercionReported := false

	for _, tc := range []struct {
		name  string
		input any
		want  uint64
	}{
		{name: "small signed bitmask", input: int64(5), want: 5},
		{name: "max signed bitmask", input: int64(-1), want: ^uint64(0)},
		{name: "max unsigned bitmask", input: ^uint64(0), want: ^uint64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qv, err := QValueFromMysqlRowEvent(
				tableMap, 0, nil, nil, types.QValueKindUint64Set, tc.input, nil, &coercionReported)
			require.NoError(t, err)
			require.Equal(t, types.QValueUint64Set{Val: tc.want}, qv)
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
