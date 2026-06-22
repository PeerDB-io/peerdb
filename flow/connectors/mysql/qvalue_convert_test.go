package connmysql

import (
	"log/slog"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	temporallog "go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
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

func TestQValueFromMysqlRowEventJsonDiffErrors(t *testing.T) {
	logger := temporallog.NewStructuredLogger(slog.New(slog.DiscardHandler))
	ev := &replication.TableMapEvent{
		Schema:     []byte("test_db"),
		Table:      []byte("test_table"),
		ColumnType: []byte{mysql.MYSQL_TYPE_JSON},
		ColumnMeta: []uint16{0},
		ColumnName: [][]byte{[]byte("doc")},
	}
	var coercionReported bool

	_, err := QValueFromMysqlRowEvent(
		ev, 0, nil, nil, types.QValueKindJSON,
		&replication.JsonDiff{Op: replication.JsonDiffOperationReplace, Path: "$.a", Value: `"b"`},
		logger, &coercionReported,
	)
	require.Error(t, err)

	var partialJSONErr *exceptions.MySQLPartialJSONUnsupportedError
	require.ErrorAs(t, err, &partialJSONErr)
	require.Equal(t, "test_db", partialJSONErr.SchemaName)
	require.Equal(t, "test_table", partialJSONErr.TableName)
	require.Equal(t, "doc", partialJSONErr.ColumnName)
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
