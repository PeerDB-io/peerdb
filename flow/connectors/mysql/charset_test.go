package connmysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestDecodeMySQLBytes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		charset string
		in      []byte
		want    string
	}{
		// MySQL latin1 is Windows-1252: 0x80 is the euro sign, 0xE9 is é.
		{"latin1 accented", "latin1", []byte{0x63, 0x61, 0x66, 0xE9}, "café"},
		{"latin1 euro", "latin1", []byte{0x80}, "€"},
		// gbk: 0xC4E3 0xBAC3 = 你好
		{"gbk hello", "gbk", []byte{0xC4, 0xE3, 0xBA, 0xC3}, "你好"},
		// sjis: 0x83 0x4F = グ
		{"sjis kana", "sjis", []byte{0x83, 0x4F}, "グ"},
		// euckr: 0xBEC8 0xB3E7 = 안녕
		{"euckr hangul", "euckr", []byte{0xBE, 0xC8, 0xB3, 0xE7}, "안녕"},
		// utf8mb4 passes through unchanged (nil encoding).
		{"utf8mb4 passthrough", "utf8mb4", []byte("café"), "café"},
		{"ascii passthrough", "ascii", []byte("plain"), "plain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := mysqlCharsetEncodings[tc.charset] // nil for utf8mb4/ascii -> passthrough
			got, err := decodeMySQLBytes(enc, tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestQValueFromMysqlRowEvent_Transcodes verifies that the CDC decode path applies
// the column's charset decoder
func TestQValueFromMysqlRowEvent_Transcodes(t *testing.T) {
	ev := &replication.TableMapEvent{ColumnType: []byte{mysql.MYSQL_TYPE_VARCHAR}}
	logger := log.NewStructuredLogger(nil)
	var coercionReported bool

	latin1 := []byte{0x63, 0x61, 0x66, 0xE9} // "café" in latin1

	// Without a decoder (utf8mb4 column) the bytes are reinterpreted verbatim - mojibake.
	raw, err := QValueFromMysqlRowEvent(
		ev, 0, nil, nil, types.QValueKindString, latin1, nil, logger, &coercionReported)
	require.NoError(t, err)
	require.NotEqual(t, "café", raw.(types.QValueString).Val)

	// With the latin1 decoder the value is correctly transcoded.
	decoded, err := QValueFromMysqlRowEvent(
		ev, 0, nil, nil, types.QValueKindString, latin1, mysqlCharsetEncodings["latin1"], logger, &coercionReported)
	require.NoError(t, err)
	require.Equal(t, "café", decoded.(types.QValueString).Val)
}
