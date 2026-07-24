package connmysql

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

const (
	maxRows       = 100
	numPartitions = 8
)

// fuzzCharsetEncodings maps each MySQL charset to a codec used to turn
// arbitrary bytes into characters of the given charset. A nil codec means
// bytes are coerced directly into valid bytes of the charset.
var fuzzCharsetEncodings = map[string]encoding.Encoding{
	"ascii":    nil,
	"latin1":   nil,
	"utf8mb4":  nil,
	"gbk":      simplifiedchinese.GBK,
	"gb2312":   simplifiedchinese.GBK,
	"gb18030":  simplifiedchinese.GB18030,
	"big5":     traditionalchinese.Big5,
	"sjis":     japanese.ShiftJIS,
	"cp932":    japanese.ShiftJIS,
	"ujis":     japanese.EUCJP,
	"eucjpms":  japanese.EUCJP,
	"euckr":    korean.EUCKR,
	"tis620":   charmap.Windows874,
	"latin2":   charmap.ISO8859_2,
	"latin5":   charmap.ISO8859_9,
	"latin7":   charmap.ISO8859_13,
	"cp1250":   charmap.Windows1250,
	"cp1251":   charmap.Windows1251,
	"cp1256":   charmap.Windows1256,
	"cp1257":   charmap.Windows1257,
	"greek":    charmap.ISO8859_7,
	"hebrew":   charmap.ISO8859_8,
	"koi8r":    charmap.KOI8R,
	"koi8u":    charmap.KOI8U,
	"cp866":    charmap.CodePage866,
	"cp850":    charmap.CodePage850,
	"cp852":    charmap.CodePage852,
	"macroman": charmap.Macintosh,
	"utf16":    unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
	"utf16le":  unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM),
	"utf32":    unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
	"ucs2":     unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
	"utf8mb3":  unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
}

// FuzzMySQLStringPartitionCoverage exercises the arbitrary-string parallel
// snapshot path against a real MySQL server across many charsets, collations,
// paddings, and key byte patterns; and ensures every row of the table is covered
// exactly once by the partitions. Test requires PeerDB catalog and MySQL to be
// running. Example:
//
//	go test ./connectors/mysql/ -run '^$' -fuzz '^FuzzMySQLStringPartitionCoverage$' -fuzztime 1h
func FuzzMySQLStringPartitionCoverage(f *testing.F) {
	ctx := context.Background()
	conn, db := setupFuzzMySQL(f, ctx)
	collationsByCharset, charsets := fetchServerCollations(f, ctx, conn)
	addKeySeeds(f, len(charsets))

	f.Fuzz(func(t *testing.T, raw []byte, charsetSelector, collationSelector uint8) {
		charsetName := charsets[int(charsetSelector)%len(charsets)]
		supported := collationsByCharset[charsetName]
		collationName := supported[int(collationSelector)%len(supported)]
		encoding := fuzzCharsetEncodings[charsetName]

		keys := decodeKeysFromRawBytes(raw, charsetName, encoding)
		if len(keys) < 2 {
			t.Skipf("need at least two distinct candidate keys")
		}

		qualifiedTbl := createTable(t, ctx, conn, db, charsetName, collationName)
		insertKeys(t, ctx, conn, qualifiedTbl, keys, encoding)

		allRows := selectRows(t, ctx, conn, qualifiedTbl, "")
		if len(allRows) < 2 {
			t.Skipf("fewer than two rows")
		}

		minVal, maxVal := fetchMinMaxKey(t, ctx, conn, qualifiedTbl)
		partitions, err := buildAdaptiveStringPartitions(ctx, conn, conn.logger, qualifiedTbl, "id", minVal, maxVal, numPartitions)
		require.NoError(t, err)

		assertExactlyOnceCoverage(t, ctx, conn, qualifiedTbl, partitions, allRows, collationName)
	})
}

func setupFuzzMySQL(f *testing.F, ctx context.Context) (*MySqlConnector, string) {
	f.Helper()
	config := internal.GetMySQLConfigFromEnv(
		protos.MySqlFlavor_MYSQL_MYSQL, protos.MySqlReplicationMechanism_MYSQL_GTID)
	conn, err := NewMySqlConnector(ctx, config)
	if err != nil {
		f.Skipf("mysql integration environment not available: %v", err)
	}
	f.Cleanup(func() { _ = conn.Close() })

	db := testDBName("fuzz")
	_, err = conn.Execute(ctx, "CREATE DATABASE IF NOT EXISTS "+db)
	require.NoError(f, err)

	f.Cleanup(func() {
		_, _ = conn.Execute(context.Background(), "DROP DATABASE IF EXISTS "+db)
	})
	return conn, db
}

func fetchServerCollations(f *testing.F, ctx context.Context, conn *MySqlConnector) (map[string][]string, []string) {
	f.Helper()
	collationsByCharset := make(map[string][]string)
	rs, err := conn.Execute(ctx, "SHOW COLLATION")
	require.NoError(f, err)
	for i := range rs.RowNumber() {
		collationName, err := rs.GetString(i, 0)
		require.NoError(f, err)
		charsetName, err := rs.GetString(i, 1)
		require.NoError(f, err)
		charsetName = strings.Clone(charsetName)
		collationsByCharset[charsetName] = append(collationsByCharset[charsetName], strings.Clone(collationName))
	}
	rs.Close()
	for _, list := range collationsByCharset {
		slices.Sort(list)
	}

	var charsets []string
	for cs := range collationsByCharset {
		if _, ok := fuzzCharsetEncodings[cs]; ok {
			charsets = append(charsets, cs)
		}
	}
	slices.Sort(charsets)
	return collationsByCharset, charsets
}

// addKeySeeds gives the fuzzer one starting input per charset. Each seed
// packs maxRows length-prefixed pseudo-random keys, which will later be
// decoded into keys in the selected charset.
func addKeySeeds(f *testing.F, numCharsets int) {
	f.Helper()
	for idx := range numCharsets {
		//nolint:gosec // not security-sensitive
		rng := rand.New(rand.NewPCG(uint64(idx), 0))
		var raw []byte
		for range maxRows {
			// key length has log-uniform distribution to mimic realistic pks,
			// so most primary keys are not too long
			l := max(1, rng.IntN(1<<rng.IntN(9)))
			raw = append(raw, byte(l))
			// append bytes of length l, what the bytes mean is
			// decided later by fuzzCoerceForCharset
			for range l {
				raw = append(raw, byte(rng.UintN(256)))
			}
		}
		f.Add(raw, uint8(idx), uint8(idx))
	}
}

func createTable(t *testing.T, ctx context.Context, conn *MySqlConnector, db, charsetName, collationName string) *common.QualifiedTable {
	t.Helper()
	tbl := &common.QualifiedTable{Namespace: db, Table: "t_" + strings.ToLower(common.RandomString(8))}
	if _, err := conn.Execute(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id VARCHAR(255) CHARACTER SET %s COLLATE %s PRIMARY KEY)",
		tbl.MySQL(), charsetName, collationName)); err != nil {
		t.Skipf("create table (%s/%s): %v", charsetName, collationName, err)
	}
	t.Cleanup(func() { _, _ = conn.Execute(context.Background(), "DROP TABLE IF EXISTS "+tbl.MySQL()) })
	return tbl
}

func insertKeys(t *testing.T, ctx context.Context, conn *MySqlConnector, tbl *common.QualifiedTable, keys []string, enc encoding.Encoding) {
	t.Helper()
	vals := make([]string, len(keys))
	for i, k := range keys {
		// X'...' inserts arbitrary key bytes without escaping; codec-mode keys
		// are UTF-8 in Go, so CONVERT has the server transcode them into the column charset.
		hexStr := "X'" + hex.EncodeToString([]byte(k)) + "'"
		if enc != nil {
			hexStr = "CONVERT(" + hexStr + " USING utf8mb4)"
		}
		vals[i] = hexStr
	}

	if _, err := conn.Execute(ctx,
		// INSERT IGNORE tolerates collation-equal duplicates
		fmt.Sprintf("INSERT IGNORE INTO %s (id) VALUES (%s)", tbl.MySQL(), strings.Join(vals, "),(")),
	); err != nil {
		t.Skipf("insert: %v", err)
	}
}

func fetchMinMaxKey(t *testing.T, ctx context.Context, conn *MySqlConnector, tbl *common.QualifiedTable) (string, string) {
	t.Helper()
	rs, err := conn.Execute(ctx, "SELECT MIN(id),MAX(id) FROM "+tbl.MySQL())
	require.NoError(t, err)
	defer rs.Close()
	minVal, err := rs.GetString(0, 0)
	require.NoError(t, err)
	maxVal, err := rs.GetString(0, 1)
	require.NoError(t, err)
	// clone because GetString is zero-copy over the row buffer recycled on the next query
	return strings.Clone(minVal), strings.Clone(maxVal)
}

func assertExactlyOnceCoverage(
	t *testing.T,
	ctx context.Context,
	conn *MySqlConnector,
	tbl *common.QualifiedTable,
	partitions []*protos.QRepPartition,
	allRows map[string]struct{},
	collationName string,
) {
	t.Helper()
	covered := make(map[string]int, len(allRows))
	for _, p := range partitions {
		sr := p.GetRange().GetStringRange()
		require.NotNil(t, sr)
		upperOp := "<"
		if sr.EndInclusive {
			upperOp = "<="
		}
		rangeSQL := fmt.Sprintf(
			" WHERE id >= '%s' AND id %s '%s'",
			escapeWithNoBackslashEscapes(sr.Start), upperOp, escapeWithNoBackslashEscapes(sr.End))
		for id := range selectRows(t, ctx, conn, tbl, rangeSQL) {
			covered[id]++
		}
	}

	for id := range allRows {
		require.Equalf(t, 1, covered[id],
			"key 0x%s covered %d times, want exactly 1 (collation=%s)", id, covered[id], collationName)
	}
	require.Lenf(t, covered, len(allRows),
		"partitions covered %d distinct keys, table has %d", len(covered), len(allRows))
}

func decodeKeysFromRawBytes(raw []byte, charset string, encoding encoding.Encoding) []string {
	var keys []string
	seen := make(map[string]struct{})
	for i := 0; i < len(raw) && len(keys) < maxRows; {
		n := int(raw[i])
		i++
		n = min(n, len(raw)-i)
		k := fuzzCoerceForCharset(raw[i:i+n], charset, encoding)
		i += n
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	return keys
}

// fuzzCoerceForCharset turns arbitrary fuzz bytes into a key in the target charset:
// when codec is available, it decodes the bytes through the charset's codec;
// otherwise, it coerces the bytes directly into valid encoding units.
func fuzzCoerceForCharset(raw []byte, charset string, enc encoding.Encoding) string {
	if enc != nil {
		decoded, err := enc.NewDecoder().Bytes(raw)
		if err != nil {
			return ""
		}
		// undecodable byte sequences become � (U+FFFD), which most legacy
		// charsets cannot store; scrub it so the insert doesn't fail on it
		return strings.ReplaceAll(string(decoded), "�", "")
	}
	switch charset {
	case "ascii":
		out := make([]byte, len(raw))
		for i, c := range raw {
			out[i] = c & 0x7f
		}
		return string(out)
	case "utf8mb4":
		return strings.ToValidUTF8(string(raw), "")
	case "latin1":
		return string(raw)
	default:
		panic("unknown charset: " + charset)
	}
}

func selectRows(t *testing.T, ctx context.Context, c *MySqlConnector, tbl *common.QualifiedTable, whereFilter string) map[string]struct{} {
	t.Helper()
	// use hex for byte-exact comparison
	rs, err := c.Execute(ctx, "SELECT HEX(id) FROM "+tbl.MySQL()+whereFilter)
	require.NoError(t, err)
	defer rs.Close()
	out := make(map[string]struct{}, rs.RowNumber())
	for i := range rs.RowNumber() {
		h, err := rs.GetString(i, 0)
		require.NoError(t, err)
		out[strings.Clone(h)] = struct{}{}
	}
	return out
}
