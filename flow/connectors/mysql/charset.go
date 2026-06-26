package connmysql

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
	"golang.org/x/text/transform"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
)

// charsetsNoTranscode lists the MySQL character sets whose stored bytes are
// already valid UTF-8 (or are opaque binary)
var charsetsNoTranscode = map[string]struct{}{
	"utf8":    {},
	"utf8mb3": {},
	"utf8mb4": {},
	"ascii":   {},
	"binary":  {},
}

// mysqlCharsetEncodings maps a MySQL character set name to the golang.org/x/text
var mysqlCharsetEncodings = map[string]encoding.Encoding{
	// Single-byte / Windows & ISO code pages.
	"latin1":   charmap.Windows1252,
	"latin2":   charmap.ISO8859_2,
	"latin5":   charmap.ISO8859_9,
	"latin7":   charmap.ISO8859_13,
	"cp1250":   charmap.Windows1250,
	"cp1251":   charmap.Windows1251,
	"cp1256":   charmap.Windows1256,
	"cp1257":   charmap.Windows1257,
	"cp850":    charmap.CodePage850,
	"cp852":    charmap.CodePage852,
	"cp866":    charmap.CodePage866,
	"koi8r":    charmap.KOI8R,
	"koi8u":    charmap.KOI8U,
	"greek":    charmap.ISO8859_7,
	"hebrew":   charmap.ISO8859_8,
	"tis620":   charmap.Windows874,
	"macroman": charmap.Macintosh,

	// Multi-byte CJK code pages.
	"gbk":     simplifiedchinese.GBK,
	"gb2312":  simplifiedchinese.GBK, // GBK is a strict superset of GB2312/EUC-CN
	"gb18030": simplifiedchinese.GB18030,
	"big5":    traditionalchinese.Big5,
	"sjis":    japanese.ShiftJIS,
	"cp932":   japanese.ShiftJIS, // cp932 is a near-superset of Shift-JIS
	"ujis":    japanese.EUCJP,
	"eucjpms": japanese.EUCJP,
	"euckr":   korean.EUCKR,

	// Wide Unicode encodings.
	"utf16":   unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
	"utf16le": unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM),
	"ucs2":    unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
	"utf32":   utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM),
}

type tableMapCollationType uint8

const (
	tableMapCharacterCollation tableMapCollationType = iota
	tableMapEnumSetCollation
)

// collationEncoding resolves a MySQL collation id (as carried in binlog
// TABLE_MAP metadata) to the x/text encoding needed to convert that column's
// bytes to UTF-8.
func (c *MySqlConnector) collationEncoding(
	ctx context.Context, collationID uint64, otelManager *otel_metrics.OtelManager,
) (encoding.Encoding, error) {
	if collationID == 0 {
		return nil, nil
	}

	recordUsedCharsetMetrics := func(ctx context.Context, charset string, status string) {
		otelManager.Metrics.UsedMySQLCharsetsCounter.Add(ctx, 1,
			metric.WithAttributeSet(attribute.NewSet(
				attribute.String("charset", charset),
				attribute.String("status", status),
			)),
		)
	}

	charset, err := c.charsetForCollation(ctx, collationID)
	if err != nil {
		return nil, err
	}
	if charset == "" {
		c.warnCharsetOnce(fmt.Sprintf("collation:%d", collationID), func() {
			c.logger.Warn("unknown MySQL collation id on CDC path, passing bytes through untranscoded",
				slog.Uint64("collationID", collationID))
		})
		return nil, nil
	}

	if _, skip := charsetsNoTranscode[charset]; skip {
		recordUsedCharsetMetrics(ctx, charset, "not_transcoded")
		return nil, nil
	}
	if enc, ok := mysqlCharsetEncodings[charset]; ok {
		recordUsedCharsetMetrics(ctx, charset, "transcoded")
		return enc, nil
	}

	recordUsedCharsetMetrics(ctx, charset, "unsupported")
	c.warnCharsetOnce(charset, func() {
		c.logger.Warn("unsupported MySQL character set on CDC path, passing bytes through untranscoded",
			slog.String("charset", charset), slog.Uint64("collationID", collationID))
	})

	return nil, nil
}

func (c *MySqlConnector) tableMapColumnEncodings(
	ctx context.Context,
	tableMap *replication.TableMapEvent,
	enumMap map[int][]string,
	setMap map[int][]string,
	otelManager *otel_metrics.OtelManager,
) ([]encoding.Encoding, error) {
	colEncodings, err := c.appendTableMapColumnEncodings(
		ctx, otelManager,
		tableMap, tableMapCharacterCollation, tableMap.DefaultCharset, tableMap.ColumnCharset,
		nil, nil, nil,
	)
	if err != nil {
		return nil, err
	}
	colEncodings, err = c.appendTableMapColumnEncodings(
		ctx, otelManager,
		tableMap, tableMapEnumSetCollation, tableMap.EnumSetDefaultCharset, tableMap.EnumSetColumnCharset,
		enumMap, setMap, colEncodings,
	)
	if err != nil {
		return nil, err
	}
	return colEncodings, nil
}

func (c *MySqlConnector) appendTableMapColumnEncodings(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	tableMap *replication.TableMapEvent,
	collationType tableMapCollationType,
	defaultCharset []uint64,
	columnCharset []uint64,
	enumMap map[int][]string,
	setMap map[int][]string,
	colEncodings []encoding.Encoding,
) ([]encoding.Encoding, error) {
	if len(defaultCharset) != 0 {
		if len(defaultCharset)%2 == 0 {
			return nil, fmt.Errorf("malformed TABLE_MAP default charset metadata: expected odd item count, got %d", len(defaultCharset))
		}

		defaultCollation := defaultCharset[0]
		collationPos := 0
		for colIdx := range int(tableMap.ColumnCount) {
			if !tableMapIncludesCollation(tableMap, collationType, colIdx) {
				continue
			}

			collationID := tableMapDefaultCollation(defaultCharset, collationPos, defaultCollation)
			var err error
			colEncodings, err = c.appendTableMapColumnEncoding(
				ctx, otelManager, tableMap, collationType, colIdx, collationID, enumMap, setMap, colEncodings,
			)
			if err != nil {
				return nil, err
			}
			collationPos++
		}
		return colEncodings, nil
	}

	if len(columnCharset) != 0 {
		collationPos := 0
		for colIdx := range int(tableMap.ColumnCount) {
			if !tableMapIncludesCollation(tableMap, collationType, colIdx) {
				continue
			}
			if collationPos >= len(columnCharset) {
				return nil, fmt.Errorf(
					"malformed TABLE_MAP column charset metadata: got %d collations, missing collation for column %d",
					len(columnCharset), colIdx,
				)
			}

			var err error
			colEncodings, err = c.appendTableMapColumnEncoding(
				ctx, otelManager, tableMap, collationType, colIdx, columnCharset[collationPos],
				enumMap, setMap, colEncodings,
			)
			if err != nil {
				return nil, err
			}
			collationPos++
		}
	}
	return colEncodings, nil
}

func tableMapDefaultCollation(defaultCharset []uint64, collationPos int, defaultCollation uint64) uint64 {
	for idx := 1; idx+1 < len(defaultCharset); idx += 2 {
		if defaultCharset[idx] == uint64(collationPos) {
			return defaultCharset[idx+1]
		}
	}
	return defaultCollation
}

func tableMapIncludesCollation(
	tableMap *replication.TableMapEvent,
	collationType tableMapCollationType,
	colIdx int,
) bool {
	switch collationType {
	case tableMapCharacterCollation:
		return tableMap.IsCharacterColumn(colIdx)
	case tableMapEnumSetCollation:
		return tableMap.IsEnumOrSetColumn(colIdx)
	default:
		return false
	}
}

func (c *MySqlConnector) appendTableMapColumnEncoding(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	tableMap *replication.TableMapEvent,
	collationType tableMapCollationType,
	colIdx int,
	collationID uint64,
	enumMap map[int][]string,
	setMap map[int][]string,
	colEncodings []encoding.Encoding,
) ([]encoding.Encoding, error) {
	enc, err := c.collationEncoding(ctx, collationID, otelManager)
	if err != nil {
		return nil, err
	}
	if enc == nil {
		return colEncodings, nil
	}

	if colEncodings == nil {
		colEncodings = make([]encoding.Encoding, len(tableMap.ColumnType))
	}
	colEncodings[colIdx] = enc

	if collationType == tableMapEnumSetCollation {
		if err := decodeMySQLStrings(enc, enumMap[colIdx]); err != nil {
			return nil, err
		}
		if err := decodeMySQLStrings(enc, setMap[colIdx]); err != nil {
			return nil, err
		}
	}
	return colEncodings, nil
}

func (c *MySqlConnector) charsetForCollation(ctx context.Context, collationID uint64) (string, error) {
	if m := c.collationCharset.Load(); m != nil {
		return (*m)[collationID], nil
	}

	m, err := c.loadCollationCharsetMap(ctx)
	if err != nil {
		return "", err
	}
	c.collationCharset.Store(&m)
	return m[collationID], nil
}

func (c *MySqlConnector) loadCollationCharsetMap(ctx context.Context) (map[uint64]string, error) {
	rs, err := c.Execute(ctx, "SELECT ID, CHARACTER_SET_NAME FROM information_schema.COLLATIONS")
	if err != nil {
		return nil, fmt.Errorf("failed to load collation charset map: %w", err)
	}

	m := make(map[uint64]string, rs.RowNumber())
	for idx := range rs.RowNumber() {
		id, err := rs.GetInt(idx, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to read collation id: %w", err)
		}
		charset, err := rs.GetString(idx, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to read collation charset name: %w", err)
		}
		m[uint64(id)] = charset
	}
	return m, nil
}

func (c *MySqlConnector) warnCharsetOnce(key string, warn func()) {
	if _, loaded := c.warnedCharsets.LoadOrStore(key, struct{}{}); !loaded {
		warn()
	}
}

// decodeMySQLBytes converts bytes stored in the column's character set to UTF-8.
func decodeMySQLBytes(enc encoding.Encoding, b []byte) (string, error) {
	if enc == nil {
		return string(b), nil
	}
	out, _, err := transform.Bytes(enc.NewDecoder(), b)
	if err != nil {
		return "", fmt.Errorf("failed to transcode column bytes to UTF-8: %w", err)
	}
	return string(out), nil
}

// decodeMySQLBytes converts string stored in the column's character set to UTF-8.
func decodeMySQLString(enc encoding.Encoding, s string) (string, error) {
	if enc == nil {
		return s, nil
	}
	out, _, err := transform.String(enc.NewDecoder(), s)
	if err != nil {
		return "", fmt.Errorf("failed to transcode column string to UTF-8: %w", err)
	}
	return out, nil
}

func decodeMySQLStrings(enc encoding.Encoding, values []string) error {
	for idx, value := range values {
		decoded, err := decodeMySQLString(enc, value)
		if err != nil {
			return err
		}
		values[idx] = decoded
	}
	return nil
}
