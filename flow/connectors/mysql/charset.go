package connmysql

import (
	"context"
	"fmt"
	"log/slog"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
	"golang.org/x/text/transform"
)

// charsetsNoTranscode lists the MySQL character sets whose stored bytes are
// already valid UTF-8 (or are opaque binary) and therefore need no conversion
// when read off the binlog. Anything in this set maps to a nil encoding and
// keeps the original zero-cost string(val) fast path.
var charsetsNoTranscode = map[string]struct{}{
	"utf8":    {},
	"utf8mb3": {},
	"utf8mb4": {},
	"ascii":   {},
	"binary":  {},
}

// mysqlCharsetEncodings maps a MySQL character set name to the golang.org/x/text
// encoding used to decode its bytes into UTF-8.
//
// Caveat worth remembering: MySQL's "latin1" is Windows-1252, NOT ISO-8859-1 —
// it assigns printable characters to the 0x80-0x9F range that ISO-8859-1 leaves
// as C1 control codes. Decoding latin1 as ISO-8859-1 silently mangles smart
// quotes, the euro sign, etc., so we deliberately use charmap.Windows1252.
//
// MySQL's "utf16"/"ucs2" are big-endian; "utf16le" is little-endian; "utf32" is
// big-endian. We ignore any BOM since binlog column data carries none.
var mysqlCharsetEncodings = map[string]encoding.Encoding{
	// Single-byte / Windows & ISO code pages.
	"latin1":   charmap.Windows1252,
	"latin2":   charmap.ISO8859_2,
	"latin5":   charmap.Windows1254,
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

// collationEncoding resolves a MySQL collation id (as carried in binlog
// TABLE_MAP metadata) to the x/text encoding needed to convert that column's
// bytes to UTF-8. It returns (nil, nil) when no transcoding is required, i.e.
// the column is already UTF-8/ascii/binary, the collation is unknown, or the
// charset is one we cannot transcode (in which case we warn once and fall back
// to passing the raw bytes through, matching pre-existing behavior).
func (c *MySqlConnector) collationEncoding(ctx context.Context, collationID uint64) (encoding.Encoding, error) {
	if collationID == 0 {
		return nil, nil
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
		return nil, nil
	}
	if enc, ok := mysqlCharsetEncodings[charset]; ok {
		return enc, nil
	}

	c.warnCharsetOnce(charset, func() {
		c.logger.Warn("unsupported MySQL character set on CDC path, passing bytes through untranscoded",
			slog.String("charset", charset), slog.Uint64("collationID", collationID))
	})
	return nil, nil
}

// charsetForCollation maps a collation id to its character set name, lazily
// loading (and caching) the full table from information_schema on first use.
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
// enc may be nil, meaning the bytes are already UTF-8/ascii/binary and are
// returned verbatim (the zero-overhead common case).
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

// decodeMySQLString is the string-typed counterpart to decodeMySQLBytes.
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
