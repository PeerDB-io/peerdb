package connmysql

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"math/bits"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	geom "github.com/twpayne/go-geos"
	"go.temporal.io/sdk/log"
	"golang.org/x/text/encoding"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func qkindFromMysqlType(mytype byte, unsigned bool, charset uint16, version uint32) (types.QValueKind, error) {
	switch mytype {
	case mysql.MYSQL_TYPE_TINY:
		if unsigned {
			return types.QValueKindUInt8, nil
		} else {
			return types.QValueKindInt8, nil
		}
	case mysql.MYSQL_TYPE_SHORT:
		if unsigned {
			return types.QValueKindUInt16, nil
		} else {
			return types.QValueKindInt16, nil
		}
	case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
		if unsigned {
			return types.QValueKindUInt32, nil
		} else {
			return types.QValueKindInt32, nil
		}
	case mysql.MYSQL_TYPE_LONGLONG:
		if unsigned {
			return types.QValueKindUInt64, nil
		} else {
			return types.QValueKindInt64, nil
		}
	case mysql.MYSQL_TYPE_FLOAT:
		return types.QValueKindFloat32, nil
	case mysql.MYSQL_TYPE_DOUBLE:
		return types.QValueKindFloat64, nil
	case mysql.MYSQL_TYPE_NULL:
		return types.QValueKindInvalid, nil
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE:
		return types.QValueKindDate, nil
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME,
		mysql.MYSQL_TYPE_TIMESTAMP2, mysql.MYSQL_TYPE_DATETIME2:
		return types.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2:
		return types.QValueKindTime, nil
	case mysql.MYSQL_TYPE_YEAR:
		return types.QValueKindInt16, nil
	case mysql.MYSQL_TYPE_BIT:
		if version >= shared.InternalVersion_MySQLConvertBitToUInt64 {
			return types.QValueKindUInt64, nil
		}
		return types.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_JSON:
		return types.QValueKindJSON, nil
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		return types.QValueKindNumeric, nil
	case mysql.MYSQL_TYPE_ENUM:
		return types.QValueKindEnum, nil
	case mysql.MYSQL_TYPE_SET:
		return types.QValueKindString, nil
	case mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB:
		if charset == 0x3f { // binary https://dev.mysql.com/doc/dev/mysql-server/8.4.3/page_protocol_basic_character_set.html
			return types.QValueKindBytes, nil
		} else {
			return types.QValueKindString, nil
		}
	case mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR:
		if charset == 0x3f {
			return types.QValueKindBytes, nil
		}
		return types.QValueKindString, nil
	case mysql.MYSQL_TYPE_GEOMETRY:
		return types.QValueKindGeometry, nil
	case mysql.MYSQL_TYPE_VECTOR:
		return types.QValueKindArrayFloat32, nil
	default:
		return types.QValueKind(""), fmt.Errorf("unknown mysql type %d", mytype)
	}
}

func QRecordSchemaFromMysqlFields(tableSchema *protos.TableSchema, fields []*mysql.Field, version uint32) (types.QRecordSchema, error) {
	tableColumns := make(map[string]*protos.FieldDescription, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		tableColumns[col.Name] = col
	}

	schema := make([]types.QField, 0, len(fields))
	for _, field := range fields {
		var precision int16
		var scale int16
		name := string(field.Name)
		var qkind types.QValueKind
		if col, ok := tableColumns[name]; ok {
			qkind = types.QValueKind(col.Type)
			if qkind == types.QValueKindNumeric {
				precision, scale = common.ParseNumericTypmod(col.TypeModifier)
			}
		} else {
			var err error
			unsigned := (field.Flag & mysql.UNSIGNED_FLAG) != 0
			qkind, err = qkindFromMysqlType(field.Type, unsigned, field.Charset, version)
			if err != nil {
				return types.QRecordSchema{}, err
			}
		}

		schema = append(schema, types.QField{
			Name:      name,
			Type:      qkind,
			Precision: precision,
			Scale:     scale,
			Nullable:  (field.Flag & mysql.NOT_NULL_FLAG) == 0,
		})
	}
	return types.QRecordSchema{Fields: schema}, nil
}

// MySQL's internal geometry format is 4-byte SRID (little-endian) followed by standard WKB.
// SRID is stripped because destinations like ClickHouse don't support EWKT (SRID=N;WKT).
func processGeometryData(data []byte) (types.QValueGeometry, error) {
	if len(data) <= 4 {
		return types.QValueGeometry{}, fmt.Errorf("geometry data too short: %d bytes", len(data))
	}
	g, err := geom.NewGeomFromWKB(data[4:])
	if err != nil {
		return types.QValueGeometry{}, exceptions.NewMySQLGeometryParseError(err)
	}
	return types.QValueGeometry{Val: g.ToWKT()}, nil
}

// https://dev.mysql.com/doc/refman/8.4/en/time.html
func processTime(str string) (time.Duration, error) {
	abs, isNeg := strings.CutPrefix(str, "-")
	tpart, frac, _ := strings.Cut(abs, ".")

	var nsec uint64
	if frac != "" {
		fint, err := strconv.ParseUint(frac, 10, 64)
		if err != nil {
			return 0, err
		}
		if len(frac) <= 9 {
			nsec = fint * uint64(math.Pow10(9-len(frac)))
		} else {
			nsec = fint
		}
	}

	if nsec > 999999999 {
		return 0, fmt.Errorf("nanoseconds (%d) should not exceed one second", nsec)
	}

	var err error
	var spart, mpart, hpart uint64
	h, ms, hasMS := strings.Cut(tpart, ":")
	if hasMS {
		m, s, hasS := strings.Cut(ms, ":")
		if hasS {
			spart, err = strconv.ParseUint(s, 10, 64)
		}
		if err == nil {
			mpart, err = strconv.ParseUint(m, 10, 64)
			if err == nil {
				hpart, err = strconv.ParseUint(h, 10, 64)
			}
		}
	} else if len(h) <= 2 {
		spart, err = strconv.ParseUint(h, 10, 64)
	} else if len(h) <= 4 {
		spart, err = strconv.ParseUint(h[len(h)-2:], 10, 64)
		if err == nil {
			mpart, err = strconv.ParseUint(h[:len(h)-2], 10, 64)
		}
	} else {
		spart, err = strconv.ParseUint(h[len(h)-2:], 10, 64)
		if err == nil {
			mpart, err = strconv.ParseUint(h[len(h)-4:len(h)-2], 10, 64)
			if err == nil {
				hpart, err = strconv.ParseUint(h[:len(h)-4], 10, 64)
			}
		}
	}

	if err != nil {
		return 0, err
	}

	sec := hpart*3600 + mpart*60 + spart
	val := time.Duration(sec)*time.Second + time.Duration(nsec)
	if isNeg {
		return -val, nil
	}
	return val, nil
}

// padTrimmedMariaDBBinary restores a fixed-width binary value that MariaDB
// trimmed when binlog-packing it. MariaDB stores BINARY(N) and the native
// uuid/inet4/inet6 types (all arriving as MYSQL_TYPE_STRING) with trailing
// 0x00 bytes stripped, so a value such as uuid 123e…4000 loses its final byte
// and an all-zero value (uuid 0000…0000, inet 0.0.0.0, inet6 ::) arrives with
// zero length. Right-padding with zeros to the column's declared width
// reconstructs the original value. width comes from binaryColumnLength, which
// also distinguishes inet4 (4) from inet6 (16) when the bytes alone cannot.
func padTrimmedMariaDBBinary(data []byte, width int) []byte {
	if width <= len(data) {
		return data
	}
	padded := make([]byte, width)
	copy(padded, data)
	return padded
}

func formatMariaDBInet(data []byte) (string, error) {
	switch len(data) {
	case 4, 16:
		return net.IP(data).String(), nil
	default:
		return "", fmt.Errorf("invalid inet byte length %d", len(data))
	}
}

func decodeMariaDBUUID(data []byte) (uuid.UUID, error) {
	if len(data) != 16 {
		return uuid.UUID{}, fmt.Errorf("invalid uuid byte length %d", len(data))
	}
	return uuid.FromBytes(data)
}

func processVector(data []byte) []float32 {
	floats := make([]float32, 0, len(data)/4)
	for i := 0; i < len(data); i += 4 {
		floats = append(floats, math.Float32frombits(binary.LittleEndian.Uint32(data[i:i+4])))
	}
	return floats
}

func QValueFromMysqlFieldValue(qkind types.QValueKind, mytype byte, fv mysql.FieldValue) (types.QValue, error) {
	switch fv.Type {
	case mysql.FieldValueTypeNull:
		return types.QValueNull(qkind), nil
	case mysql.FieldValueTypeUnsigned:
		v := fv.AsUint64()
		switch qkind {
		case types.QValueKindUint16Enum:
			return types.QValueUint16Enum{Val: uint16(v)}, nil
		case types.QValueKindBoolean:
			return types.QValueBoolean{Val: v != 0}, nil
		case types.QValueKindInt8:
			return types.QValueInt8{Val: int8(v)}, nil
		case types.QValueKindInt16:
			return types.QValueInt16{Val: int16(v)}, nil
		case types.QValueKindInt32:
			return types.QValueInt32{Val: int32(v)}, nil
		case types.QValueKindInt64:
			return types.QValueInt64{Val: int64(v)}, nil
		case types.QValueKindUInt8:
			return types.QValueUInt8{Val: uint8(v)}, nil
		case types.QValueKindUInt16:
			return types.QValueUInt16{Val: uint16(v)}, nil
		case types.QValueKindUInt32:
			return types.QValueUInt32{Val: uint32(v)}, nil
		case types.QValueKindUInt64:
			return types.QValueUInt64{Val: v}, nil
		default:
			return nil, fmt.Errorf("cannot convert uint64 to %s", qkind)
		}
	case mysql.FieldValueTypeSigned:
		v := fv.AsInt64()
		switch qkind {
		case types.QValueKindBoolean:
			return types.QValueBoolean{Val: v != 0}, nil
		case types.QValueKindInt8:
			return types.QValueInt8{Val: int8(v)}, nil
		case types.QValueKindInt16:
			return types.QValueInt16{Val: int16(v)}, nil
		case types.QValueKindInt32:
			return types.QValueInt32{Val: int32(v)}, nil
		case types.QValueKindInt64:
			return types.QValueInt64{Val: v}, nil
		case types.QValueKindUInt8:
			return types.QValueUInt8{Val: uint8(v)}, nil
		case types.QValueKindUInt16:
			return types.QValueUInt16{Val: uint16(v)}, nil
		case types.QValueKindUInt32:
			return types.QValueUInt32{Val: uint32(v)}, nil
		case types.QValueKindUInt64:
			return types.QValueUInt64{Val: uint64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert int64 to %s", qkind)
		}
	case mysql.FieldValueTypeFloat:
		v := fv.AsFloat64()
		switch qkind {
		case types.QValueKindFloat32:
			return types.QValueFloat32{Val: float32(v)}, nil
		case types.QValueKindFloat64:
			return types.QValueFloat64{Val: float64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert float64 to %s", qkind)
		}
	case mysql.FieldValueTypeString:
		v := fv.AsString()
		unsafeString := shared.UnsafeFastReadOnlyBytesToString(v)
		switch qkind {
		case types.QValueKindUInt64: // bit
			var bit uint64
			for _, b := range v {
				bit = (bit << 8) | uint64(b)
			}
			return types.QValueUInt64{Val: bit}, nil
		case types.QValueKindString:
			return types.QValueString{Val: string(v)}, nil
		case types.QValueKindEnum:
			return types.QValueEnum{Val: string(v)}, nil
		case types.QValueKindBytes:
			return types.QValueBytes{Val: slices.Clone(v)}, nil
		case types.QValueKindJSON:
			return types.QValueJSON{Val: string(v)}, nil
		case types.QValueKindUUID:
			// snapshot reads via the text protocol, so MariaDB sends the canonical string
			u, err := uuid.Parse(unsafeString)
			if err != nil {
				return nil, err
			}
			return types.QValueUUID{Val: u}, nil
		case types.QValueKindINET:
			// MariaDB INET4/INET6 render as text over the wire
			return types.QValueINET{Val: string(v)}, nil
		case types.QValueKindGeometry:
			return processGeometryData(v)
		case types.QValueKindNumeric:
			val, err := decimal.NewFromString(unsafeString)
			if err != nil {
				return nil, err
			}
			return types.QValueNumeric{Val: val}, nil
		case types.QValueKindTimestamp:
			if strings.HasPrefix(unsafeString, "0000-00-00") {
				return types.QValueTimestamp{Val: time.Unix(0, 0)}, nil
			}
			val, err := time.Parse("2006-01-02 15:04:05.999999", strings.ReplaceAll(unsafeString, "-00", "-01"))
			if err != nil {
				return nil, err
			}
			return types.QValueTimestamp{Val: val}, nil
		case types.QValueKindTime:
			tm, err := processTime(unsafeString)
			if err != nil {
				return nil, err
			}
			return types.QValueTime{Val: tm}, nil
		case types.QValueKindDate:
			if unsafeString == "0000-00-00" {
				return types.QValueDate{Val: time.Unix(0, 0)}, nil
			}
			val, err := time.Parse(time.DateOnly, strings.ReplaceAll(unsafeString, "-00", "-01"))
			if err != nil {
				return nil, err
			}
			return types.QValueDate{Val: val}, nil
		case types.QValueKindArrayFloat32:
			floats := processVector(v)
			return types.QValueArrayFloat32{Val: floats}, nil
		default:
			return nil, fmt.Errorf("cannot convert bytes %v to %s", v, qkind)
		}
	default:
		return nil, fmt.Errorf("unexpected mysql type %d", fv.Type)
	}
}

// binaryColumnLength returns the declared length N of a fixed-length BINARY(N)
// `mytype` is the MySQL type for the column
// `meta` is the metadata for the column type: length for fixed length types.
// Reimplementation of MySQL metadata decoding:
//   - MySQL 8.4: https://github.com/mysql/mysql-server/blob/845d525d49c8027a4d0cdcc43372c96ba295c857/sql/log_event.cc#L1792-L1805
//   - MySQL 5.7: https://github.com/mysql/mysql-server/blob/f7680e98b6bbe3500399fbad465d08a6b75d7a5c/sql/log_event.cc#L2047-L2064
//   - MariaDB 10.6:
//     https://github.com/MariaDB/server/blob/fcd3f81e08daea471d371d3be051e6feabb06399/sql/rpl_utility_server.cc#L139-L140
//     https://github.com/MariaDB/server/blob/fcd3f81e08daea471d371d3be051e6feabb06399/sql/sql_type_string.cc#L70-L73
func binaryColumnLength(mytype byte, meta uint16) int {
	if mytype != mysql.MYSQL_TYPE_STRING {
		return 0
	}
	if meta < 256 { // no bit-packing, just the length
		return int(meta)
	}
	// bit-packed, for more than 255 bytes:
	// value = lower_byte(meta) + 2^4 * ((higher_byte(meta)&0x30) XOR 0x30)
	lowerMetaByte := uint8(meta & 0xFF)
	higherMetaByte := uint8(meta >> 8)
	borrowedBitsMask := uint8(0x30)
	extraBits := higherMetaByte & borrowedBitsMask
	if extraBits != borrowedBitsMask { // More than 255 bytes
		return int(lowerMetaByte) | (int((extraBits)^borrowedBitsMask) << 4)
	}
	return int(meta & 0xFF)
}

func QValueFromMysqlRowEvent(
	ev *replication.TableMapEvent, idx int,
	enums []string, sets []string,
	qkind types.QValueKind, val any, enc encoding.Encoding, logger log.Logger, coercionReported *bool,
) (types.QValue, error) {
	mytype := ev.ColumnType[idx]

	// See go-mysql row_event.go for mapping
	switch val := val.(type) {
	case nil:
		return types.QValueNull(qkind), nil
	case int8: // minimal-metadata streams return all integers as signed
		switch qkind {
		case types.QValueKindBoolean:
			return types.QValueBoolean{Val: val != 0}, nil
		case types.QValueKindString:
			return types.QValueString{Val: strconv.FormatInt(int64(val), 10)}, nil
		case types.QValueKindUInt8:
			return types.QValueUInt8{Val: uint8(val)}, nil
		default:
			return types.QValueInt8{Val: val}, nil
		}
	case uint8:
		switch qkind {
		case types.QValueKindBoolean:
			return types.QValueBoolean{Val: val != 0}, nil
		case types.QValueKindString:
			return types.QValueString{Val: strconv.FormatUint(uint64(val), 10)}, nil
		case types.QValueKindInt8:
			return types.QValueInt8{Val: int8(val)}, nil
		default:
			return types.QValueUInt8{Val: val}, nil
		}
	case int16:
		switch qkind {
		case types.QValueKindUInt16:
			return types.QValueUInt16{Val: uint16(val)}, nil
		case types.QValueKindString:
			return types.QValueString{Val: strconv.FormatInt(int64(val), 10)}, nil
		default:
			return types.QValueInt16{Val: val}, nil
		}
	case uint16:
		switch qkind {
		case types.QValueKindInt16:
			return types.QValueInt16{Val: int16(val)}, nil
		case types.QValueKindString:
			return types.QValueString{Val: strconv.FormatUint(uint64(val), 10)}, nil
		default:
			return types.QValueUInt16{Val: val}, nil
		}
	case int32:
		switch qkind {
		case types.QValueKindUInt32:
			if mytype == mysql.MYSQL_TYPE_INT24 {
				return types.QValueUInt32{Val: uint32(val) & 0xFFFFFF}, nil
			} else {
				return types.QValueUInt32{Val: uint32(val)}, nil
			}
		case types.QValueKindString:
			return types.QValueString{Val: strconv.FormatInt(int64(val), 10)}, nil
		default:
			return types.QValueInt32{Val: val}, nil
		}
	case uint32:
		switch qkind {
		case types.QValueKindInt32:
			return types.QValueInt32{Val: int32(val)}, nil
		case types.QValueKindString:
			return types.QValueString{Val: strconv.FormatUint(uint64(val), 10)}, nil
		default:
			return types.QValueUInt32{Val: val}, nil
		}
	case int64:
		switch qkind {
		case types.QValueKindUInt64:
			return types.QValueUInt64{Val: uint64(val)}, nil
		case types.QValueKindInt64:
			return types.QValueInt64{Val: val}, nil
		case types.QValueKindString: // set
			var set []string
			if sets == nil {
				return types.QValueString{Val: strconv.FormatInt(val, 10)}, nil
			}
			for val != 0 {
				idx := bits.TrailingZeros64(uint64(val))
				if idx < len(sets) {
					set = append(set, sets[idx])
					val ^= int64(1) << idx
				} else {
					return nil, fmt.Errorf("set value out of range %d %v", idx, sets)
				}
			}
			return types.QValueString{Val: strings.Join(set, ",")}, nil
		case types.QValueKindUint16Enum:
			return types.QValueUint16Enum{Val: uint16(val)}, nil
		case types.QValueKindEnum: // enum
			if val == 0 {
				return types.QValueEnum{Val: ""}, nil
			} else if int(val)-1 < len(enums) {
				return types.QValueEnum{Val: enums[int(val)-1]}, nil
			} else if enums == nil {
				return types.QValueEnum{Val: strconv.FormatInt(val, 10)}, nil
			} else {
				return nil, fmt.Errorf("enum value out of range %d %v", val, enums)
			}
		}
	case uint64:
		switch qkind {
		case types.QValueKindInt64:
			return types.QValueInt64{Val: int64(val)}, nil
		case types.QValueKindString:
			return types.QValueString{Val: strconv.FormatUint(val, 10)}, nil
		default:
			return types.QValueUInt64{Val: val}, nil
		}
	case float32:
		if qkind == types.QValueKindFloat64 {
			return types.QValueFloat64{Val: float64(val)}, nil
		}
		return types.QValueFloat32{Val: val}, nil
	case float64:
		if qkind == types.QValueKindFloat32 {
			return types.QValueFloat32{Val: float32(val)}, nil
		}
		return types.QValueFloat64{Val: val}, nil
	case decimal.Decimal:
		return types.QValueNumeric{Val: val}, nil
	case int:
		// YEAR: https://dev.mysql.com/doc/refman/8.4/en/year.html
		return types.QValueInt16{Val: int16(val)}, nil
	case time.Time:
		return types.QValueTimestamp{Val: val}, nil
	case *replication.JsonDiff:
		// TODO support somehow??
		return types.QValueNull(types.QValueKindJSON), nil
	case []byte:
		switch qkind {
		case types.QValueKindBytes:
			return types.QValueBytes{Val: val}, nil
		case types.QValueKindString:
			s, err := decodeMySQLBytes(enc, val)
			if err != nil {
				return nil, err
			}
			return types.QValueString{Val: s}, nil
		case types.QValueKindEnum:
			s, err := decodeMySQLBytes(enc, val)
			if err != nil {
				return nil, err
			}
			return types.QValueEnum{Val: s}, nil
		case types.QValueKindJSON:
			// MySQL always stores JSON internally as utf8mb4, so it never needs transcoding.
			return types.QValueJSON{Val: string(val)}, nil
		case types.QValueKindGeometry:
			// Handle geometry data as binary (WKB format)
			return processGeometryData(val)
		case types.QValueKindArrayFloat32:
			floats := processVector(val)
			return types.QValueArrayFloat32{Val: floats}, nil
		}
	case string:
		switch qkind {
		case types.QValueKindBytes:
			b := shared.UnsafeFastStringToReadOnlyBytes(val)
			if n := binaryColumnLength(mytype, ev.ColumnMeta[idx]); len(b) < n {
				padded := make([]byte, n)
				copy(padded, b)
				b = padded
			}
			return types.QValueBytes{Val: b}, nil
		case types.QValueKindString:
			s, err := decodeMySQLString(enc, val)
			if err != nil {
				return nil, err
			}
			return types.QValueString{Val: s}, nil
		case types.QValueKindEnum:
			s, err := decodeMySQLString(enc, val)
			if err != nil {
				return nil, err
			}
			return types.QValueEnum{Val: s}, nil
		case types.QValueKindJSON:
			return types.QValueJSON{Val: val}, nil
		case types.QValueKindArrayFloat32:
			b := shared.UnsafeFastStringToReadOnlyBytes(val)
			floats := processVector(b)
			return types.QValueArrayFloat32{Val: floats}, nil
		case types.QValueKindUUID:
			b := padTrimmedMariaDBBinary(shared.UnsafeFastStringToReadOnlyBytes(val), binaryColumnLength(mytype, ev.ColumnMeta[idx]))
			u, err := decodeMariaDBUUID(b)
			if err != nil {
				return nil, err
			}
			return types.QValueUUID{Val: u}, nil
		case types.QValueKindINET:
			b := padTrimmedMariaDBBinary(shared.UnsafeFastStringToReadOnlyBytes(val), binaryColumnLength(mytype, ev.ColumnMeta[idx]))
			s, err := formatMariaDBInet(b)
			if err != nil {
				return nil, err
			}
			return types.QValueINET{Val: s}, nil
		case types.QValueKindTime:
			tm, err := processTime(val)
			if err != nil {
				return nil, err
			}
			return types.QValueTime{Val: tm}, nil
		case types.QValueKindDate:
			switch mytype {
			case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2,
				mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
				// Column was altered from DATE to DATETIME/TIMESTAMP.
				// go-mysql returns strings for pre-1970, zero, and partial zero dates:
				// DATETIME    zero, partial zero
				// https://github.com/go-mysql-org/go-mysql/blob/v1.13.0/replication/row_event.go#L1331-L1363
				// DATETIME2   zero, pre-1970, partial zero
				// https://github.com/go-mysql-org/go-mysql/blob/v1.13.0/replication/row_event.go#L1690-L1748
				// TIMESTAMP   zero
				// https://github.com/go-mysql-org/go-mysql/blob/v1.13.0/replication/row_event.go#L1316-L1327
				// TIMESTAMP2  zero
				// https://github.com/go-mysql-org/go-mysql/blob/v1.13.0/replication/row_event.go#L1663-L1686
				if strings.HasPrefix(val, "0000-00-00") {
					return types.QValueDate{Val: time.Unix(0, 0).UTC()}, nil
				}
				tm, err := time.Parse("2006-01-02 15:04:05.999999", strings.ReplaceAll(val, "-00", "-01"))
				if err != nil {
					return nil, err
				}
				return types.QValueDate{Val: tm.Truncate(24 * time.Hour).UTC()}, nil
			default:
				if val == "0000-00-00" {
					return types.QValueDate{Val: time.Unix(0, 0).UTC()}, nil
				}
				val, err := time.Parse(time.DateOnly, strings.ReplaceAll(val, "-00", "-01"))
				if err != nil {
					return nil, err
				}
				return types.QValueDate{Val: val.UTC()}, nil
			}
		case types.QValueKindTimestamp: // 0000-00-00 ends up here
			if mytype == mysql.MYSQL_TYPE_TIME || mytype == mysql.MYSQL_TYPE_TIME2 {
				tm, err := processTime(val)
				if err != nil {
					return nil, err
				}
				return types.QValueTimestamp{Val: time.Unix(0, 0).UTC().Add(tm)}, nil
			}
			if strings.HasPrefix(val, "0000-00-00") {
				return types.QValueTimestamp{Val: time.Unix(0, 0).UTC()}, nil
			}
			tm, err := time.Parse("2006-01-02 15:04:05.999999", strings.ReplaceAll(val, "-00", "-01"))
			if err != nil {
				return nil, err
			}
			return types.QValueTimestamp{Val: tm.UTC()}, nil
		case types.QValueKindBoolean:
			// integer types shouldn't get here, but try work with schema changes
			return types.QValueBoolean{
				Val: strings.EqualFold(val, "true") || strings.EqualFold(val, "t") ||
					strings.EqualFold(val, "on") || strings.EqualFold(val, "yes") || strings.EqualFold(val, "1"),
			}, nil
		case types.QValueKindInt8:
			v, err := strconv.ParseInt(val, 10, 8)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueInt8{Val: int8(v)}, nil
		case types.QValueKindInt16:
			v, err := strconv.ParseInt(val, 10, 16)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueInt16{Val: int16(v)}, nil
		case types.QValueKindInt32:
			v, err := strconv.ParseInt(val, 10, 32)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueInt32{Val: int32(v)}, nil
		case types.QValueKindInt64:
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueInt64{Val: v}, nil
		case types.QValueKindUInt8:
			v, err := strconv.ParseUint(val, 10, 8)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueUInt8{Val: uint8(v)}, nil
		case types.QValueKindUInt16:
			v, err := strconv.ParseUint(val, 10, 16)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueUInt16{Val: uint16(v)}, nil
		case types.QValueKindUInt32:
			v, err := strconv.ParseUint(val, 10, 32)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueUInt32{Val: uint32(v)}, nil
		case types.QValueKindUInt64:
			v, err := strconv.ParseUint(val, 10, 64)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueUInt64{Val: v}, nil
		case types.QValueKindFloat32:
			v, err := strconv.ParseFloat(val, 32)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueFloat32{Val: float32(v)}, nil
		case types.QValueKindFloat64:
			v, err := strconv.ParseFloat(val, 64)
			if err != nil && !*coercionReported {
				*coercionReported = true
				logger.Warn("coercion failed to parse int", slog.Any("error", err))
			}
			return types.QValueFloat64{Val: v}, nil
		}
	}

	schemaName := string(ev.Schema)
	tableName := string(ev.Table)
	columnName := "__peerdb_unknown_" + strconv.Itoa(idx)
	if len(ev.ColumnName) > idx {
		columnName = string(ev.ColumnName[idx])
	}
	qkindStr := string(qkind)

	err := exceptions.NewMySQLIncompatibleColumnTypeError(
		fmt.Sprintf("%s.%s", schemaName, tableName), columnName, mytype, fmt.Sprintf("%T", val), qkindStr)
	logger.Warn(err.Error())
	return nil, err
}
