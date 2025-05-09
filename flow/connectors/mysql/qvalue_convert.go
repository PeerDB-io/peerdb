package connmysql

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/shopspring/decimal"
	geom "github.com/twpayne/go-geos"

	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func qkindFromMysql(field *mysql.Field) (qvalue.QValueKind, error) {
	unsigned := (field.Flag & mysql.UNSIGNED_FLAG) != 0
	switch field.Type {
	case mysql.MYSQL_TYPE_TINY:
		if unsigned {
			return qvalue.QValueKindUInt8, nil
		} else {
			return qvalue.QValueKindInt8, nil
		}
	case mysql.MYSQL_TYPE_SHORT:
		if unsigned {
			return qvalue.QValueKindUInt16, nil
		} else {
			return qvalue.QValueKindInt16, nil
		}
	case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
		if unsigned {
			return qvalue.QValueKindUInt32, nil
		} else {
			return qvalue.QValueKindInt32, nil
		}
	case mysql.MYSQL_TYPE_LONGLONG:
		if unsigned {
			return qvalue.QValueKindUInt64, nil
		} else {
			return qvalue.QValueKindInt64, nil
		}
	case mysql.MYSQL_TYPE_FLOAT:
		return qvalue.QValueKindFloat32, nil
	case mysql.MYSQL_TYPE_DOUBLE:
		return qvalue.QValueKindFloat64, nil
	case mysql.MYSQL_TYPE_NULL:
		return qvalue.QValueKindInvalid, nil
	case mysql.MYSQL_TYPE_TIMESTAMP:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_DATE:
		return qvalue.QValueKindDate, nil
	case mysql.MYSQL_TYPE_TIME:
		return qvalue.QValueKindTime, nil
	case mysql.MYSQL_TYPE_DATETIME:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_YEAR:
		return qvalue.QValueKindInt16, nil
	case mysql.MYSQL_TYPE_NEWDATE:
		return qvalue.QValueKindDate, nil
	case mysql.MYSQL_TYPE_VARCHAR:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_BIT:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_DATETIME2:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_TIME2:
		return qvalue.QValueKindTime, nil
	case mysql.MYSQL_TYPE_JSON:
		return qvalue.QValueKindJSON, nil
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		return qvalue.QValueKindNumeric, nil
	case mysql.MYSQL_TYPE_ENUM:
		return qvalue.QValueKindEnum, nil
	case mysql.MYSQL_TYPE_SET:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB:
		if field.Charset == 0x3f { // binary https://dev.mysql.com/doc/dev/mysql-server/8.4.3/page_protocol_basic_character_set.html
			return qvalue.QValueKindBytes, nil
		} else {
			return qvalue.QValueKindString, nil
		}
	case mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_GEOMETRY:
		return qvalue.QValueKindGeometry, nil
	case mysql.MYSQL_TYPE_VECTOR:
		return qvalue.QValueKindArrayFloat32, nil
	default:
		return qvalue.QValueKind(""), fmt.Errorf("unknown mysql type %d", field.Type)
	}
}

func qkindFromMysqlColumnType(ct string) (qvalue.QValueKind, error) {
	ct, isUnsigned := strings.CutSuffix(ct, " unsigned")
	ct, param, _ := strings.Cut(ct, "(")
	switch strings.ToLower(ct) {
	case "json":
		return qvalue.QValueKindJSON, nil
	case "char", "varchar", "text", "set", "tinytext", "mediumtext", "longtext":
		return qvalue.QValueKindString, nil
	case "enum":
		return qvalue.QValueKindEnum, nil
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return qvalue.QValueKindBytes, nil
	case "date":
		return qvalue.QValueKindDate, nil
	case "time":
		return qvalue.QValueKindTime, nil
	case "datetime", "timestamp":
		return qvalue.QValueKindTimestamp, nil
	case "decimal", "numeric":
		return qvalue.QValueKindNumeric, nil
	case "float":
		return qvalue.QValueKindFloat32, nil
	case "double":
		return qvalue.QValueKindFloat64, nil
	case "tinyint":
		if strings.HasPrefix(param, "1)") {
			return qvalue.QValueKindBoolean, nil
		} else if isUnsigned {
			return qvalue.QValueKindUInt8, nil
		} else {
			return qvalue.QValueKindInt8, nil
		}
	case "smallint", "year":
		if isUnsigned {
			return qvalue.QValueKindUInt16, nil
		} else {
			return qvalue.QValueKindInt16, nil
		}
	case "mediumint", "int":
		if isUnsigned {
			return qvalue.QValueKindUInt32, nil
		} else {
			return qvalue.QValueKindInt32, nil
		}
	case "bit":
		return qvalue.QValueKindUInt64, nil
	case "bigint":
		if isUnsigned {
			return qvalue.QValueKindUInt64, nil
		} else {
			return qvalue.QValueKindInt64, nil
		}
	case "vector":
		return qvalue.QValueKindArrayFloat32, nil
	case "geometry", "point", "polygon", "linestring", "multipoint", "multipolygon", "geomcollection":
		return qvalue.QValueKindGeometry, nil
	default:
		return qvalue.QValueKind(""), fmt.Errorf("unknown mysql type %s", ct)
	}
}

func QRecordSchemaFromMysqlFields(tableSchema *protos.TableSchema, fields []*mysql.Field) (qvalue.QRecordSchema, error) {
	tableColumns := make(map[string]*protos.FieldDescription, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		tableColumns[col.Name] = col
	}

	schema := make([]qvalue.QField, 0, len(fields))
	for _, field := range fields {
		var precision int16
		var scale int16
		name := string(field.Name)
		var qkind qvalue.QValueKind
		if col, ok := tableColumns[name]; ok {
			qkind = qvalue.QValueKind(col.Type)
			if qkind == qvalue.QValueKindNumeric {
				precision, scale = datatypes.ParseNumericTypmod(col.TypeModifier)
			}
		} else {
			var err error
			qkind, err = qkindFromMysql(field)
			if err != nil {
				return qvalue.QRecordSchema{}, err
			}
		}

		schema = append(schema, qvalue.QField{
			Name:      name,
			Type:      qkind,
			Precision: precision,
			Scale:     scale,
			Nullable:  (field.Flag & mysql.NOT_NULL_FLAG) == 0,
		})
	}
	return qvalue.QRecordSchema{Fields: schema}, nil
}

// Helper function to convert MySQL geometry binary data to WKT format
func geometryValueFromBytes(wkbData []byte) (string, error) {
	// Try to parse it as WKB with the MySQL header
	g, err := geom.NewGeomFromWKB(wkbData)
	if err != nil {
		return "", err
	}

	// Convert to WKT format
	wkt := g.ToWKT()
	if srid := g.SRID(); srid != 0 {
		wkt = fmt.Sprintf("SRID=%d;%s", srid, wkt)
	}
	return wkt, nil
}

// Helper function to process geometry data and return a QValueGeometry
func processGeometryData(data []byte) qvalue.QValueGeometry {
	// For geometry data, we need to convert from MySQL's binary format to WKT
	if len(data) > 4 {
		wkt, err := geometryValueFromBytes(data)
		if err == nil {
			return qvalue.QValueGeometry{Val: wkt}
		}
	}
	strVal := string(data)
	return qvalue.QValueGeometry{Val: strVal}
}

func QValueFromMysqlFieldValue(qkind qvalue.QValueKind, fv mysql.FieldValue) (qvalue.QValue, error) {
	switch fv.Type {
	case mysql.FieldValueTypeNull:
		return qvalue.QValueNull(qkind), nil
	case mysql.FieldValueTypeUnsigned:
		v := fv.AsUint64()
		switch qkind {
		case qvalue.QValueKindBoolean:
			return qvalue.QValueBoolean{Val: v != 0}, nil
		case qvalue.QValueKindInt8:
			return qvalue.QValueInt8{Val: int8(v)}, nil
		case qvalue.QValueKindInt16:
			return qvalue.QValueInt16{Val: int16(v)}, nil
		case qvalue.QValueKindInt32:
			return qvalue.QValueInt32{Val: int32(v)}, nil
		case qvalue.QValueKindInt64:
			return qvalue.QValueInt64{Val: int64(v)}, nil
		case qvalue.QValueKindUInt8:
			return qvalue.QValueUInt8{Val: uint8(v)}, nil
		case qvalue.QValueKindUInt16:
			return qvalue.QValueUInt16{Val: uint16(v)}, nil
		case qvalue.QValueKindUInt32:
			return qvalue.QValueUInt32{Val: uint32(v)}, nil
		case qvalue.QValueKindUInt64:
			return qvalue.QValueUInt64{Val: v}, nil
		default:
			return nil, fmt.Errorf("cannot convert uint64 to %s", qkind)
		}
	case mysql.FieldValueTypeSigned:
		v := fv.AsInt64()
		switch qkind {
		case qvalue.QValueKindBoolean:
			return qvalue.QValueBoolean{Val: v != 0}, nil
		case qvalue.QValueKindInt8:
			return qvalue.QValueInt8{Val: int8(v)}, nil
		case qvalue.QValueKindInt16:
			return qvalue.QValueInt16{Val: int16(v)}, nil
		case qvalue.QValueKindInt32:
			return qvalue.QValueInt32{Val: int32(v)}, nil
		case qvalue.QValueKindInt64:
			return qvalue.QValueInt64{Val: v}, nil
		case qvalue.QValueKindUInt8:
			return qvalue.QValueUInt8{Val: uint8(v)}, nil
		case qvalue.QValueKindUInt16:
			return qvalue.QValueUInt16{Val: uint16(v)}, nil
		case qvalue.QValueKindUInt32:
			return qvalue.QValueUInt32{Val: uint32(v)}, nil
		case qvalue.QValueKindUInt64:
			return qvalue.QValueUInt64{Val: uint64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert int64 to %s", qkind)
		}
	case mysql.FieldValueTypeFloat:
		v := fv.AsFloat64()
		switch qkind {
		case qvalue.QValueKindFloat32:
			return qvalue.QValueFloat32{Val: float32(v)}, nil
		case qvalue.QValueKindFloat64:
			return qvalue.QValueFloat64{Val: float64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert float64 to %s", qkind)
		}
	case mysql.FieldValueTypeString:
		v := fv.AsString()
		unsafeString := shared.UnsafeFastReadOnlyBytesToString(v)
		switch qkind {
		case qvalue.QValueKindUInt64: // bit
			var bit uint64
			for _, b := range v {
				bit = (bit << 8) | uint64(b)
			}
			return qvalue.QValueUInt64{Val: bit}, nil
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: string(v)}, nil
		case qvalue.QValueKindEnum:
			return qvalue.QValueEnum{Val: string(v)}, nil
		case qvalue.QValueKindBytes:
			return qvalue.QValueBytes{Val: slices.Clone(v)}, nil
		case qvalue.QValueKindJSON:
			return qvalue.QValueJSON{Val: string(v)}, nil
		case qvalue.QValueKindGeometry:
			return processGeometryData(v), nil
		case qvalue.QValueKindNumeric:
			val, err := decimal.NewFromString(unsafeString)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueNumeric{Val: val}, nil
		case qvalue.QValueKindTimestamp:
			if strings.HasPrefix(unsafeString, "0000-00-00") {
				return qvalue.QValueTimestamp{Val: time.Unix(0, 0)}, nil
			}
			val, err := time.Parse("2006-01-02 15:04:05.999999", unsafeString)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueTimestamp{Val: val}, nil
		case qvalue.QValueKindTime:
			val, err := time.Parse("15:04:05.999999", unsafeString)
			if err != nil {
				return nil, err
			}
			h, m, s := val.Clock()
			return qvalue.QValueTime{Val: time.Date(1970, 1, 1, h, m, s, val.Nanosecond(), val.Location())}, nil
		case qvalue.QValueKindDate:
			if unsafeString == "0000-00-00" {
				return qvalue.QValueDate{Val: time.Unix(0, 0)}, nil
			}
			val, err := time.Parse(time.DateOnly, unsafeString)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueDate{Val: val}, nil
		case qvalue.QValueKindArrayFloat32:
			floats := make([]float32, 0, len(v)/4)
			for i := 0; i < len(v); i += 4 {
				floats = append(floats, math.Float32frombits(binary.LittleEndian.Uint32(v[i:])))
			}
			return qvalue.QValueArrayFloat32{Val: floats}, nil
		default:
			return nil, fmt.Errorf("cannot convert bytes %v to %s", v, qkind)
		}
	default:
		return nil, fmt.Errorf("unexpected mysql type %d", fv.Type)
	}
}

func QValueFromMysqlRowEvent(
	mytype byte, enums []string, sets []string,
	qkind qvalue.QValueKind, val any,
) (qvalue.QValue, error) {
	// See go-mysql row_event.go for mapping
	switch val := val.(type) {
	case nil:
		return qvalue.QValueNull(qkind), nil
	case int8: // go-mysql reads all integers as signed, consumer needs to check metadata & convert
		if qkind == qvalue.QValueKindBoolean {
			return qvalue.QValueBoolean{Val: val != 0}, nil
		} else if qkind == qvalue.QValueKindUInt8 {
			return qvalue.QValueUInt8{Val: uint8(val)}, nil
		} else {
			return qvalue.QValueInt8{Val: val}, nil
		}
	case int16:
		if qkind == qvalue.QValueKindUInt16 {
			return qvalue.QValueUInt16{Val: uint16(val)}, nil
		} else {
			return qvalue.QValueInt16{Val: val}, nil
		}
	case int32:
		if qkind == qvalue.QValueKindUInt32 {
			if mytype == mysql.MYSQL_TYPE_INT24 {
				return qvalue.QValueUInt32{Val: uint32(val) & 0xFFFFFF}, nil
			} else {
				return qvalue.QValueUInt32{Val: uint32(val)}, nil
			}
		} else {
			return qvalue.QValueInt32{Val: val}, nil
		}
	case int64:
		switch qkind {
		case qvalue.QValueKindUInt64:
			return qvalue.QValueUInt64{Val: uint64(val)}, nil
		case qvalue.QValueKindInt64:
			return qvalue.QValueInt64{Val: val}, nil
		case qvalue.QValueKindString: // set
			var set []string
			if sets == nil {
				return qvalue.QValueString{Val: strconv.FormatInt(val, 10)}, nil
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
			return qvalue.QValueString{Val: strings.Join(set, ",")}, nil
		case qvalue.QValueKindEnum: // enum
			if val == 0 {
				return qvalue.QValueEnum{Val: ""}, nil
			} else if int(val)-1 < len(enums) {
				return qvalue.QValueEnum{Val: enums[int(val)-1]}, nil
			} else if enums == nil {
				return qvalue.QValueEnum{Val: strconv.FormatInt(val, 10)}, nil
			} else {
				return nil, fmt.Errorf("enum value out of range %d %v", val, enums)
			}
		}
	case float32:
		return qvalue.QValueFloat32{Val: val}, nil
	case float64:
		return qvalue.QValueFloat64{Val: val}, nil
	case decimal.Decimal:
		return qvalue.QValueNumeric{Val: val}, nil
	case int:
		// YEAR: https://dev.mysql.com/doc/refman/8.4/en/year.html
		return qvalue.QValueInt16{Val: int16(val)}, nil
	case time.Time:
		return qvalue.QValueTimestamp{Val: val}, nil
	case *replication.JsonDiff:
		// TODO support somehow??
		return qvalue.QValueNull(qvalue.QValueKindJSON), nil
	case []byte:
		switch qkind {
		case qvalue.QValueKindBytes:
			return qvalue.QValueBytes{Val: val}, nil
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: string(val)}, nil
		case qvalue.QValueKindEnum:
			return qvalue.QValueEnum{Val: string(val)}, nil
		case qvalue.QValueKindJSON:
			return qvalue.QValueJSON{Val: string(val)}, nil
		case qvalue.QValueKindGeometry:
			// Handle geometry data as binary (WKB format)
			return processGeometryData(val), nil
		case qvalue.QValueKindArrayFloat32:
			floats := make([]float32, 0, len(val)/4)
			for i := 0; i < len(val); i += 4 {
				floats = append(floats, math.Float32frombits(binary.LittleEndian.Uint32(val[i:])))
			}
			return qvalue.QValueArrayFloat32{Val: floats}, nil
		}
	case string:
		switch qkind {
		case qvalue.QValueKindBytes:
			return qvalue.QValueBytes{Val: shared.UnsafeFastStringToReadOnlyBytes(val)}, nil
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: val}, nil
		case qvalue.QValueKindEnum:
			return qvalue.QValueEnum{Val: val}, nil
		case qvalue.QValueKindJSON:
			return qvalue.QValueJSON{Val: val}, nil
		case qvalue.QValueKindTime:
			val, err := time.Parse("15:04:05.999999", val)
			if err != nil {
				return nil, err
			}
			h, m, s := val.Clock()
			return qvalue.QValueTime{Val: time.Date(1970, 1, 1, h, m, s, val.Nanosecond(), val.Location())}, nil
		case qvalue.QValueKindDate:
			if val == "0000-00-00" {
				return qvalue.QValueDate{Val: time.Unix(0, 0)}, nil
			}
			val, err := time.Parse(time.DateOnly, val)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueDate{Val: val}, nil
		case qvalue.QValueKindTimestamp: // 0000-00-00 ends up here
			if strings.HasPrefix(val, "0000-00-00") {
				return qvalue.QValueTimestamp{Val: time.Unix(0, 0)}, nil
			}
			val, err := time.Parse("2006-01-02 15:04:05.999999", val)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueTimestamp{Val: val}, nil
		}
	}
	return nil, fmt.Errorf("unexpected type %T for mysql type %d", val, mytype)
}
