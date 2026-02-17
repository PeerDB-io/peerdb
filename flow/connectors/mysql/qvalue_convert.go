package connmysql

import (
	"encoding/binary"
	"fmt"
	"log/slog"
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
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func qkindFromMysqlType(mytype byte, unsigned bool, charset uint16) (types.QValueKind, error) {
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
		return types.QValueKindString, nil
	case mysql.MYSQL_TYPE_GEOMETRY:
		return types.QValueKindGeometry, nil
	case mysql.MYSQL_TYPE_VECTOR:
		return types.QValueKindArrayFloat32, nil
	default:
		return types.QValueKind(""), fmt.Errorf("unknown mysql type %d", mytype)
	}
}

func QRecordSchemaFromMysqlFields(tableSchema *protos.TableSchema, fields []*mysql.Field) (types.QRecordSchema, error) {
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
				precision, scale = datatypes.ParseNumericTypmod(col.TypeModifier)
			}
		} else {
			var err error
			unsigned := (field.Flag & mysql.UNSIGNED_FLAG) != 0
			qkind, err = qkindFromMysqlType(field.Type, unsigned, field.Charset)
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
func processGeometryData(data []byte) types.QValueGeometry {
	// For geometry data, we need to convert from MySQL's binary format to WKT
	if len(data) > 4 {
		wkt, err := geometryValueFromBytes(data)
		if err == nil {
			return types.QValueGeometry{Val: wkt}
		}
	}
	return types.QValueGeometry{Val: string(data)}
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

func QValueFromMysqlFieldValue(qkind types.QValueKind, mytype byte, fv mysql.FieldValue) (types.QValue, error) {
	switch fv.Type {
	case mysql.FieldValueTypeNull:
		return types.QValueNull(qkind), nil
	case mysql.FieldValueTypeUnsigned:
		v := fv.AsUint64()
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
		case types.QValueKindGeometry:
			return processGeometryData(v), nil
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
			floats := make([]float32, 0, len(v)/4)
			for i := 0; i < len(v); i += 4 {
				floats = append(floats, math.Float32frombits(binary.LittleEndian.Uint32(v[i:])))
			}
			return types.QValueArrayFloat32{Val: floats}, nil
		default:
			return nil, fmt.Errorf("cannot convert bytes %v to %s", v, qkind)
		}
	default:
		return nil, fmt.Errorf("unexpected mysql type %d", fv.Type)
	}
}

func QValueFromMysqlRowEvent(
	ev *replication.TableMapEvent, idx int,
	enums []string, sets []string,
	qkind types.QValueKind, val any, logger log.Logger, coercionReported *bool,
) (types.QValue, error) {
	mytype := ev.ColumnType[idx]

	// See go-mysql row_event.go for mapping
	switch val := val.(type) {
	case nil:
		return types.QValueNull(qkind), nil
	case int8: // go-mysql reads all integers as signed, consumer needs to check metadata & convert
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
	case int16:
		switch qkind {
		case types.QValueKindUInt16:
			return types.QValueUInt16{Val: uint16(val)}, nil
		case types.QValueKindString:
			return types.QValueString{Val: strconv.FormatInt(int64(val), 10)}, nil
		default:
			return types.QValueInt16{Val: val}, nil
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
			return types.QValueString{Val: string(val)}, nil
		case types.QValueKindEnum:
			return types.QValueEnum{Val: string(val)}, nil
		case types.QValueKindJSON:
			return types.QValueJSON{Val: string(val)}, nil
		case types.QValueKindGeometry:
			// Handle geometry data as binary (WKB format)
			return processGeometryData(val), nil
		case types.QValueKindArrayFloat32:
			floats := make([]float32, 0, len(val)/4)
			for i := 0; i < len(val); i += 4 {
				floats = append(floats, math.Float32frombits(binary.LittleEndian.Uint32(val[i:])))
			}
			return types.QValueArrayFloat32{Val: floats}, nil
		}
	case string:
		switch qkind {
		case types.QValueKindBytes:
			return types.QValueBytes{Val: shared.UnsafeFastStringToReadOnlyBytes(val)}, nil
		case types.QValueKindString:
			return types.QValueString{Val: val}, nil
		case types.QValueKindEnum:
			return types.QValueEnum{Val: val}, nil
		case types.QValueKindJSON:
			return types.QValueJSON{Val: val}, nil
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
