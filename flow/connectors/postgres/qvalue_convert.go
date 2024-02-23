package connpostgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

var big10 = big.NewInt(10)

func (c *PostgresConnector) postgresOIDToQValueKind(recvOID uint32) qvalue.QValueKind {
	switch recvOID {
	case pgtype.BoolOID:
		return qvalue.QValueKindBoolean
	case pgtype.Int2OID:
		return qvalue.QValueKindInt16
	case pgtype.Int4OID:
		return qvalue.QValueKindInt32
	case pgtype.Int8OID:
		return qvalue.QValueKindInt64
	case pgtype.Float4OID:
		return qvalue.QValueKindFloat32
	case pgtype.Float8OID:
		return qvalue.QValueKindFloat64
	case pgtype.QCharOID:
		return qvalue.QValueKindQChar
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return qvalue.QValueKindString
	case pgtype.ByteaOID:
		return qvalue.QValueKindBytes
	case pgtype.JSONOID, pgtype.JSONBOID:
		return qvalue.QValueKindJSON
	case pgtype.UUIDOID:
		return qvalue.QValueKindUUID
	case pgtype.TimeOID:
		return qvalue.QValueKindTime
	case pgtype.DateOID:
		return qvalue.QValueKindDate
	case pgtype.CIDROID:
		return qvalue.QValueKindCIDR
	case pgtype.MacaddrOID:
		return qvalue.QValueKindMacaddr
	case pgtype.InetOID:
		return qvalue.QValueKindINET
	case pgtype.TimestampOID:
		return qvalue.QValueKindTimestamp
	case pgtype.TimestamptzOID:
		return qvalue.QValueKindTimestampTZ
	case pgtype.NumericOID:
		return qvalue.QValueKindNumeric
	case pgtype.BitOID, pgtype.VarbitOID:
		return qvalue.QValueKindBit
	case pgtype.Int2ArrayOID:
		return qvalue.QValueKindArrayInt16
	case pgtype.Int4ArrayOID:
		return qvalue.QValueKindArrayInt32
	case pgtype.Int8ArrayOID:
		return qvalue.QValueKindArrayInt64
	case pgtype.PointOID:
		return qvalue.QValueKindPoint
	case pgtype.Float4ArrayOID:
		return qvalue.QValueKindArrayFloat32
	case pgtype.Float8ArrayOID:
		return qvalue.QValueKindArrayFloat64
	case pgtype.BoolArrayOID:
		return qvalue.QValueKindArrayBoolean
	case pgtype.DateArrayOID:
		return qvalue.QValueKindArrayDate
	case pgtype.TimestampArrayOID:
		return qvalue.QValueKindArrayTimestamp
	case pgtype.TimestamptzArrayOID:
		return qvalue.QValueKindArrayTimestampTZ
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID:
		return qvalue.QValueKindArrayString
	default:
		typeName, ok := pgtype.NewMap().TypeForOID(recvOID)
		if !ok {
			// workaround for some types not being defined by pgtype
			if recvOID == uint32(oid.T_timetz) {
				return qvalue.QValueKindTimeTZ
			} else if recvOID == uint32(oid.T_xml) { // XML
				return qvalue.QValueKindString
			} else if recvOID == uint32(oid.T_money) { // MONEY
				return qvalue.QValueKindString
			} else if recvOID == uint32(oid.T_txid_snapshot) { // TXID_SNAPSHOT
				return qvalue.QValueKindString
			} else if recvOID == uint32(oid.T_tsvector) { // TSVECTOR
				return qvalue.QValueKindString
			} else if recvOID == uint32(oid.T_tsquery) { // TSQUERY
				return qvalue.QValueKindString
			} else if recvOID == uint32(oid.T_point) { // POINT
				return qvalue.QValueKindPoint
			}

			return qvalue.QValueKindInvalid
		} else {
			_, warned := c.hushWarnOID[recvOID]
			if !warned {
				c.logger.Warn(fmt.Sprintf("unsupported field type: %d - type name - %s; returning as string", recvOID, typeName.Name))
				c.hushWarnOID[recvOID] = struct{}{}
			}
			return qvalue.QValueKindString
		}
	}
}

func qValueKindToPostgresType(colTypeStr string) string {
	switch qvalue.QValueKind(colTypeStr) {
	case qvalue.QValueKindBoolean:
		return "BOOLEAN"
	case qvalue.QValueKindInt16:
		return "SMALLINT"
	case qvalue.QValueKindInt32:
		return "INTEGER"
	case qvalue.QValueKindInt64:
		return "BIGINT"
	case qvalue.QValueKindFloat32:
		return "REAL"
	case qvalue.QValueKindFloat64:
		return "DOUBLE PRECISION"
	case qvalue.QValueKindQChar:
		return "\"char\""
	case qvalue.QValueKindString:
		return "TEXT"
	case qvalue.QValueKindBytes:
		return "BYTEA"
	case qvalue.QValueKindJSON:
		return "JSON"
	case qvalue.QValueKindHStore:
		return "HSTORE"
	case qvalue.QValueKindUUID:
		return "UUID"
	case qvalue.QValueKindTime:
		return "TIME"
	case qvalue.QValueKindTimeTZ:
		return "TIMETZ"
	case qvalue.QValueKindDate:
		return "DATE"
	case qvalue.QValueKindTimestamp:
		return "TIMESTAMP"
	case qvalue.QValueKindTimestampTZ:
		return "TIMESTAMPTZ"
	case qvalue.QValueKindNumeric:
		return "NUMERIC"
	case qvalue.QValueKindBit:
		return "BIT"
	case qvalue.QValueKindINET:
		return "INET"
	case qvalue.QValueKindCIDR:
		return "CIDR"
	case qvalue.QValueKindMacaddr:
		return "MACADDR"
	case qvalue.QValueKindArrayInt16:
		return "SMALLINT[]"
	case qvalue.QValueKindArrayInt32:
		return "INTEGER[]"
	case qvalue.QValueKindArrayInt64:
		return "BIGINT[]"
	case qvalue.QValueKindArrayFloat32:
		return "REAL[]"
	case qvalue.QValueKindArrayFloat64:
		return "DOUBLE PRECISION[]"
	case qvalue.QValueKindArrayDate:
		return "DATE[]"
	case qvalue.QValueKindArrayTimestamp:
		return "TIMESTAMP[]"
	case qvalue.QValueKindArrayTimestampTZ:
		return "TIMESTAMPTZ[]"
	case qvalue.QValueKindArrayBoolean:
		return "BOOLEAN[]"
	case qvalue.QValueKindArrayString:
		return "TEXT[]"
	case qvalue.QValueKindGeography:
		return "GEOGRAPHY"
	case qvalue.QValueKindGeometry:
		return "GEOMETRY"
	case qvalue.QValueKindPoint:
		return "POINT"
	default:
		return "TEXT"
	}
}

func parseJSON(value interface{}) (qvalue.QValue, error) {
	jsonVal, err := json.Marshal(value)
	if err != nil {
		return qvalue.QValue{}, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return qvalue.QValue{Kind: qvalue.QValueKindJSON, Value: string(jsonVal)}, nil
}

func convertToArray[T any](kind qvalue.QValueKind, value interface{}) (qvalue.QValue, error) {
	switch v := value.(type) {
	case pgtype.Array[T]:
		if v.Valid {
			return qvalue.QValue{Kind: kind, Value: v.Elements}, nil
		}
	case []T:
		return qvalue.QValue{Kind: kind, Value: v}, nil
	case []interface{}:
		return qvalue.QValue{Kind: kind, Value: shared.ArrayCastElements[T](v)}, nil
	}
	return qvalue.QValue{}, fmt.Errorf("failed to parse array %s from %T: %v", kind, value, value)
}

func ParseFieldFromQValueKind(qvalueKind qvalue.QValueKind, value any) (qvalue.QValue, error) {
	val := qvalue.QValue{}

	if value == nil {
		val = qvalue.QValue{Kind: qvalueKind, Value: nil}
		return val, nil
	}

	switch qvalueKind {
	case qvalue.QValueKindTimestamp:
		timestamp := value.(time.Time)
		val = qvalue.QValue{Kind: qvalue.QValueKindTimestamp, Value: timestamp}
	case qvalue.QValueKindTimestampTZ:
		timestamp := value.(time.Time)
		val = qvalue.QValue{Kind: qvalue.QValueKindTimestampTZ, Value: timestamp}
	case qvalue.QValueKindDate:
		date := value.(time.Time)
		val = qvalue.QValue{Kind: qvalue.QValueKindDate, Value: date}
	case qvalue.QValueKindTime:
		timeVal := value.(pgtype.Time)
		if timeVal.Valid {
			var timeValStr any
			timeValStr, err := timeVal.Value()
			if err != nil {
				return qvalue.QValue{}, fmt.Errorf("failed to parse time: %w", err)
			}
			// edge case, only Postgres supports this extreme value for time
			timeValStr = strings.Replace(timeValStr.(string), "24:00:00.000000", "23:59:59.999999", 1)
			t, err := time.Parse("15:04:05.999999", timeValStr.(string))
			t = t.AddDate(1970, 0, 0)
			if err != nil {
				return qvalue.QValue{}, fmt.Errorf("failed to parse time: %w", err)
			}
			val = qvalue.QValue{Kind: qvalue.QValueKindTime, Value: t}
		}
	case qvalue.QValueKindTimeTZ:
		timeVal := value.(string)
		// edge case, Postgres supports this extreme value for time
		timeVal = strings.Replace(timeVal, "24:00:00.000000", "23:59:59.999999", 1)
		// edge case, Postgres prints +0000 as +00
		timeVal = strings.Replace(timeVal, "+00", "+0000", 1)
		t, err := time.Parse("15:04:05.999999-0700", timeVal)
		if err != nil {
			return qvalue.QValue{}, fmt.Errorf("failed to parse time: %w", err)
		}
		t = t.AddDate(1970, 0, 0)
		val = qvalue.QValue{Kind: qvalue.QValueKindTimeTZ, Value: t}

	case qvalue.QValueKindBoolean:
		boolVal := value.(bool)
		val = qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: boolVal}
	case qvalue.QValueKindJSON:
		tmp, err := parseJSON(value)
		if err != nil {
			return qvalue.QValue{}, fmt.Errorf("failed to parse JSON: %w", err)
		}
		val = tmp
	case qvalue.QValueKindInt16:
		intVal := value.(int16)
		val = qvalue.QValue{Kind: qvalue.QValueKindInt16, Value: int32(intVal)}
	case qvalue.QValueKindInt32:
		intVal := value.(int32)
		val = qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: intVal}
	case qvalue.QValueKindInt64:
		intVal := value.(int64)
		val = qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal}
	case qvalue.QValueKindFloat32:
		floatVal := value.(float32)
		val = qvalue.QValue{Kind: qvalue.QValueKindFloat32, Value: floatVal}
	case qvalue.QValueKindFloat64:
		floatVal := value.(float64)
		val = qvalue.QValue{Kind: qvalue.QValueKindFloat64, Value: floatVal}
	case qvalue.QValueKindQChar:
		val = qvalue.QValue{Kind: qvalue.QValueKindQChar, Value: uint8(value.(rune))}
	case qvalue.QValueKindString:
		// handling all unsupported types with strings as well for now.
		val = qvalue.QValue{Kind: qvalue.QValueKindString, Value: fmt.Sprint(value)}
	case qvalue.QValueKindUUID:
		switch value.(type) {
		case string:
			val = qvalue.QValue{Kind: qvalue.QValueKindUUID, Value: value}
		case [16]byte:
			val = qvalue.QValue{Kind: qvalue.QValueKindUUID, Value: value}
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse UUID: %v", value)
		}
	case qvalue.QValueKindINET:
		switch v := value.(type) {
		case string:
			val = qvalue.QValue{Kind: qvalue.QValueKindINET, Value: value}
		case [16]byte:
			val = qvalue.QValue{Kind: qvalue.QValueKindINET, Value: value}
		case netip.Prefix:
			val = qvalue.QValue{Kind: qvalue.QValueKindINET, Value: v.String()}
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse INET: %v", v)
		}
	case qvalue.QValueKindCIDR:
		switch v := value.(type) {
		case string:
			val = qvalue.QValue{Kind: qvalue.QValueKindCIDR, Value: value}
		case [16]byte:
			val = qvalue.QValue{Kind: qvalue.QValueKindCIDR, Value: value}
		case netip.Prefix:
			val = qvalue.QValue{Kind: qvalue.QValueKindCIDR, Value: v.String()}
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse CIDR: %v", value)
		}
	case qvalue.QValueKindMacaddr:
		switch value.(type) {
		case string:
			val = qvalue.QValue{Kind: qvalue.QValueKindMacaddr, Value: value}
		case [16]byte:
			val = qvalue.QValue{Kind: qvalue.QValueKindMacaddr, Value: value}
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse MACADDR: %v", value)
		}
	case qvalue.QValueKindBytes:
		rawBytes := value.([]byte)
		val = qvalue.QValue{Kind: qvalue.QValueKindBytes, Value: rawBytes}
	case qvalue.QValueKindBit:
		bitsVal := value.(pgtype.Bits)
		if bitsVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindBit, Value: bitsVal.Bytes}
		}
	case qvalue.QValueKindNumeric:
		switch numVal := value.(type) {
		case pgtype.Numeric:
			if numVal.Valid {
				rat, err := numericToRat(&numVal)
				if err != nil {
					return qvalue.QValue{}, fmt.Errorf("failed to convert numeric [%v] to rat: %w", value, err)
				}
				val = qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: rat}
			}
		case string:
			rat := new(big.Rat)
			rat, ok := rat.SetString(numVal)
			if !ok {
				return qvalue.QValue{}, fmt.Errorf("failed to convert string [%v] to rat", value)
			}
			val = qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: rat}
		}

	case qvalue.QValueKindArrayFloat32:
		return convertToArray[float32](qvalueKind, value)
	case qvalue.QValueKindArrayFloat64:
		return convertToArray[float64](qvalueKind, value)
	case qvalue.QValueKindArrayInt16:
		return convertToArray[int16](qvalueKind, value)
	case qvalue.QValueKindArrayInt32:
		return convertToArray[int32](qvalueKind, value)
	case qvalue.QValueKindArrayInt64:
		return convertToArray[int64](qvalueKind, value)
	case qvalue.QValueKindArrayDate, qvalue.QValueKindArrayTimestamp, qvalue.QValueKindArrayTimestampTZ:
		return convertToArray[time.Time](qvalueKind, value)
	case qvalue.QValueKindArrayBoolean:
		return convertToArray[bool](qvalueKind, value)
	case qvalue.QValueKindArrayString:
		return convertToArray[string](qvalueKind, value)
	case qvalue.QValueKindPoint:
		xCoord := value.(pgtype.Point).P.X
		yCoord := value.(pgtype.Point).P.Y
		val = qvalue.QValue{
			Kind:  qvalue.QValueKindPoint,
			Value: fmt.Sprintf("POINT(%f %f)", xCoord, yCoord),
		}
	default:
		textVal, ok := value.(string)
		if ok {
			val = qvalue.QValue{Kind: qvalue.QValueKindString, Value: textVal}
		}
	}

	// parsing into pgtype failed.
	if val == (qvalue.QValue{}) {
		return qvalue.QValue{}, fmt.Errorf("failed to parse value %v into QValueKind %v", value, qvalueKind)
	}
	return val, nil
}

func (c *PostgresConnector) parseFieldFromPostgresOID(oid uint32, value interface{}) (qvalue.QValue, error) {
	return ParseFieldFromQValueKind(c.postgresOIDToQValueKind(oid), value)
}

func numericToRat(numVal *pgtype.Numeric) (*big.Rat, error) {
	if numVal.Valid {
		if numVal.NaN {
			// set to nil if NaN
			return nil, nil
		}

		switch numVal.InfinityModifier {
		case pgtype.NegativeInfinity, pgtype.Infinity:
			return nil, nil
		}

		rat := new(big.Rat).SetInt(numVal.Int)
		if numVal.Exp > 0 {
			mul := new(big.Int).Exp(big10, big.NewInt(int64(numVal.Exp)), nil)
			rat.Mul(rat, new(big.Rat).SetInt(mul))
		} else if numVal.Exp < 0 {
			mul := new(big.Int).Exp(big10, big.NewInt(int64(-numVal.Exp)), nil)
			rat.Quo(rat, new(big.Rat).SetInt(mul))
		}
		return rat, nil
	}

	// handle invalid numeric
	return nil, errors.New("invalid numeric")
}

func customTypeToQKind(typeName string) qvalue.QValueKind {
	var qValueKind qvalue.QValueKind
	switch typeName {
	case "geometry":
		qValueKind = qvalue.QValueKindGeometry
	case "geography":
		qValueKind = qvalue.QValueKindGeography
	case "hstore":
		qValueKind = qvalue.QValueKindHStore
	default:
		qValueKind = qvalue.QValueKindString
	}
	return qValueKind
}
