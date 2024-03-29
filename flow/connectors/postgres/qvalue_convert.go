package connpostgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
	"github.com/shopspring/decimal"

	peerdb_interval "github.com/PeerDB-io/peer-flow/interval"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (c *PostgresConnector) postgresOIDToQType(recvOID uint32) qvalue.QType {
	switch recvOID {
	case pgtype.BoolOID:
		return qvalue.QType{Kind: qvalue.QValueKindBoolean}
	case pgtype.Int2OID:
		return qvalue.QType{Kind: qvalue.QValueKindInt16}
	case pgtype.Int4OID:
		return qvalue.QType{Kind: qvalue.QValueKindInt32}
	case pgtype.Int8OID:
		return qvalue.QType{Kind: qvalue.QValueKindInt64}
	case pgtype.Float4OID:
		return qvalue.QType{Kind: qvalue.QValueKindFloat32}
	case pgtype.Float8OID:
		return qvalue.QType{Kind: qvalue.QValueKindFloat64}
	case pgtype.QCharOID:
		return qvalue.QType{Kind: qvalue.QValueKindQChar}
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return qvalue.QType{Kind: qvalue.QValueKindString}
	case pgtype.ByteaOID:
		return qvalue.QType{Kind: qvalue.QValueKindBytes}
	case pgtype.JSONOID, pgtype.JSONBOID:
		return qvalue.QType{Kind: qvalue.QValueKindJSON}
	case pgtype.UUIDOID:
		return qvalue.QType{Kind: qvalue.QValueKindUUID}
	case pgtype.TimeOID:
		return qvalue.QType{Kind: qvalue.QValueKindTime}
	case pgtype.DateOID:
		return qvalue.QType{Kind: qvalue.QValueKindDate}
	case pgtype.CIDROID:
		return qvalue.QType{Kind: qvalue.QValueKindCIDR}
	case pgtype.MacaddrOID:
		return qvalue.QType{Kind: qvalue.QValueKindMacaddr}
	case pgtype.InetOID:
		return qvalue.QType{Kind: qvalue.QValueKindINET}
	case pgtype.TimestampOID:
		return qvalue.QType{Kind: qvalue.QValueKindTimestamp}
	case pgtype.TimestamptzOID:
		return qvalue.QType{Kind: qvalue.QValueKindTimestampTZ}
	case pgtype.NumericOID:
		return qvalue.QType{Kind: qvalue.QValueKindNumeric}
	case pgtype.BitOID, pgtype.VarbitOID:
		return qvalue.QType{Kind: qvalue.QValueKindBit}
	case pgtype.Int2ArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayInt16}
	case pgtype.Int4ArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayInt32}
	case pgtype.Int8ArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayInt64}
	case pgtype.PointOID:
		return qvalue.QType{Kind: qvalue.QValueKindPoint}
	case pgtype.Float4ArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayFloat32}
	case pgtype.Float8ArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayFloat64}
	case pgtype.BoolArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayBoolean}
	case pgtype.DateArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayDate}
	case pgtype.TimestampArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayTimestamp}
	case pgtype.TimestamptzArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayTimestampTZ}
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID:
		return qvalue.QType{Kind: qvalue.QValueKindArrayString}
	case pgtype.IntervalOID:
		return qvalue.QType{Kind: qvalue.QValueKindInterval}
	default:
		typeName, ok := pgtype.NewMap().TypeForOID(recvOID)
		if !ok {
			// workaround for some types not being defined by pgtype
			if recvOID == uint32(oid.T_timetz) {
				return qvalue.QType{Kind: qvalue.QValueKindTimeTZ}
			} else if recvOID == uint32(oid.T_xml) { // XML
				return qvalue.QType{Kind: qvalue.QValueKindString}
			} else if recvOID == uint32(oid.T_money) { // MONEY
				return qvalue.QType{Kind: qvalue.QValueKindString}
			} else if recvOID == uint32(oid.T_txid_snapshot) { // TXID_SNAPSHOT
				return qvalue.QType{Kind: qvalue.QValueKindString}
			} else if recvOID == uint32(oid.T_tsvector) { // TSVECTOR
				return qvalue.QType{Kind: qvalue.QValueKindString}
			} else if recvOID == uint32(oid.T_tsquery) { // TSQUERY
				return qvalue.QType{Kind: qvalue.QValueKindString}
			} else if recvOID == uint32(oid.T_point) { // POINT
				return qvalue.QType{Kind: qvalue.QValueKindPoint}
			}

			return qvalue.QType{Kind: qvalue.QValueKindInvalid}
		} else {
			_, warned := c.hushWarnOID[recvOID]
			if !warned {
				c.logger.Warn(fmt.Sprintf("unsupported field type: %d - type name - %s; returning as string", recvOID, typeName.Name))
				c.hushWarnOID[recvOID] = struct{}{}
			}
			return qvalue.QType{Kind: qvalue.QValueKindString}
		}
	}
}

func parseKindToPostgresType(colType string) string {
	if ok, array := strings.CutPrefix(colType, "array_"); ok {
		return parseKindToPostgresType(array) + "[]"
	}
	switch qvalue.ParseQKind(colType) {
	case qvalue.QKindBoolean:
		return "BOOLEAN"
	case qvalue.QKindInt16:
		return "SMALLINT"
	case qvalue.QKindInt32:
		return "INTEGER"
	case qvalue.QKindInt64:
		return "BIGINT"
	case qvalue.QKindFloat32:
		return "REAL"
	case qvalue.QKindFloat64:
		return "DOUBLE PRECISION"
	case qvalue.QKindQChar:
		return "\"char\""
	case qvalue.QKindString:
		return "TEXT"
	case qvalue.QKindBytes:
		return "BYTEA"
	case qvalue.QKindJSON:
		return "JSON"
	case qvalue.QKindHStore:
		return "HSTORE"
	case qvalue.QKindUUID:
		return "UUID"
	case qvalue.QKindTime:
		return "TIME"
	case qvalue.QKindTimeTZ:
		return "TIMETZ"
	case qvalue.QKindDate:
		return "DATE"
	case qvalue.QKindTimestamp:
		return "TIMESTAMP"
	case qvalue.QKindTimestampTZ:
		return "TIMESTAMPTZ"
	case qvalue.QKindNumeric:
		return "NUMERIC"
	case qvalue.QKindBit:
		return "BIT"
	case qvalue.QKindINET:
		return "INET"
	case qvalue.QKindCIDR:
		return "CIDR"
	case qvalue.QKindMacaddr:
		return "MACADDR"
	case qvalue.QKindArrayInt16:
		return "SMALLINT[]"
	case qvalue.QKindArrayInt32:
		return "INTEGER[]"
	case qvalue.QKindArrayInt64:
		return "BIGINT[]"
	case qvalue.QKindArrayFloat32:
		return "REAL[]"
	case qvalue.QKindArrayFloat64:
		return "DOUBLE PRECISION[]"
	case qvalue.QKindArrayDate:
		return "DATE[]"
	case qvalue.QKindArrayTimestamp:
		return "TIMESTAMP[]"
	case qvalue.QKindArrayTimestampTZ:
		return "TIMESTAMPTZ[]"
	case qvalue.QKindArrayBoolean:
		return "BOOLEAN[]"
	case qvalue.QKindArrayString:
		return "TEXT[]"
	case qvalue.QKindGeography:
		return "GEOGRAPHY"
	case qvalue.QKindGeometry:
		return "GEOMETRY"
	case qvalue.QKindPoint:
		return "POINT"
	default:
		return "TEXT"
	}
}

func parseJSON(value interface{}) (qvalue.QValue, error) {
	jsonVal, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return qvalue.QValueJSON{Val: string(jsonVal)}, nil
}

func convertToArray[T any](kind qvalue.QKind, value interface{}) ([]T, error) {
	switch v := value.(type) {
	case pgtype.Array[T]:
		if v.Valid {
			return v.Elements, nil
		}
	case []T:
		return v, nil
	case []interface{}:
		return shared.ArrayCastElements[T](v), nil
	}
	return nil, fmt.Errorf("failed to parse array %s from %T: %v", kind, value, value)
}

func parseFieldWithQType(qt qvalue.QType, value interface{}) (qvalue.QValue, error) {
	if value == nil {
		return qvalue.QValueNull(kind), nil
	}
	if qt.Array > 0 {
		switch qt.Kind {
		case qvalue.QKindFloat32:
			a, err := convertToArray[float32](qt.Kind, value)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueArrayFloat32{Val: a}, nil
		case qvalue.QKindFloat64:
			a, err := convertToArray[float64](qt.Kind, value)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueArrayFloat64{Val: a}, nil
		case qvalue.QKindInt16:
			a, err := convertToArray[int16](qt.Kind, value)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueArrayInt16{Val: a}, nil
		case qvalue.QKindInt32:
			a, err := convertToArray[int32](qt.Kind, value)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueArrayInt32{Val: a}, nil
		case qvalue.QKindInt64:
			a, err := convertToArray[int64](qt.Kind, value)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueArrayInt64{Val: a}, nil
		case qvalue.QKindDate, qvalue.QKindTimestamp, qvalue.QKindTimestampTZ:
			a, err := convertToArray[time.Time](qt.Kind, value)
			if err != nil {
				return nil, err
			}
			switch qt.Kind {
			case qvalue.QKindDate:
				return qvalue.QValueArrayDate{Val: a}, nil
			case qvalue.QKindTimestamp:
				return qvalue.QValueArrayTimestamp{Val: a}, nil
			case qvalue.QKindTimestampTZ:
				return qvalue.QValueArrayTimestampTZ{Val: a}, nil
			}
		case qvalue.QKindBoolean:
			a, err := convertToArray[bool](qt.Kind, value)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueArrayBoolean{Val: a}, nil
		case qvalue.QKindString:
			a, err := convertToArray[string](qt.Kind, value)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueArrayString{Val: a}, nil
		}
	} else {
		switch qt.Kind {
		case qvalue.QKindTimestamp:
			timestamp := value.(time.Time)
			return qvalue.QValueTimestamp{Val: timestamp}, nil
		case qvalue.QKindTimestampTZ:
			timestamp := value.(time.Time)
			return qvalue.QValueTimestampTZ{Val: timestamp}, nil
		case qvalue.QKindInterval:
			intervalObject := value.(pgtype.Interval)
			var interval peerdb_interval.PeerDBInterval
			interval.Hours = int(intervalObject.Microseconds / 3600000000)
			interval.Minutes = int((intervalObject.Microseconds % 3600000000) / 60000000)
			interval.Seconds = float64(intervalObject.Microseconds%60000000) / 1000000.0
			interval.Days = int(intervalObject.Days)
			interval.Years = int(intervalObject.Months / 12)
			interval.Months = int(intervalObject.Months % 12)
			interval.Valid = intervalObject.Valid

			intervalJSON, err := json.Marshal(interval)
			if err != nil {
				return nil, fmt.Errorf("failed to parse interval: %w", err)
			}

			if !interval.Valid {
				return nil, fmt.Errorf("invalid interval: %v", value)
			}

			return qvalue.QValueString{Val: string(intervalJSON)}, nil
		case qvalue.QKindDate:
			date := value.(time.Time)
			return qvalue.QValueDate{Val: date}, nil
		case qvalue.QKindTime:
			timeVal := value.(pgtype.Time)
			if timeVal.Valid {
				// 86399999999 to prevent 24:00:00
				return qvalue.QValueTime{Val: time.UnixMicro(min(timeVal.Microseconds, 86399999999))}, nil
			}
		case qvalue.QKindTimeTZ:
			timeVal := value.(string)
			// edge case, Postgres supports this extreme value for time
			timeVal = strings.Replace(timeVal, "24:00:00.000000", "23:59:59.999999", 1)
			// edge case, Postgres prints +0000 as +00
			timeVal = strings.Replace(timeVal, "+00", "+0000", 1)
			t, err := time.Parse("15:04:05.999999-0700", timeVal)
			if err != nil {
				return nil, fmt.Errorf("failed to parse time: %w", err)
			}
			t = t.AddDate(1970, 0, 0)
			return qvalue.QValueTimeTZ{Val: t}, nil

		case qvalue.QKindBoolean:
			boolVal := value.(bool)
			return qvalue.QValueBoolean{Val: boolVal}, nil
		case qvalue.QKindJSON:
			tmp, err := parseJSON(value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse JSON: %w", err)
			}
			return tmp, nil
		case qvalue.QKindInt16:
			intVal := value.(int16)
			return qvalue.QValueInt16{Val: intVal}, nil
		case qvalue.QKindInt32:
			intVal := value.(int32)
			return qvalue.QValueInt32{Val: intVal}, nil
		case qvalue.QKindInt64:
			intVal := value.(int64)
			return qvalue.QValueInt64{Val: intVal}, nil
		case qvalue.QKindFloat32:
			floatVal := value.(float32)
			return qvalue.QValueFloat32{Val: floatVal}, nil
		case qvalue.QKindFloat64:
			floatVal := value.(float64)
			return qvalue.QValueFloat64{Val: floatVal}, nil
		case qvalue.QKindQChar:
			return qvalue.QValueQChar{Val: uint8(value.(rune))}, nil
		case qvalue.QKindString:
			// handling all unsupported types with strings as well for now.
			return qvalue.QValueString{Val: fmt.Sprint(value)}, nil
		case qvalue.QKindUUID:
			switch v := value.(type) {
			case string:
				id, err := uuid.Parse(v)
				if err != nil {
					return nil, fmt.Errorf("failed to parse UUID: %w", err)
				}
				return qvalue.QValueUUID{Val: [16]byte(id)}, nil
			case [16]byte:
				return qvalue.QValueUUID{Val: v}, nil
			default:
				return nil, fmt.Errorf("failed to parse UUID: %v", value)
			}
		case qvalue.QKindINET:
			switch v := value.(type) {
			case string:
				return qvalue.QValueINET{Val: v}, nil
			case [16]byte:
				return qvalue.QValueINET{Val: string(v[:])}, nil
			case netip.Prefix:
				return qvalue.QValueINET{Val: v.String()}, nil
			default:
				return nil, fmt.Errorf("failed to parse INET: %v", v)
			}
		case qvalue.QKindCIDR:
			switch v := value.(type) {
			case string:
				return qvalue.QValueCIDR{Val: v}, nil
			case [16]byte:
				return qvalue.QValueCIDR{Val: string(v[:])}, nil
			case netip.Prefix:
				return qvalue.QValueCIDR{Val: v.String()}, nil
			default:
				return nil, fmt.Errorf("failed to parse CIDR: %v", value)
			}
		case qvalue.QKindMacaddr:
			switch v := value.(type) {
			case string:
				return qvalue.QValueMacaddr{Val: v}, nil
			case [16]byte:
				return qvalue.QValueMacaddr{Val: string(v[:])}, nil
			default:
				return nil, fmt.Errorf("failed to parse MACADDR: %v", value)
			}
		case qvalue.QKindBytes:
			rawBytes := value.([]byte)
			return qvalue.QValueBytes{Val: rawBytes}, nil
		case qvalue.QKindBit:
			bitsVal := value.(pgtype.Bits)
			if bitsVal.Valid {
				return qvalue.QValueBit{Val: bitsVal.Bytes}, nil
			}
		case qvalue.QKindNumeric:
			numVal := value.(pgtype.Numeric)
			if numVal.Valid {
				num, err := numericToDecimal(numVal)
				if err != nil {
					return nil, fmt.Errorf("failed to convert numeric [%v] to decimal: %w", value, err)
				}
				return num, nil
			}
		case qvalue.QKindPoint:
			xCoord := value.(pgtype.Point).P.X
			yCoord := value.(pgtype.Point).P.Y
			return qvalue.QValuePoint{
				Val: fmt.Sprintf("POINT(%f %f)", xCoord, yCoord),
			}, nil
		default:
			textVal, ok := value.(string)
			if ok {
				return qvalue.QValueString{Val: textVal}, nil
			}
		}
	}

	// parsing into pgtype failed.
	return nil, fmt.Errorf("failed to parse value %v into QKind %s", value, qt.Kind)
}

func (c *PostgresConnector) parseFieldFromPostgresOID(oid uint32, value interface{}) (qvalue.QValue, error) {
	return parseFieldWithQType(c.postgresOIDToQType(oid), value)
}

func numericToDecimal(numVal pgtype.Numeric) (qvalue.QValue, error) {
	switch {
	case !numVal.Valid:
		return qvalue.QValueNull(qvalue.QValueKindNumeric), errors.New("invalid numeric")
	case numVal.NaN, numVal.InfinityModifier == pgtype.Infinity,
		numVal.InfinityModifier == pgtype.NegativeInfinity:
		return qvalue.QValueNull(qvalue.QValueKindNumeric), nil
	default:
		return qvalue.QValueNumeric{Val: decimal.NewFromBigInt(numVal.Int, numVal.Exp)}, nil
	}
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
