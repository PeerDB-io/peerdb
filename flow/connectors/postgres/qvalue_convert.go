package connpostgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
	"github.com/shopspring/decimal"

	datatypes "github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (c *PostgresConnector) postgresOIDToName(recvOID uint32) string {
	if ty, ok := pgtype.NewMap().TypeForOID(recvOID); ok {
		return ty.Name
	}
	// workaround for some types not being defined by pgtype
	switch recvOID {
	case uint32(oid.T_timetz):
		return "timetz"
	case uint32(oid.T_xml):
		return "xml"
	case uint32(oid.T_money):
		return "money"
	case uint32(oid.T_txid_snapshot):
		return "txid_snapshot"
	case uint32(oid.T_tsvector):
		return "tsvector"
	case uint32(oid.T_tsquery):
		return "tsquery"
	default:
		return ""
	}
}

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
	case pgtype.JSONOID:
		return qvalue.QValueKindJSON
	case pgtype.JSONBOID:
		return qvalue.QValueKindJSONB
	case pgtype.UUIDOID:
		return qvalue.QValueKindUUID
	case pgtype.UUIDArrayOID:
		return qvalue.QValueKindArrayUUID
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
	case pgtype.JSONArrayOID:
		return qvalue.QValueKindArrayJSON
	case pgtype.JSONBArrayOID:
		return qvalue.QValueKindArrayJSONB
	case pgtype.IntervalOID:
		return qvalue.QValueKindInterval
	case pgtype.TstzrangeOID:
		return qvalue.QValueKindTSTZRange
	default:
		typeName, ok := pgtype.NewMap().TypeForOID(recvOID)
		if !ok {
			// workaround for some types not being defined by pgtype
			switch recvOID {
			case uint32(oid.T_timetz):
				return qvalue.QValueKindTimeTZ
			case uint32(oid.T_xml):
				return qvalue.QValueKindString
			case uint32(oid.T_money):
				return qvalue.QValueKindString
			case uint32(oid.T_txid_snapshot):
				return qvalue.QValueKindString
			case uint32(oid.T_tsvector):
				return qvalue.QValueKindString
			case uint32(oid.T_tsquery):
				return qvalue.QValueKindString
			case uint32(oid.T_point):
				return qvalue.QValueKindPoint
			default:
				return qvalue.QValueKindInvalid
			}
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
	case qvalue.QValueKindJSONB:
		return "JSONB"
	case qvalue.QValueKindHStore:
		return "HSTORE"
	case qvalue.QValueKindUUID:
		return "UUID"
	case qvalue.QValueKindArrayUUID:
		return "UUID[]"
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
	case qvalue.QValueKindArrayJSON:
		return "JSON[]"
	case qvalue.QValueKindArrayJSONB:
		return "JSONB[]"
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

func parseJSON(value interface{}, isArray bool) (qvalue.QValue, error) {
	jsonVal, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return qvalue.QValueJSON{Val: string(jsonVal), IsArray: isArray}, nil
}

func parseUUID(value interface{}) (uuid.UUID, error) {
	switch v := value.(type) {
	case string:
		return uuid.Parse(v)
	case [16]byte:
		return uuid.UUID(v), nil
	case uuid.UUID:
		return v, nil
	default:
		return uuid.UUID{}, fmt.Errorf("unsupported type for UUID: %T", value)
	}
}

func parseUUIDArray(value interface{}) (qvalue.QValue, error) {
	switch v := value.(type) {
	case []string:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, s := range v {
			id, err := uuid.Parse(s)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID in array: %w", err)
			}
			uuids = append(uuids, id)
		}
		return qvalue.QValueArrayUUID{Val: uuids}, nil
	case [][16]byte:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, v := range v {
			uuids = append(uuids, uuid.UUID(v))
		}
		return qvalue.QValueArrayUUID{Val: uuids}, nil
	case []uuid.UUID:
		return qvalue.QValueArrayUUID{Val: v}, nil
	case []interface{}:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, v := range v {
			id, err := parseUUID(v)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID interface{} value in array: %w", err)
			}
			uuids = append(uuids, id)
		}
		return qvalue.QValueArrayUUID{Val: uuids}, nil
	default:
		return nil, fmt.Errorf("unsupported type for UUID array: %T", value)
	}
}

func convertToArray[T any](kind qvalue.QValueKind, value interface{}) ([]T, error) {
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

func parseFieldFromQValueKind(qvalueKind qvalue.QValueKind, value interface{}) (qvalue.QValue, error) {
	if value == nil {
		return qvalue.QValueNull(qvalueKind), nil
	}

	switch qvalueKind {
	case qvalue.QValueKindTimestamp:
		switch val := value.(type) {
		case time.Time:
			return qvalue.QValueTimestamp{Val: val}, nil
		case pgtype.InfinityModifier:
			return qvalue.QValueNull(qvalueKind), nil
		}
	case qvalue.QValueKindTimestampTZ:
		switch val := value.(type) {
		case time.Time:
			return qvalue.QValueTimestampTZ{Val: val}, nil
		case pgtype.InfinityModifier:
			return qvalue.QValueNull(qvalueKind), nil
		}
	case qvalue.QValueKindInterval:
		intervalObject := value.(pgtype.Interval)
		var interval datatypes.PeerDBInterval
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
	case qvalue.QValueKindTSTZRange:
		tstzrangeObject := value.(pgtype.Range[interface{}])
		lowerBoundType := tstzrangeObject.LowerType
		upperBoundType := tstzrangeObject.UpperType
		lowerTime, err := convertTimeRangeBound(tstzrangeObject.Lower)
		if err != nil {
			return nil, fmt.Errorf("[tstzrange]error for lower time bound: %w", err)
		}

		upperTime, err := convertTimeRangeBound(tstzrangeObject.Upper)
		if err != nil {
			return nil, fmt.Errorf("[tstzrange]error for upper time bound: %w", err)
		}

		lowerBracket := "["
		if lowerBoundType == pgtype.Exclusive {
			lowerBracket = "("
		}
		upperBracket := "]"
		if upperBoundType == pgtype.Exclusive {
			upperBracket = ")"
		}
		tstzrangeStr := fmt.Sprintf("%s%v,%v%s",
			lowerBracket, lowerTime, upperTime, upperBracket)
		return qvalue.QValueTSTZRange{Val: tstzrangeStr}, nil
	case qvalue.QValueKindDate:
		switch val := value.(type) {
		case time.Time:
			return qvalue.QValueDate{Val: val}, nil
		case pgtype.InfinityModifier:
			return qvalue.QValueNull(qvalueKind), nil
		}
	case qvalue.QValueKindTime:
		timeVal := value.(pgtype.Time)
		if timeVal.Valid {
			// 86399999999 to prevent 24:00:00
			return qvalue.QValueTime{Val: time.UnixMicro(min(timeVal.Microseconds, 86399999999))}, nil
		}
	case qvalue.QValueKindTimeTZ:
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

	case qvalue.QValueKindBoolean:
		boolVal := value.(bool)
		return qvalue.QValueBoolean{Val: boolVal}, nil
	case qvalue.QValueKindJSON, qvalue.QValueKindJSONB:
		tmp, err := parseJSON(value, false)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		return tmp, nil
	case qvalue.QValueKindArrayJSON, qvalue.QValueKindArrayJSONB:
		tmp, err := parseJSON(value, true)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON Array: %w", err)
		}
		return tmp, nil
	case qvalue.QValueKindInt16:
		intVal := value.(int16)
		return qvalue.QValueInt16{Val: intVal}, nil
	case qvalue.QValueKindInt32:
		intVal := value.(int32)
		return qvalue.QValueInt32{Val: intVal}, nil
	case qvalue.QValueKindInt64:
		intVal := value.(int64)
		return qvalue.QValueInt64{Val: intVal}, nil
	case qvalue.QValueKindFloat32:
		floatVal := value.(float32)
		return qvalue.QValueFloat32{Val: floatVal}, nil
	case qvalue.QValueKindFloat64:
		floatVal := value.(float64)
		return qvalue.QValueFloat64{Val: floatVal}, nil
	case qvalue.QValueKindQChar:
		return qvalue.QValueQChar{Val: uint8(value.(rune))}, nil
	case qvalue.QValueKindString:
		// handling all unsupported types with strings as well for now.
		return qvalue.QValueString{Val: fmt.Sprint(value)}, nil
	case qvalue.QValueKindUUID:
		tmp, err := parseUUID(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse UUID: %w", err)
		}
		return qvalue.QValueUUID{Val: tmp}, nil
	case qvalue.QValueKindArrayUUID:
		tmp, err := parseUUIDArray(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse UUID array: %w", err)
		}
		return tmp, nil
	case qvalue.QValueKindINET:
		switch v := value.(type) {
		case string:
			return qvalue.QValueINET{Val: v}, nil
		case netip.Prefix:
			return qvalue.QValueINET{Val: v.String()}, nil
		default:
			return nil, fmt.Errorf("failed to parse INET: %v", v)
		}
	case qvalue.QValueKindCIDR:
		switch v := value.(type) {
		case string:
			return qvalue.QValueCIDR{Val: v}, nil
		case netip.Prefix:
			return qvalue.QValueCIDR{Val: v.String()}, nil
		default:
			return nil, fmt.Errorf("failed to parse CIDR: %v", value)
		}
	case qvalue.QValueKindMacaddr:
		switch v := value.(type) {
		case string:
			return qvalue.QValueMacaddr{Val: v}, nil
		case net.HardwareAddr:
			return qvalue.QValueMacaddr{Val: v.String()}, nil
		default:
			return nil, fmt.Errorf("failed to parse MACADDR: %v %T", value, v)
		}
	case qvalue.QValueKindBytes:
		rawBytes := value.([]byte)
		return qvalue.QValueBytes{Val: rawBytes}, nil
	case qvalue.QValueKindNumeric:
		numVal := value.(pgtype.Numeric)
		if numVal.Valid {
			num, err := numericToDecimal(numVal)
			if err != nil {
				return nil, fmt.Errorf("failed to convert numeric [%v] to decimal: %w", value, err)
			}
			return num, nil
		}
	case qvalue.QValueKindArrayFloat32:
		a, err := convertToArray[float32](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return qvalue.QValueArrayFloat32{Val: a}, nil
	case qvalue.QValueKindArrayFloat64:
		a, err := convertToArray[float64](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return qvalue.QValueArrayFloat64{Val: a}, nil
	case qvalue.QValueKindArrayInt16:
		a, err := convertToArray[int16](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return qvalue.QValueArrayInt16{Val: a}, nil
	case qvalue.QValueKindArrayInt32:
		a, err := convertToArray[int32](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return qvalue.QValueArrayInt32{Val: a}, nil
	case qvalue.QValueKindArrayInt64:
		a, err := convertToArray[int64](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return qvalue.QValueArrayInt64{Val: a}, nil
	case qvalue.QValueKindArrayDate, qvalue.QValueKindArrayTimestamp, qvalue.QValueKindArrayTimestampTZ:
		a, err := convertToArray[time.Time](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		switch qvalueKind {
		case qvalue.QValueKindArrayDate:
			return qvalue.QValueArrayDate{Val: a}, nil
		case qvalue.QValueKindArrayTimestamp:
			return qvalue.QValueArrayTimestamp{Val: a}, nil
		case qvalue.QValueKindArrayTimestampTZ:
			return qvalue.QValueArrayTimestampTZ{Val: a}, nil
		}
	case qvalue.QValueKindArrayBoolean:
		a, err := convertToArray[bool](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return qvalue.QValueArrayBoolean{Val: a}, nil
	case qvalue.QValueKindArrayString:
		a, err := convertToArray[string](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return qvalue.QValueArrayString{Val: a}, nil
	case qvalue.QValueKindPoint:
		coord := value.(pgtype.Point).P
		return qvalue.QValuePoint{
			Val: fmt.Sprintf("POINT(%f %f)", coord.X, coord.Y),
		}, nil
	default:
		textVal, ok := value.(string)
		if ok {
			return qvalue.QValueString{Val: textVal}, nil
		}
	}

	// parsing into pgtype failed.
	return nil, fmt.Errorf("failed to parse value %v into QValueKind %v", value, qvalueKind)
}

func (c *PostgresConnector) parseFieldFromPostgresOID(oid uint32, value interface{}) (qvalue.QValue, error) {
	return parseFieldFromQValueKind(c.postgresOIDToQValueKind(oid), value)
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
	switch typeName {
	case "geometry":
		return qvalue.QValueKindGeometry
	case "geography":
		return qvalue.QValueKindGeography
	case "hstore":
		return qvalue.QValueKindHStore
	default:
		return qvalue.QValueKindString
	}
}

// Postgres does not like timestamps of the form 2006-01-02 15:04:05 +0000 UTC
// in tstzrange.
// convertTimeRangeBound removes the +0000 UTC part
func convertTimeRangeBound(timeBound interface{}) (string, error) {
	layout := "2006-01-02 15:04:05 -0700 MST"
	postgresFormat := "2006-01-02 15:04:05"
	var convertedTime string
	if timeBound != nil {
		lowerParsed, err := time.Parse(layout, fmt.Sprint(timeBound))
		if err != nil {
			return "", fmt.Errorf("unexpected lower bound value in tstzrange. Error: %v", err)
		}
		convertedTime = lowerParsed.Format(postgresFormat)
	} else {
		convertedTime = ""
	}

	return convertedTime, nil
}
