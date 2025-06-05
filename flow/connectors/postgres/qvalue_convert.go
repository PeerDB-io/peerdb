package connpostgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
	"github.com/shopspring/decimal"

	datatypes "github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *PostgresConnector) postgresOIDToName(recvOID uint32, customTypeMapping map[uint32]shared.CustomDataType) (string, error) {
	if ty, ok := c.typeMap.TypeForOID(recvOID); ok {
		return ty.Name, nil
	}
	// workaround for some types not being defined by pgtype
	switch oid.Oid(recvOID) {
	case oid.T_timetz:
		return "timetz", nil
	case oid.T_xml:
		return "xml", nil
	case oid.T_money:
		return "money", nil
	case oid.T_txid_snapshot:
		return "txid_snapshot", nil
	case oid.T_tsvector:
		return "tsvector", nil
	case oid.T_tsquery:
		return "tsquery", nil
	default:
		typeData, ok := customTypeMapping[recvOID]
		if !ok {
			return "", fmt.Errorf("error getting type name for %d", recvOID)
		}
		return typeData.Name, nil
	}
}

func (c *PostgresConnector) postgresOIDToQValueKind(
	recvOID uint32,
	customTypeMapping map[uint32]shared.CustomDataType,
) types.QValueKind {
	switch recvOID {
	case pgtype.BoolOID:
		return types.QValueKindBoolean
	case pgtype.Int2OID:
		return types.QValueKindInt16
	case pgtype.Int4OID:
		return types.QValueKindInt32
	case pgtype.Int8OID:
		return types.QValueKindInt64
	case pgtype.Float4OID:
		return types.QValueKindFloat32
	case pgtype.Float8OID:
		return types.QValueKindFloat64
	case pgtype.QCharOID:
		return types.QValueKindQChar
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return types.QValueKindString
	case pgtype.ByteaOID:
		return types.QValueKindBytes
	case pgtype.JSONOID:
		return types.QValueKindJSON
	case pgtype.JSONBOID:
		return types.QValueKindJSONB
	case pgtype.UUIDOID:
		return types.QValueKindUUID
	case pgtype.TimeOID:
		return types.QValueKindTime
	case pgtype.DateOID:
		return types.QValueKindDate
	case pgtype.CIDROID:
		return types.QValueKindCIDR
	case pgtype.MacaddrOID:
		return types.QValueKindMacaddr
	case pgtype.InetOID:
		return types.QValueKindINET
	case pgtype.TimestampOID:
		return types.QValueKindTimestamp
	case pgtype.TimestamptzOID:
		return types.QValueKindTimestampTZ
	case pgtype.NumericOID:
		return types.QValueKindNumeric
	case pgtype.Int2ArrayOID:
		return types.QValueKindArrayInt16
	case pgtype.Int4ArrayOID:
		return types.QValueKindArrayInt32
	case pgtype.Int8ArrayOID:
		return types.QValueKindArrayInt64
	case pgtype.PointOID:
		return types.QValueKindPoint
	case pgtype.Float4ArrayOID:
		return types.QValueKindArrayFloat32
	case pgtype.Float8ArrayOID:
		return types.QValueKindArrayFloat64
	case pgtype.BoolArrayOID:
		return types.QValueKindArrayBoolean
	case pgtype.DateArrayOID:
		return types.QValueKindArrayDate
	case pgtype.TimestampArrayOID:
		return types.QValueKindArrayTimestamp
	case pgtype.TimestamptzArrayOID:
		return types.QValueKindArrayTimestampTZ
	case pgtype.UUIDArrayOID:
		return types.QValueKindArrayUUID
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID:
		return types.QValueKindArrayString
	case pgtype.JSONArrayOID:
		return types.QValueKindArrayJSON
	case pgtype.JSONBArrayOID:
		return types.QValueKindArrayJSONB
	case pgtype.NumericArrayOID:
		return types.QValueKindArrayNumeric
	case pgtype.IntervalOID:
		return types.QValueKindInterval
	case pgtype.TstzrangeOID:
		return types.QValueKindTSTZRange
	default:
		if typeName, ok := c.typeMap.TypeForOID(recvOID); ok {
			colType := types.QValueKindString
			if typeData, ok := customTypeMapping[recvOID]; ok {
				colType = customTypeToQKind(typeData)
			}
			if _, warned := c.hushWarnOID[recvOID]; !warned {
				c.logger.Warn("unsupported field type",
					slog.Int64("oid", int64(recvOID)), slog.String("typeName", typeName.Name), slog.String("mapping", string(colType)))
				c.hushWarnOID[recvOID] = struct{}{}
			}
			return colType
		} else {
			// workaround for some types not being defined by pgtype
			switch oid.Oid(recvOID) {
			case oid.T_timetz:
				return types.QValueKindTimeTZ
			case oid.T_point:
				return types.QValueKindPoint
			default:
				if typeData, ok := customTypeMapping[recvOID]; ok {
					return customTypeToQKind(typeData)
				}
				return types.QValueKindString
			}
		}
	}
}

func qValueKindToPostgresType(colTypeStr string) string {
	switch types.QValueKind(colTypeStr) {
	case types.QValueKindBoolean:
		return "BOOLEAN"
	case types.QValueKindInt16, types.QValueKindUInt16, types.QValueKindInt8, types.QValueKindUInt8:
		return "SMALLINT"
	case types.QValueKindInt32, types.QValueKindUInt32:
		return "INTEGER"
	case types.QValueKindInt64, types.QValueKindUInt64:
		return "BIGINT"
	case types.QValueKindFloat32:
		return "REAL"
	case types.QValueKindFloat64:
		return "DOUBLE PRECISION"
	case types.QValueKindQChar:
		return "\"char\""
	case types.QValueKindString, types.QValueKindEnum:
		return "TEXT"
	case types.QValueKindBytes:
		return "BYTEA"
	case types.QValueKindJSON:
		return "JSON"
	case types.QValueKindJSONB:
		return "JSONB"
	case types.QValueKindHStore:
		return "HSTORE"
	case types.QValueKindUUID:
		return "UUID"
	case types.QValueKindArrayUUID:
		return "UUID[]"
	case types.QValueKindTime:
		return "TIME"
	case types.QValueKindTimeTZ:
		return "TIMETZ"
	case types.QValueKindDate:
		return "DATE"
	case types.QValueKindTimestamp:
		return "TIMESTAMP"
	case types.QValueKindTimestampTZ:
		return "TIMESTAMPTZ"
	case types.QValueKindNumeric:
		return "NUMERIC"
	case types.QValueKindINET:
		return "INET"
	case types.QValueKindCIDR:
		return "CIDR"
	case types.QValueKindMacaddr:
		return "MACADDR"
	case types.QValueKindArrayInt16:
		return "SMALLINT[]"
	case types.QValueKindArrayInt32:
		return "INTEGER[]"
	case types.QValueKindArrayInt64:
		return "BIGINT[]"
	case types.QValueKindArrayFloat32:
		return "REAL[]"
	case types.QValueKindArrayFloat64:
		return "DOUBLE PRECISION[]"
	case types.QValueKindArrayDate:
		return "DATE[]"
	case types.QValueKindArrayTimestamp:
		return "TIMESTAMP[]"
	case types.QValueKindArrayTimestampTZ:
		return "TIMESTAMPTZ[]"
	case types.QValueKindArrayBoolean:
		return "BOOLEAN[]"
	case types.QValueKindArrayString, types.QValueKindArrayEnum:
		return "TEXT[]"
	case types.QValueKindArrayJSON:
		return "JSON[]"
	case types.QValueKindArrayJSONB:
		return "JSONB[]"
	case types.QValueKindArrayNumeric:
		return "NUMERIC[]"
	case types.QValueKindGeography:
		return "GEOGRAPHY"
	case types.QValueKindGeometry:
		return "GEOMETRY"
	case types.QValueKindPoint:
		return "POINT"
	default:
		return "TEXT"
	}
}

func parseJSON(value any, isArray bool) (types.QValue, error) {
	jsonVal, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return types.QValueJSON{Val: string(jsonVal), IsArray: isArray}, nil
}

func parseUUID(value any) (uuid.UUID, error) {
	switch v := value.(type) {
	case string:
		return uuid.Parse(v)
	case [16]byte:
		return uuid.UUID(v), nil
	case uuid.UUID:
		return v, nil
	case nil:
		return uuid.UUID{}, nil
	default:
		return uuid.UUID{}, fmt.Errorf("unsupported type for UUID: %T", value)
	}
}

func parseUUIDArray(value any) (types.QValue, error) {
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
		return types.QValueArrayUUID{Val: uuids}, nil
	case [][16]byte:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, v := range v {
			uuids = append(uuids, uuid.UUID(v))
		}
		return types.QValueArrayUUID{Val: uuids}, nil
	case []uuid.UUID:
		return types.QValueArrayUUID{Val: v}, nil
	case []any:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, v := range v {
			id, err := parseUUID(v)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID any value in array: %w", err)
			}
			uuids = append(uuids, id)
		}
		return types.QValueArrayUUID{Val: uuids}, nil
	default:
		return nil, fmt.Errorf("unsupported type for UUID array: %T", value)
	}
}

func convertToArray[T any](kind types.QValueKind, value any) ([]T, error) {
	switch v := value.(type) {
	case pgtype.Array[T]:
		if v.Valid {
			return v.Elements, nil
		}
	case []T:
		return v, nil
	case []any:
		return shared.ArrayCastElements[T](v), nil
	}
	return nil, fmt.Errorf("failed to parse array %s from %T: %v", kind, value, value)
}

func (c *PostgresConnector) parseFieldFromPostgresOID(
	oid uint32, value any, customTypeMapping map[uint32]shared.CustomDataType,
) (types.QValue, error) {
	qvalueKind := c.postgresOIDToQValueKind(oid, customTypeMapping)
	if value == nil {
		return types.QValueNull(qvalueKind), nil
	}

	switch qvalueKind {
	case types.QValueKindTimestamp:
		switch val := value.(type) {
		case time.Time:
			return types.QValueTimestamp{Val: val}, nil
		case pgtype.InfinityModifier:
			return types.QValueNull(qvalueKind), nil
		}
	case types.QValueKindTimestampTZ:
		switch val := value.(type) {
		case time.Time:
			return types.QValueTimestampTZ{Val: val}, nil
		case pgtype.InfinityModifier:
			return types.QValueNull(qvalueKind), nil
		}
	case types.QValueKindInterval:
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

		return types.QValueString{Val: string(intervalJSON)}, nil
	case types.QValueKindTSTZRange:
		tstzrangeObject := value.(pgtype.Range[any])
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
		return types.QValueTSTZRange{Val: tstzrangeStr}, nil
	case types.QValueKindDate:
		switch val := value.(type) {
		case time.Time:
			return types.QValueDate{Val: val}, nil
		case pgtype.InfinityModifier:
			return types.QValueNull(qvalueKind), nil
		}
	case types.QValueKindTime:
		timeVal := value.(pgtype.Time)
		if timeVal.Valid {
			// 86399999999 to prevent 24:00:00
			return types.QValueTime{Val: time.UnixMicro(min(timeVal.Microseconds, 86399999999))}, nil
		}
	case types.QValueKindTimeTZ:
		timeVal := value.(string)
		// edge case, Postgres supports this extreme value for time
		timeVal = strings.Replace(timeVal, "24:00:00.000000", "23:59:59.999999", 1)
		tzidx := strings.LastIndexAny(timeVal, "+-")
		if tzidx > 0 {
			// postgres may print +xx00 as +xx
			if tzidx < len(timeVal)-5 && timeVal[tzidx+3] == ':' {
				timeVal = timeVal[:tzidx+3] + timeVal[tzidx+4:]
			} else if tzidx == len(timeVal)-3 {
				timeVal += "00"
			}
			if timeVal[tzidx-1] == ' ' {
				timeVal = timeVal[:tzidx-1] + timeVal[tzidx:]
			}
		}

		t, err := time.Parse("15:04:05.999999-0700", timeVal)
		if err != nil {
			return nil, fmt.Errorf("failed to parse time: %w", err)
		}
		return types.QValueTimeTZ{Val: t.AddDate(1970, 0, 0)}, nil
	case types.QValueKindBoolean:
		boolVal := value.(bool)
		return types.QValueBoolean{Val: boolVal}, nil
	case types.QValueKindJSON, types.QValueKindJSONB:
		tmp, err := parseJSON(value, false)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		return tmp, nil
	case types.QValueKindArrayJSON, types.QValueKindArrayJSONB:
		tmp, err := parseJSON(value, true)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON Array: %w", err)
		}
		return tmp, nil
	case types.QValueKindInt16:
		intVal := value.(int16)
		return types.QValueInt16{Val: intVal}, nil
	case types.QValueKindInt32:
		intVal := value.(int32)
		return types.QValueInt32{Val: intVal}, nil
	case types.QValueKindInt64:
		intVal := value.(int64)
		return types.QValueInt64{Val: intVal}, nil
	case types.QValueKindFloat32:
		floatVal := value.(float32)
		return types.QValueFloat32{Val: floatVal}, nil
	case types.QValueKindFloat64:
		floatVal := value.(float64)
		return types.QValueFloat64{Val: floatVal}, nil
	case types.QValueKindQChar:
		return types.QValueQChar{Val: uint8(value.(rune))}, nil
	case types.QValueKindString:
		// handling all unsupported types with strings as well for now.
		return types.QValueString{Val: fmt.Sprint(value)}, nil
	case types.QValueKindEnum:
		return types.QValueEnum{Val: fmt.Sprint(value)}, nil
	case types.QValueKindUUID:
		tmp, err := parseUUID(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse UUID: %w", err)
		}
		return types.QValueUUID{Val: tmp}, nil
	case types.QValueKindArrayUUID:
		tmp, err := parseUUIDArray(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse UUID array: %w", err)
		}
		return tmp, nil
	case types.QValueKindINET:
		switch v := value.(type) {
		case string:
			return types.QValueINET{Val: v}, nil
		case netip.Prefix:
			return types.QValueINET{Val: v.String()}, nil
		default:
			return nil, fmt.Errorf("failed to parse INET: %v", v)
		}
	case types.QValueKindCIDR:
		switch v := value.(type) {
		case string:
			return types.QValueCIDR{Val: v}, nil
		case netip.Prefix:
			return types.QValueCIDR{Val: v.String()}, nil
		default:
			return nil, fmt.Errorf("failed to parse CIDR: %v", value)
		}
	case types.QValueKindMacaddr:
		switch v := value.(type) {
		case string:
			return types.QValueMacaddr{Val: v}, nil
		case net.HardwareAddr:
			return types.QValueMacaddr{Val: v.String()}, nil
		default:
			return nil, fmt.Errorf("failed to parse MACADDR: %v %T", value, v)
		}
	case types.QValueKindBytes:
		rawBytes := value.([]byte)
		return types.QValueBytes{Val: rawBytes}, nil
	case types.QValueKindNumeric:
		numVal := value.(pgtype.Numeric)
		if numVal.Valid {
			num, err := numericToDecimal(numVal)
			if err != nil {
				return nil, fmt.Errorf("failed to convert numeric [%v] to decimal: %w", value, err)
			}
			return num, nil
		}
	case types.QValueKindArrayFloat32:
		a, err := convertToArray[float32](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayFloat32{Val: a}, nil
	case types.QValueKindArrayFloat64:
		a, err := convertToArray[float64](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayFloat64{Val: a}, nil
	case types.QValueKindArrayInt16:
		a, err := convertToArray[int16](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt16{Val: a}, nil
	case types.QValueKindArrayInt32:
		a, err := convertToArray[int32](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt32{Val: a}, nil
	case types.QValueKindArrayInt64:
		a, err := convertToArray[int64](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt64{Val: a}, nil
	case types.QValueKindArrayDate, types.QValueKindArrayTimestamp, types.QValueKindArrayTimestampTZ:
		a, err := convertToArray[time.Time](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		switch qvalueKind {
		case types.QValueKindArrayDate:
			return types.QValueArrayDate{Val: a}, nil
		case types.QValueKindArrayTimestamp:
			return types.QValueArrayTimestamp{Val: a}, nil
		case types.QValueKindArrayTimestampTZ:
			return types.QValueArrayTimestampTZ{Val: a}, nil
		}
	case types.QValueKindArrayBoolean:
		a, err := convertToArray[bool](qvalueKind, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayBoolean{Val: a}, nil
	case types.QValueKindArrayString:
		if str, ok := value.(string); ok {
			delim := byte(',')
			if typeData, ok := customTypeMapping[oid]; ok {
				delim = typeData.Delim
			}
			return types.QValueArrayString{Val: shared.ParsePgArrayStringToStringSlice(str, delim)}, nil
		} else {
			a, err := convertToArray[string](qvalueKind, value)
			if err != nil {
				return nil, err
			}
			return types.QValueArrayString{Val: a}, nil
		}
	case types.QValueKindArrayEnum:
		if str, ok := value.(string); ok {
			delim := byte(',')
			if typeData, ok := customTypeMapping[oid]; ok {
				delim = typeData.Delim
			}
			return types.QValueArrayEnum{Val: shared.ParsePgArrayStringToStringSlice(str, delim)}, nil
		} else {
			a, err := convertToArray[string](qvalueKind, value)
			if err != nil {
				return nil, err
			}
			return types.QValueArrayEnum{Val: a}, nil
		}
	case types.QValueKindArrayNumeric:
		if items, ok := value.([]any); ok {
			a := make([]string, len(items))
			success := true
		loop:
			for i, item := range items {
				var numeric pgtype.Numeric
				var ok bool
				if numeric, ok = item.(pgtype.Numeric); !ok {
					success = false
					break loop
				}
				bytes, _ := numeric.MarshalJSON()
				a[i] = shared.UnsafeFastReadOnlyBytesToString(bytes)
			}
			if success {
				return types.QValueArrayString{Val: a}, nil
			}
		}
		return nil, fmt.Errorf("failed to parse numeric array from %T: %v", value, value)
	case types.QValueKindPoint:
		coord := value.(pgtype.Point).P
		return types.QValuePoint{
			Val: fmt.Sprintf("POINT(%f %f)", coord.X, coord.Y),
		}, nil
	case types.QValueKindHStore:
		return types.QValueHStore{Val: fmt.Sprint(value)}, nil
	case types.QValueKindGeography, types.QValueKindGeometry:
		wkbString, ok := value.(string)
		wkt, err := datatypes.GeoValidate(wkbString)
		if err != nil || !ok {
			return types.QValueNull(types.QValueKindGeography), nil
		} else if qvalueKind == types.QValueKindGeography {
			return types.QValueGeography{Val: wkt}, nil
		} else {
			return types.QValueGeometry{Val: wkt}, nil
		}
	default:
		if textVal, ok := value.(string); ok {
			return types.QValueString{Val: textVal}, nil
		}
	}

	// parsing into pgtype failed.
	return nil, fmt.Errorf("failed to parse value %v into QValueKind %v", value, qvalueKind)
}

func numericToDecimal(numVal pgtype.Numeric) (types.QValue, error) {
	switch {
	case !numVal.Valid:
		return types.QValueNull(types.QValueKindNumeric), errors.New("invalid numeric")
	case numVal.NaN, numVal.InfinityModifier == pgtype.Infinity,
		numVal.InfinityModifier == pgtype.NegativeInfinity:
		return types.QValueNull(types.QValueKindNumeric), nil
	default:
		return types.QValueNumeric{Val: decimal.NewFromBigInt(numVal.Int, numVal.Exp)}, nil
	}
}

func customTypeToQKind(typeData shared.CustomDataType) types.QValueKind {
	if typeData.Type == 'e' {
		if typeData.Delim != 0 {
			return types.QValueKindArrayEnum
		} else {
			return types.QValueKindEnum
		}
	}

	if typeData.Delim != 0 {
		return types.QValueKindArrayString
	}

	switch typeData.Name {
	case "geometry":
		return types.QValueKindGeometry
	case "geography":
		return types.QValueKindGeography
	case "hstore":
		return types.QValueKindHStore
	default:
		return types.QValueKindString
	}
}

// Postgres does not like timestamps of the form 2006-01-02 15:04:05 +0000 UTC
// in tstzrange.
// convertTimeRangeBound removes the +0000 UTC part
func convertTimeRangeBound(timeBound any) (string, error) {
	if timeBound, isInfinite := timeBound.(pgtype.InfinityModifier); isInfinite {
		return timeBound.String(), nil
	}

	layout := "2006-01-02 15:04:05 -0700 MST"
	postgresFormat := "2006-01-02 15:04:05"
	if timeBound != nil {
		lowerParsed, err := time.Parse(layout, fmt.Sprint(timeBound))
		if err != nil {
			return "", fmt.Errorf("unexpected bound value in tstzrange. Error: %v", err)
		}
		return lowerParsed.Format(postgresFormat), nil
	}
	return "", nil
}
