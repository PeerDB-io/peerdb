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
	"github.com/pgvector/pgvector-go"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *PostgresConnector) postgresOIDToName(recvOID uint32, customTypeMapping map[uint32]shared.CustomDataType) (string, error) {
	if ty, ok := c.typeMap.TypeForOID(recvOID); ok {
		return ty.Name, nil
	}
	// workaround for some types not being defined by pgtype
	switch recvOID {
	case pgtype.TimetzOID:
		return "timetz", nil
	case pgtype.XMLOID:
		return "xml", nil
	case shared.MoneyOID:
		return "money", nil
	case shared.TxidSnapshotOID:
		return "txid_snapshot", nil
	case shared.TsvectorOID:
		return "tsvector", nil
	case shared.TsqueryOID:
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
	version uint32,
) types.QValueKind {
	colType, err := PostgresOIDToQValueKind(recvOID, customTypeMapping, c.typeMap, version)
	if err != nil {
		if _, warned := c.hushWarnOID[recvOID]; !warned {
			c.logger.Warn(
				"unsupported field type",
				slog.Int64("oid", int64(recvOID)),
				slog.String("typeName", err.Error()),
				slog.String("mapping", string(colType)))
			c.hushWarnOID[recvOID] = struct{}{}
		}
	}
	return colType
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
	case types.QValueKindArrayInterval:
		return "TEXT[]"
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

func intervalToString(intervalObject pgtype.Interval) (string, error) {
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
		return "", fmt.Errorf("failed to parse interval: %w", err)
	}

	if !interval.Valid {
		return "", fmt.Errorf("invalid interval: %v", intervalObject)
	}

	return string(intervalJSON), nil
}

var ErrMismatchingRangeType = errors.New("mismatching range type")

func rangeToTyped[T any](r pgtype.Range[any]) (pgtype.Range[*T], error) {
	var lower, upper *T
	if r.Lower != nil {
		lowerVal, ok := r.Lower.(T)
		if !ok {
			return pgtype.Range[*T]{}, ErrMismatchingRangeType
		}
		lower = &lowerVal
	}
	if r.Upper != nil {
		upperVal, ok := r.Upper.(T)
		if !ok {
			return pgtype.Range[*T]{}, ErrMismatchingRangeType
		}
		upper = &upperVal
	}
	return pgtype.Range[*T]{
		Lower:     lower,
		Upper:     upper,
		LowerType: r.LowerType,
		UpperType: r.UpperType,
		Valid:     r.Valid,
	}, nil
}

func multirangeToTyped[T any](multirange pgtype.Multirange[pgtype.Range[any]]) (pgtype.Multirange[pgtype.Range[*T]], error) {
	ranges := make([]pgtype.Range[*T], 0, multirange.Len())
	for _, anyR := range multirange {
		r, err := rangeToTyped[T](anyR)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	return pgtype.Multirange[pgtype.Range[*T]](ranges), nil
}

func (c *PostgresConnector) convertToString(oid uint32, value any) string {
	if value == nil {
		return ""
	}
	if buf, err := c.typeMap.Encode(oid, pgtype.TextFormatCode, value, nil); err == nil {
		return shared.UnsafeFastReadOnlyBytesToString(buf)
	}
	// pgx returns us type-erased ranges that it doesn't know how to encode
	// but if we bring the types back it becomes able to
	if r, ok := value.(pgtype.Range[any]); ok {
		var typedR any
		var err error
		switch oid {
		case pgtype.Int4rangeOID:
			typedR, err = rangeToTyped[int32](r)
		case pgtype.Int8rangeOID:
			typedR, err = rangeToTyped[int64](r)
		case pgtype.NumrangeOID:
			typedR, err = rangeToTyped[pgtype.Numeric](r)
		case pgtype.DaterangeOID, pgtype.TsrangeOID, pgtype.TstzrangeOID:
			// It might seem like tstzrange needs special handling
			// but it's actually all UTC under the hood
			typedR, err = rangeToTyped[time.Time](r)
		default:
			err = errors.ErrUnsupported
		}
		if err == nil {
			var buf []byte
			buf, err = c.typeMap.Encode(oid, pgtype.TextFormatCode, typedR, nil)
			if err == nil {
				return shared.UnsafeFastReadOnlyBytesToString(buf)
			}
		}
		c.logger.Warn(fmt.Sprintf(
			"couldn't encode range %v (%T, oid %d): %v", value, value, oid, err,
		))
	}
	if multirange, ok := value.(pgtype.Multirange[pgtype.Range[any]]); ok {
		var typedM any
		var err error
		switch oid {
		case pgtype.Int4multirangeOID:
			typedM, err = multirangeToTyped[int32](multirange)
		case pgtype.Int8multirangeOID:
			typedM, err = multirangeToTyped[int64](multirange)
		case pgtype.NummultirangeOID:
			typedM, err = multirangeToTyped[pgtype.Numeric](multirange)
		case pgtype.DatemultirangeOID, pgtype.TsmultirangeOID, pgtype.TstzmultirangeOID:
			typedM, err = multirangeToTyped[time.Time](multirange)
		default:
			err = errors.ErrUnsupported
		}
		if err == nil {
			var buf []byte
			buf, err = c.typeMap.Encode(oid, pgtype.TextFormatCode, typedM, nil)
			if err == nil {
				return shared.UnsafeFastReadOnlyBytesToString(buf)
			}
		}
		c.logger.Warn(fmt.Sprintf(
			"couldn't encode multirange %v (%T, oid %d): %v", value, value, oid, err,
		))
	}
	return fmt.Sprint(value)
}

var arrayOidToRangeOid = map[uint32]uint32{
	pgtype.Int4rangeArrayOID:      pgtype.Int4rangeOID,
	pgtype.Int8rangeArrayOID:      pgtype.Int8rangeOID,
	pgtype.NumrangeArrayOID:       pgtype.NumrangeOID,
	pgtype.DaterangeArrayOID:      pgtype.DaterangeOID,
	pgtype.TsrangeArrayOID:        pgtype.TsrangeOID,
	pgtype.TstzrangeArrayOID:      pgtype.TstzrangeOID,
	pgtype.Int4multirangeArrayOID: pgtype.Int4multirangeOID,
	pgtype.Int8multirangeArrayOID: pgtype.Int8multirangeOID,
	pgtype.NummultirangeArrayOID:  pgtype.NummultirangeOID,
	pgtype.DatemultirangeArrayOID: pgtype.DatemultirangeOID,
	pgtype.TsmultirangeArrayOID:   pgtype.TsmultirangeOID,
	pgtype.TstzmultirangeArrayOID: pgtype.TstzmultirangeOID,
}

func (c *PostgresConnector) convertToStringArray(kind types.QValueKind, oid uint32, value any) ([]string, error) {
	switch v := value.(type) {
	case pgtype.Array[string]:
		if v.Valid {
			return v.Elements, nil
		}
	case []string:
		return v, nil
	case []any:
		itemOid := oid
		if rangeOid, ok := arrayOidToRangeOid[oid]; ok {
			itemOid = rangeOid
		}
		res := make([]string, 0, len(v))
		for _, item := range v {
			str := c.convertToString(itemOid, item)
			res = append(res, str)
		}
		return res, nil
	}
	return nil, fmt.Errorf("failed to parse array %s from %T: %v", kind, value, value)
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
	oid uint32, typmod int32, value any, customTypeMapping map[uint32]shared.CustomDataType, version uint32,
) (types.QValue, error) {
	qvalueKind := c.postgresOIDToQValueKind(oid, customTypeMapping, version)
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
		if interval, ok := value.(pgtype.Interval); ok {
			str, err := intervalToString(interval)
			if err != nil {
				return nil, err
			}
			return types.QValueInterval{Val: str}, nil
		}
	case types.QValueKindArrayInterval:
		if arr, ok := value.([]any); ok {
			success := true
			strs := make([]string, 0, len(arr))
			for _, item := range arr {
				if item == nil {
					strs = append(strs, "")
				} else if interval, ok := item.(pgtype.Interval); ok {
					str, err := intervalToString(interval)
					if err != nil {
						return nil, fmt.Errorf("failed to parse interval array: %w", err)
					}
					strs = append(strs, str)
				} else {
					success = false
					break
				}
			}
			if success {
				return types.QValueArrayInterval{Val: strs}, nil
			}
		}
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
			return types.QValueTime{Val: time.Duration(timeVal.Microseconds) * time.Microsecond}, nil
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
		return types.QValueTimeTZ{Val: t.UTC().Sub(shared.Year0000)}, nil
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
		str := c.convertToString(oid, value)
		return types.QValueString{Val: str}, nil
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
			num, ok := validNumericToDecimal(numVal)
			if !ok {
				return types.QValueNull(types.QValueKindNumeric), nil
			}
			precision, scale := datatypes.ParseNumericTypmod(typmod)
			return types.QValueNumeric{
				Val:       num,
				Precision: precision,
				Scale:     scale,
			}, nil
		}
	case types.QValueKindArrayFloat32:
		switch value := value.(type) {
		case string:
			typeData := customTypeMapping[oid]
			switch typeData.Name {
			case "vector":
				var vector pgvector.Vector
				if err := vector.Parse(value); err != nil {
					return nil, fmt.Errorf("[pg] failed to parse vector: %w", err)
				}
				return types.QValueArrayFloat32{Val: vector.Slice()}, nil
			case "halfvec":
				var halfvec pgvector.HalfVector
				if err := halfvec.Parse(value); err != nil {
					return nil, fmt.Errorf("[pg] failed to parse halfvec: %w", err)
				}
				return types.QValueArrayFloat32{Val: halfvec.Slice()}, nil
			case "sparsevec":
				var sparsevec pgvector.SparseVector
				if err := sparsevec.Parse(value); err != nil {
					return nil, fmt.Errorf("[pg] failed to parse sparsevec: %w", err)
				}
				return types.QValueArrayFloat32{Val: sparsevec.Slice()}, nil
			default:
				return nil, fmt.Errorf("unknown float array type %s", typeData.Name)
			}
		case interface{ Slice() []float32 }:
			return types.QValueArrayFloat32{Val: value.Slice()}, nil
		default:
			a, err := convertToArray[float32](qvalueKind, value)
			if err != nil {
				return nil, err
			}
			return types.QValueArrayFloat32{Val: a}, nil
		}
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
			arr := shared.ParsePgArrayStringToStringSlice(str, delim)
			return types.QValueArrayString{Val: arr}, nil
		} else {
			// Arrays of unsupported types become string arrays too
			arr, err := c.convertToStringArray(qvalueKind, oid, value)
			if err != nil {
				return nil, err
			}
			return types.QValueArrayString{Val: arr}, nil
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
		if v, ok := value.([]any); ok {
			numArr := make([]decimal.Decimal, 0, len(v))
			allValid := true
			for _, anyVal := range v {
				if anyVal == nil {
					numArr = append(numArr, decimal.Decimal{})
					continue
				}
				numVal, ok := anyVal.(pgtype.Numeric)
				if !ok {
					return nil, fmt.Errorf("failed to cast ArrayNumeric as []pgtype.Numeric: got %T", anyVal)
				}
				if !numVal.Valid {
					allValid = false
					break
				}
				num, ok := validNumericToDecimal(numVal)
				if !ok {
					numArr = append(numArr, decimal.Decimal{})
				} else {
					numArr = append(numArr, num)
				}
			}
			if allValid {
				precision, scale := datatypes.ParseNumericTypmod(typmod)
				return types.QValueArrayNumeric{
					Val:       numArr,
					Precision: precision,
					Scale:     scale,
				}, nil
			}
		}
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
			if err != nil {
				c.logger.Warn("failure during GeoValidate", slog.Any("error", err))
			}
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
	return nil, fmt.Errorf("failed to parse value %v (%T) into QValueKind %v", value, value, qvalueKind)
}

func validNumericToDecimal(numVal pgtype.Numeric) (decimal.Decimal, bool) {
	if numVal.NaN || numVal.InfinityModifier == pgtype.Infinity ||
		numVal.InfinityModifier == pgtype.NegativeInfinity {
		return decimal.Decimal{}, false
	} else {
		return decimal.NewFromBigInt(numVal.Int, numVal.Exp), true
	}
}
