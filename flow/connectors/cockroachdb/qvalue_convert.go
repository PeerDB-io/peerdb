package conncockroachdb

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// crdbTypeToQValueKind maps CockroachDB information_schema column types to QValueKinds.
// data_type is information_schema.columns.data_type; udt_name disambiguates
// arrays (data_type 'ARRAY') and user-defined types (data_type 'USER-DEFINED').
func crdbTypeToQValueKind(dataType string, udtName string) types.QValueKind {
	switch strings.ToLower(dataType) {
	case "bigint", "int8", "oid":
		return types.QValueKindInt64
	case "integer", "int4":
		return types.QValueKindInt32
	case "smallint", "int2":
		return types.QValueKindInt16
	case "numeric", "decimal":
		return types.QValueKindNumeric
	case "double precision", "float8":
		return types.QValueKindFloat64
	case "real", "float4":
		return types.QValueKindFloat32
	case "boolean", "bool":
		return types.QValueKindBoolean
	case "character varying", "character", "text", "name", `"char"`, "bit", "bit varying", "varbit":
		return types.QValueKindString
	case "bytes", "bytea", "blob":
		return types.QValueKindBytes
	case "uuid":
		return types.QValueKindUUID
	case "jsonb", "json":
		return types.QValueKindJSON
	case "timestamp without time zone", "timestamp":
		return types.QValueKindTimestamp
	case "timestamp with time zone", "timestamptz":
		return types.QValueKindTimestampTZ
	case "date":
		return types.QValueKindDate
	case "time without time zone", "time":
		return types.QValueKindTime
	case "time with time zone", "timetz":
		return types.QValueKindTimeTZ
	case "interval":
		return types.QValueKindInterval
	case "inet":
		return types.QValueKindINET
	case "geography":
		return types.QValueKindGeography
	case "geometry":
		return types.QValueKindGeometry
	case "array":
		return crdbArrayUdtToQValueKind(udtName)
	case "user-defined":
		// CockroachDB user-defined types are enums
		return types.QValueKindEnum
	default:
		// vector, tsvector, tsquery, refcursor and anything new: replicate as text
		return types.QValueKindString
	}
}

// crdbOIDToQValueKind maps wire type OIDs to QValueKinds for result columns
// that cannot be matched to a table column (e.g. custom query expressions).
func crdbOIDToQValueKind(oid uint32) types.QValueKind {
	switch oid {
	case pgtype.BoolOID:
		return types.QValueKindBoolean
	case pgtype.Int2OID:
		return types.QValueKindInt16
	case pgtype.Int4OID:
		return types.QValueKindInt32
	case pgtype.Int8OID, pgtype.OIDOID:
		return types.QValueKindInt64
	case pgtype.Float4OID:
		return types.QValueKindFloat32
	case pgtype.Float8OID:
		return types.QValueKindFloat64
	case pgtype.NumericOID:
		return types.QValueKindNumeric
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID, pgtype.QCharOID:
		return types.QValueKindString
	case pgtype.ByteaOID:
		return types.QValueKindBytes
	case pgtype.UUIDOID:
		return types.QValueKindUUID
	case pgtype.JSONOID, pgtype.JSONBOID:
		return types.QValueKindJSON
	case pgtype.TimestampOID:
		return types.QValueKindTimestamp
	case pgtype.TimestamptzOID:
		return types.QValueKindTimestampTZ
	case pgtype.DateOID:
		return types.QValueKindDate
	case pgtype.TimeOID:
		return types.QValueKindTime
	case pgtype.TimetzOID:
		return types.QValueKindTimeTZ
	case pgtype.IntervalOID:
		return types.QValueKindInterval
	case pgtype.InetOID:
		return types.QValueKindINET
	case pgtype.Int2ArrayOID:
		return types.QValueKindArrayInt16
	case pgtype.Int4ArrayOID:
		return types.QValueKindArrayInt32
	case pgtype.Int8ArrayOID:
		return types.QValueKindArrayInt64
	case pgtype.Float4ArrayOID:
		return types.QValueKindArrayFloat32
	case pgtype.Float8ArrayOID:
		return types.QValueKindArrayFloat64
	case pgtype.NumericArrayOID:
		return types.QValueKindArrayNumeric
	case pgtype.BoolArrayOID:
		return types.QValueKindArrayBoolean
	case pgtype.UUIDArrayOID:
		return types.QValueKindArrayUUID
	case pgtype.JSONArrayOID, pgtype.JSONBArrayOID:
		return types.QValueKindArrayJSON
	case pgtype.DateArrayOID:
		return types.QValueKindArrayDate
	case pgtype.TimestampArrayOID:
		return types.QValueKindArrayTimestamp
	case pgtype.TimestamptzArrayOID:
		return types.QValueKindArrayTimestampTZ
	case pgtype.IntervalArrayOID:
		return types.QValueKindArrayInterval
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID:
		return types.QValueKindArrayString
	default:
		return types.QValueKindString
	}
}

// qvalueFromCrdbValue converts a value decoded by pgx into the QValue matching field.Type.
func qvalueFromCrdbValue(
	field types.QField,
	dstType protos.DBType,
	oid uint32,
	typeMap *pgtype.Map,
	value any,
) (types.QValue, error) {
	if value == nil {
		return types.QValueNull(field.Type), nil
	}

	switch field.Type {
	case types.QValueKindBoolean:
		if v, ok := value.(bool); ok {
			return types.QValueBoolean{Val: v}, nil
		}
	case types.QValueKindInt16:
		if v, ok := value.(int16); ok {
			return types.QValueInt16{Val: v}, nil
		}
	case types.QValueKindInt32:
		if v, ok := value.(int32); ok {
			return types.QValueInt32{Val: v}, nil
		}
	case types.QValueKindInt64:
		switch v := value.(type) {
		case int64:
			return types.QValueInt64{Val: v}, nil
		case uint32:
			// pgx decodes the oid type as uint32
			return types.QValueInt64{Val: int64(v)}, nil
		}
	case types.QValueKindFloat32:
		if v, ok := value.(float32); ok {
			return types.QValueFloat32{Val: v}, nil
		}
	case types.QValueKindFloat64:
		if v, ok := value.(float64); ok {
			return types.QValueFloat64{Val: v}, nil
		}
	case types.QValueKindNumeric:
		if v, ok := value.(pgtype.Numeric); ok {
			num, valid := numericToDecimal(v)
			if !valid {
				if field.Nullable {
					return types.QValueNull(types.QValueKindNumeric), nil
				}
				return types.QValueNumeric{Precision: field.Precision, Scale: field.Scale}, nil
			}
			return types.QValueNumeric{Val: num, Precision: field.Precision, Scale: field.Scale}, nil
		}
	case types.QValueKindString:
		return types.QValueString{Val: convertToString(typeMap, oid, value)}, nil
	case types.QValueKindEnum:
		return types.QValueEnum{Val: convertToString(typeMap, oid, value)}, nil
	case types.QValueKindBytes:
		if v, ok := value.([]byte); ok {
			return types.QValueBytes{Val: v}, nil
		}
	case types.QValueKindUUID:
		v, err := parseUUID(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse UUID: %w", err)
		}
		return types.QValueUUID{Val: v}, nil
	case types.QValueKindJSON:
		if v, ok := value.(string); ok {
			return types.QValueJSON{Val: v}, nil
		}
	case types.QValueKindTimestamp:
		switch v := value.(type) {
		case time.Time:
			return types.QValueTimestamp{Val: v}, nil
		case pgtype.InfinityModifier:
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			}
			return types.QValueTimestamp{Val: qvalue.DefaultTime(dstType)}, nil
		}
	case types.QValueKindTimestampTZ:
		switch v := value.(type) {
		case time.Time:
			return types.QValueTimestampTZ{Val: v}, nil
		case pgtype.InfinityModifier:
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			}
			return types.QValueTimestampTZ{Val: qvalue.DefaultTime(dstType)}, nil
		}
	case types.QValueKindDate:
		switch v := value.(type) {
		case time.Time:
			return types.QValueDate{Val: v}, nil
		case pgtype.InfinityModifier:
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			}
			return types.QValueDate{Val: qvalue.DefaultTime(dstType)}, nil
		}
	case types.QValueKindTime:
		if v, ok := value.(pgtype.Time); ok {
			if !v.Valid {
				if field.Nullable {
					return types.QValueNull(types.QValueKindTime), nil
				}
				return types.QValueTime{}, nil
			}
			return types.QValueTime{Val: time.Duration(v.Microseconds) * time.Microsecond}, nil
		}
	case types.QValueKindTimeTZ:
		if v, ok := value.(string); ok {
			d, err := parseTimeTZ(v)
			if err != nil {
				return nil, err
			}
			return types.QValueTimeTZ{Val: d}, nil
		}
	case types.QValueKindInterval:
		if v, ok := value.(pgtype.Interval); ok {
			str, err := intervalToString(v)
			if err != nil {
				return nil, err
			}
			return types.QValueInterval{Val: str}, nil
		}
	case types.QValueKindINET:
		switch v := value.(type) {
		case string:
			return types.QValueINET{Val: v}, nil
		case netip.Prefix:
			return types.QValueINET{Val: v.String()}, nil
		}
	case types.QValueKindGeography, types.QValueKindGeometry:
		wkbString, ok := value.(string)
		wkt, err := datatypes.GeoValidate(wkbString)
		if err != nil || !ok {
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			} else if field.Type == types.QValueKindGeography {
				return types.QValueGeography{}, nil
			}
			return types.QValueGeometry{}, nil
		} else if field.Type == types.QValueKindGeography {
			return types.QValueGeography{Val: wkt}, nil
		}
		return types.QValueGeometry{Val: wkt}, nil
	case types.QValueKindArrayInt16:
		a, err := convertToArray[int16](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt16{Val: a}, nil
	case types.QValueKindArrayInt32:
		a, err := convertToArray[int32](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt32{Val: a}, nil
	case types.QValueKindArrayInt64:
		a, err := convertToArray[int64](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt64{Val: a}, nil
	case types.QValueKindArrayFloat32:
		a, err := convertToArray[float32](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayFloat32{Val: a}, nil
	case types.QValueKindArrayFloat64:
		a, err := convertToArray[float64](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayFloat64{Val: a}, nil
	case types.QValueKindArrayBoolean:
		a, err := convertToArray[bool](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayBoolean{Val: a}, nil
	case types.QValueKindArrayDate, types.QValueKindArrayTimestamp, types.QValueKindArrayTimestampTZ:
		a, err := convertToArray[time.Time](field.Type, value)
		if err != nil {
			return nil, err
		}
		switch field.Type {
		case types.QValueKindArrayDate:
			return types.QValueArrayDate{Val: a}, nil
		case types.QValueKindArrayTimestamp:
			return types.QValueArrayTimestamp{Val: a}, nil
		default:
			return types.QValueArrayTimestampTZ{Val: a}, nil
		}
	case types.QValueKindArrayUUID:
		a, err := parseUUIDArray(value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayUUID{Val: a}, nil
	case types.QValueKindArrayNumeric:
		if v, ok := value.([]any); ok {
			numArr := make([]decimal.Decimal, 0, len(v))
			for _, anyVal := range v {
				if anyVal == nil {
					numArr = append(numArr, decimal.Decimal{})
					continue
				}
				numVal, ok := anyVal.(pgtype.Numeric)
				if !ok {
					return nil, fmt.Errorf("failed to cast ArrayNumeric element: got %T", anyVal)
				}
				num, valid := numericToDecimal(numVal)
				if !valid {
					num = decimal.Decimal{}
				}
				numArr = append(numArr, num)
			}
			return types.QValueArrayNumeric{Val: numArr, Precision: field.Precision, Scale: field.Scale}, nil
		}
	case types.QValueKindArrayInterval:
		if v, ok := value.([]any); ok {
			strs := make([]string, 0, len(v))
			for _, anyVal := range v {
				if anyVal == nil {
					strs = append(strs, "")
					continue
				}
				interval, ok := anyVal.(pgtype.Interval)
				if !ok {
					return nil, fmt.Errorf("failed to cast ArrayInterval element: got %T", anyVal)
				}
				str, err := intervalToString(interval)
				if err != nil {
					return nil, err
				}
				strs = append(strs, str)
			}
			return types.QValueArrayInterval{Val: strs}, nil
		}
	case types.QValueKindArrayString:
		switch v := value.(type) {
		case string:
			return types.QValueArrayString{Val: shared.ParsePgArrayStringToStringSlice(v, ',')}, nil
		case []string:
			return types.QValueArrayString{Val: v}, nil
		case []any:
			strs := make([]string, 0, len(v))
			for _, anyVal := range v {
				strs = append(strs, convertToString(typeMap, 0, anyVal))
			}
			return types.QValueArrayString{Val: strs}, nil
		}
	default:
		if v, ok := value.(string); ok {
			return types.QValueString{Val: v}, nil
		}
	}

	return nil, fmt.Errorf("failed to parse value %v (%T) into QValueKind %v", value, value, field.Type)
}

func convertToString(typeMap *pgtype.Map, oid uint32, value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	if oid != 0 {
		if buf, err := typeMap.Encode(oid, pgtype.TextFormatCode, value, nil); err == nil {
			return string(buf)
		}
	}
	return fmt.Sprint(value)
}

func convertToArray[T any](kind types.QValueKind, value any) ([]T, error) {
	switch v := value.(type) {
	case []T:
		return v, nil
	case []any:
		return shared.ArrayCastElements[T](v), nil
	}
	return nil, fmt.Errorf("failed to parse array %s from %T: %v", kind, value, value)
}

func numericToDecimal(numVal pgtype.Numeric) (decimal.Decimal, bool) {
	if !numVal.Valid || numVal.NaN ||
		numVal.InfinityModifier == pgtype.Infinity || numVal.InfinityModifier == pgtype.NegativeInfinity {
		return decimal.Decimal{}, false
	}
	return decimal.NewFromBigInt(numVal.Int, numVal.Exp), true
}

func parseUUID(value any) (uuid.UUID, error) {
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

func parseUUIDArray(value any) ([]uuid.UUID, error) {
	switch v := value.(type) {
	case []string:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, s := range v {
			u, err := uuid.Parse(s)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID in array: %w", err)
			}
			uuids = append(uuids, u)
		}
		return uuids, nil
	case [][16]byte:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, v16 := range v {
			uuids = append(uuids, uuid.UUID(v16))
		}
		return uuids, nil
	case []uuid.UUID:
		return v, nil
	case []any:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, anyVal := range v {
			if anyVal == nil {
				uuids = append(uuids, uuid.Nil)
				continue
			}
			u, err := parseUUID(anyVal)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID in array: %w", err)
			}
			uuids = append(uuids, u)
		}
		return uuids, nil
	default:
		return nil, fmt.Errorf("unsupported type for UUID array: %T", value)
	}
}

var timeTZLayouts = [...]string{
	"15:04:05.999999-07:00:00",
	"15:04:05.999999-07:00",
	"15:04:05.999999-0700",
	"15:04:05.999999-07",
}

func parseTimeTZ(value string) (time.Duration, error) {
	// CockroachDB permits this extreme value for time
	value = strings.Replace(value, "24:00:00", "23:59:59.999999", 1)
	for _, layout := range timeTZLayouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UTC().Sub(shared.Year0000), nil
		}
	}
	return 0, fmt.Errorf("failed to parse timetz: %s", value)
}

func intervalToString(intervalObject pgtype.Interval) (string, error) {
	if !intervalObject.Valid {
		return "", fmt.Errorf("invalid interval: %v", intervalObject)
	}

	interval := datatypes.PeerDBInterval{
		Hours:   int(intervalObject.Microseconds / 3600000000),
		Minutes: int((intervalObject.Microseconds % 3600000000) / 60000000),
		Seconds: float64(intervalObject.Microseconds%60000000) / 1000000.0,
		Days:    int(intervalObject.Days),
		Years:   int(intervalObject.Months / 12),
		Months:  int(intervalObject.Months % 12),
		Valid:   intervalObject.Valid,
	}

	intervalJSON, err := json.Marshal(interval)
	if err != nil {
		return "", fmt.Errorf("failed to marshal interval: %w", err)
	}
	return string(intervalJSON), nil
}

func crdbArrayUdtToQValueKind(udtName string) types.QValueKind {
	switch strings.ToLower(strings.TrimPrefix(udtName, "_")) {
	case "int8", "oid":
		return types.QValueKindArrayInt64
	case "int4":
		return types.QValueKindArrayInt32
	case "int2":
		return types.QValueKindArrayInt16
	case "numeric", "decimal":
		return types.QValueKindArrayNumeric
	case "float8":
		return types.QValueKindArrayFloat64
	case "float4":
		return types.QValueKindArrayFloat32
	case "bool":
		return types.QValueKindArrayBoolean
	case "uuid":
		return types.QValueKindArrayUUID
	case "jsonb", "json":
		return types.QValueKindArrayJSON
	case "date":
		return types.QValueKindArrayDate
	case "timestamp":
		return types.QValueKindArrayTimestamp
	case "timestamptz":
		return types.QValueKindArrayTimestampTZ
	case "interval":
		return types.QValueKindArrayInterval
	default:
		return types.QValueKindArrayString
	}
}
