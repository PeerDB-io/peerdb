package connpostgres

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
	log "github.com/sirupsen/logrus"

	//nolint:all
	geom "github.com/twpayne/go-geos"
)

func postgresOIDToQValueKind(recvOID uint32) qvalue.QValueKind {
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
	case pgtype.TimestampOID:
		return qvalue.QValueKindTimestamp
	case pgtype.TimestamptzOID:
		return qvalue.QValueKindTimestampTZ
	case pgtype.NumericOID:
		return qvalue.QValueKindNumeric
	case pgtype.BitOID, pgtype.VarbitOID:
		return qvalue.QValueKindBit
	case pgtype.Int2ArrayOID:
		return qvalue.QValueKindArrayInt32
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
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID:
		return qvalue.QValueKindArrayString
	case pgtype.TsrangeOID:
		return qvalue.QValueKindTSRange
	case pgtype.TstzrangeOID:
		return qvalue.QValueKindTSTzRange
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
			log.Warnf("unsupported field type: %v - type name - %s; returning as string", recvOID, typeName.Name)
			return qvalue.QValueKindString
		}
	}
}

func qValueKindToPostgresType(qvalueKind string) string {
	switch qvalue.QValueKind(qvalueKind) {
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
	case qvalue.QValueKindString:
		return "TEXT"
	case qvalue.QValueKindBytes:
		return "BYTEA"
	case qvalue.QValueKindJSON:
		return "JSONB"
	case qvalue.QValueKindUUID:
		return "UUID"
	case qvalue.QValueKindTime:
		return "TIME"
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
	case qvalue.QValueKindArrayInt32:
		return "INTEGER[]"
	case qvalue.QValueKindArrayInt64:
		return "BIGINT[]"
	case qvalue.QValueKindArrayFloat32:
		return "REAL[]"
	case qvalue.QValueKindArrayFloat64:
		return "DOUBLE PRECISION[]"
	case qvalue.QValueKindArrayString:
		return "TEXT[]"
	case qvalue.QValueKindTSRange:
		return "TSRANGE"
	case qvalue.QValueKindTSTzRange:
		return "TSTZRANGE"
	default:
		return "TEXT"
	}
}

func parseJSON(value interface{}) (*qvalue.QValue, error) {
	jsonVal, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return &qvalue.QValue{Kind: qvalue.QValueKindJSON, Value: string(jsonVal)}, nil
}

func parseFieldFromQValueKind(qvalueKind qvalue.QValueKind, value interface{}) (*qvalue.QValue, error) {
	var val *qvalue.QValue = nil

	if value == nil {
		val = &qvalue.QValue{Kind: qvalueKind, Value: nil}
		return val, nil
	}

	switch qvalueKind {
	case qvalue.QValueKindTimestamp:
		timestamp := value.(time.Time)
		val = &qvalue.QValue{Kind: qvalue.QValueKindTimestamp, Value: timestamp}
	case qvalue.QValueKindTimestampTZ:
		timestamp := value.(time.Time)
		val = &qvalue.QValue{Kind: qvalue.QValueKindTimestampTZ, Value: timestamp}
	case qvalue.QValueKindDate:
		date := value.(time.Time)
		val = &qvalue.QValue{Kind: qvalue.QValueKindDate, Value: date}
	case qvalue.QValueKindTime:
		timeVal := value.(pgtype.Time)
		if timeVal.Valid {
			var timeValStr any
			timeValStr, err := timeVal.Value()
			if err != nil {
				return nil, fmt.Errorf("failed to parse time: %w", err)
			}
			// edge case, only Postgres supports this extreme value for time
			timeValStr = strings.Replace(timeValStr.(string), "24:00:00.000000", "23:59:59.999999", 1)
			t, err := time.Parse("15:04:05.999999", timeValStr.(string))
			t = t.AddDate(1970, 0, 0)
			if err != nil {
				return nil, fmt.Errorf("failed to parse time: %w", err)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindTime, Value: t}
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
		val = &qvalue.QValue{Kind: qvalue.QValueKindTimeTZ, Value: t}

	case qvalue.QValueKindBoolean:
		boolVal := value.(bool)
		val = &qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: boolVal}
	case qvalue.QValueKindJSON:
		tmp, err := parseJSON(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		val = tmp
	case qvalue.QValueKindInt16:
		intVal := value.(int16)
		val = &qvalue.QValue{Kind: qvalue.QValueKindInt16, Value: int32(intVal)}
	case qvalue.QValueKindInt32:
		intVal := value.(int32)
		val = &qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: intVal}
	case qvalue.QValueKindInt64:
		intVal := value.(int64)
		val = &qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal}
	case qvalue.QValueKindFloat32:
		floatVal := value.(float32)
		val = &qvalue.QValue{Kind: qvalue.QValueKindFloat32, Value: floatVal}
	case qvalue.QValueKindFloat64:
		floatVal := value.(float64)
		val = &qvalue.QValue{Kind: qvalue.QValueKindFloat64, Value: floatVal}
	case qvalue.QValueKindString:
		// handling all unsupported types with strings as well for now.
		textVal := value
		val = &qvalue.QValue{Kind: qvalue.QValueKindString, Value: fmt.Sprint(textVal)}
	case qvalue.QValueKindUUID:
		switch value.(type) {
		case string:
			val = &qvalue.QValue{Kind: qvalue.QValueKindUUID, Value: value}
		case [16]byte:
			val = &qvalue.QValue{Kind: qvalue.QValueKindUUID, Value: value}
		default:
			return nil, fmt.Errorf("failed to parse UUID: %v", value)
		}
	case qvalue.QValueKindBytes:
		rawBytes := value.([]byte)
		val = &qvalue.QValue{Kind: qvalue.QValueKindBytes, Value: rawBytes}
	case qvalue.QValueKindBit:
		bitsVal := value.(pgtype.Bits)
		if bitsVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindBit, Value: bitsVal.Bytes}
		}
	case qvalue.QValueKindNumeric:
		numVal := value.(pgtype.Numeric)
		if numVal.Valid {
			rat, err := numericToRat(&numVal)
			if err != nil {
				return nil, fmt.Errorf("failed to convert numeric [%v] to rat: %w", value, err)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: rat}
		}
	case qvalue.QValueKindArrayFloat32:
		switch v := value.(type) {
		case pgtype.Array[float32]:
			if v.Valid {
				val = &qvalue.QValue{Kind: qvalue.QValueKindArrayFloat32, Value: v.Elements}
			}
		case []float32:
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayFloat32, Value: v}
		case []interface{}:
			float32Array := make([]float32, len(v))
			for i, val := range v {
				float32Array[i] = val.(float32)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayFloat32, Value: float32Array}
		default:
			return nil, fmt.Errorf("failed to parse array float32: %v", value)
		}
	case qvalue.QValueKindArrayFloat64:
		switch v := value.(type) {
		case pgtype.Array[float64]:
			if v.Valid {
				val = &qvalue.QValue{Kind: qvalue.QValueKindArrayFloat64, Value: v.Elements}
			}
		case []float64:
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayFloat64, Value: v}
		case []interface{}:
			float64Array := make([]float64, len(v))
			for i, val := range v {
				float64Array[i] = val.(float64)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayFloat64, Value: float64Array}
		default:
			return nil, fmt.Errorf("failed to parse array float64: %v", value)
		}
	case qvalue.QValueKindArrayInt32:
		switch v := value.(type) {
		case pgtype.Array[int32]:
			if v.Valid {
				val = &qvalue.QValue{Kind: qvalue.QValueKindArrayInt32, Value: v.Elements}
			}
		case []int32:
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayInt32, Value: v}
		case []interface{}:
			int32Array := make([]int32, len(v))
			for i, val := range v {
				int32Array[i] = val.(int32)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayInt32, Value: int32Array}
		default:
			return nil, fmt.Errorf("failed to parse array int32: %v", value)
		}
	case qvalue.QValueKindArrayInt64:
		switch v := value.(type) {
		case pgtype.Array[int64]:
			if v.Valid {
				val = &qvalue.QValue{Kind: qvalue.QValueKindArrayInt64, Value: v.Elements}
			}
		case []int64:
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayInt64, Value: v}
		case []interface{}:
			int64Array := make([]int64, len(v))
			for i, val := range v {
				int64Array[i] = val.(int64)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayInt64, Value: int64Array}
		default:
			return nil, fmt.Errorf("failed to parse array int64: %v", value)
		}
	case qvalue.QValueKindArrayString:
		switch v := value.(type) {
		case pgtype.Array[string]:
			if v.Valid {
				val = &qvalue.QValue{Kind: qvalue.QValueKindArrayString, Value: v.Elements}
			}
		case []string:
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayString, Value: v}
		case []interface{}:
			stringArray := make([]string, len(v))
			for i, val := range v {
				stringArray[i] = val.(string)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindArrayString, Value: stringArray}
		default:
			return nil, fmt.Errorf("failed to parse array string: %v", value)
		}
	case qvalue.QValueKindHStore:
		hstoreVal, err := value.(pgtype.Hstore).HstoreValue()
		if err != nil {
			return nil, fmt.Errorf("failed to parse hstore: %w", err)
		}
		val = &qvalue.QValue{Kind: qvalue.QValueKindHStore, Value: hstoreVal}
	case qvalue.QValueKindPoint:
		xCoord := value.(pgtype.Point).P.X
		yCoord := value.(pgtype.Point).P.Y
		val = &qvalue.QValue{Kind: qvalue.QValueKindPoint,
			Value: fmt.Sprintf("POINT(%f %f)", xCoord, yCoord)}
	default:
		// log.Warnf("unhandled QValueKind => %v, parsing as string", qvalueKind)
		textVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("failed to parse value %v into QValueKind %v", value, qvalueKind)
		}
		val = &qvalue.QValue{Kind: qvalue.QValueKindString, Value: textVal}
	}

	// parsing into pgtype failed.
	if val == nil {
		return nil, fmt.Errorf("failed to parse value %v into QValueKind %v", value, qvalueKind)
	}
	return val, nil
}

func parseFieldFromPostgresOID(oid uint32, value interface{}) (*qvalue.QValue, error) {
	return parseFieldFromQValueKind(postgresOIDToQValueKind(oid), value)
}

func numericToRat(numVal *pgtype.Numeric) (*big.Rat, error) {
	if numVal.Valid {
		if numVal.NaN {
			return nil, errors.New("numeric value is NaN")
		}

		switch numVal.InfinityModifier {
		case pgtype.NegativeInfinity, pgtype.Infinity:
			return nil, errors.New("numeric value is infinity")
		}

		rat := new(big.Rat)

		rat.SetInt(numVal.Int)
		divisor := new(big.Rat).SetFloat64(math.Pow10(int(-numVal.Exp)))
		rat.Quo(rat, divisor)

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
	default:
		qValueKind = qvalue.QValueKindString
	}
	return qValueKind
}

// returns the WKT representation of the geometry object if it is valid
func GeoValidate(hexWkb string) (string, error) {
	// Decode the WKB hex string into binary
	wkb, hexErr := hex.DecodeString(hexWkb)
	if hexErr != nil {
		log.Warnf("Ignoring invalid WKB: %s", hexWkb)
		return "", hexErr
	}

	// UnmarshalWKB performs geometry validation along with WKB parsing
	geometryObject, geoErr := geom.NewGeomFromWKB(wkb)
	if geoErr != nil {
		log.Warnf("Ignoring invalid geometry WKB %s: %v", hexWkb, geoErr)
		return "", geoErr
	}

	invalidReason := geometryObject.IsValidReason()
	if invalidReason != "Valid Geometry" {
		log.Warnf("Ignoring invalid geometry shape %s: %s", hexWkb, invalidReason)
		return "", errors.New(invalidReason)
	}

	wkt := geometryObject.ToWKT()
	return wkt, nil
}
