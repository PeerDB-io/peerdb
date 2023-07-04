package connpostgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

func getQValueKindForPostgresOID(oid uint32) qvalue.QValueKind {
	switch oid {
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
	default:
		typeName, ok := pgtype.NewMap().TypeForOID(oid)
		if !ok {
			// workaround for TIMETZ not being defined by this pgtype
			if oid == 1266 {
				return qvalue.QValueKindTimeTZ
			}
			log.Warnf("failed to get type name for oid: %v", oid)
			return qvalue.QValueKindInvalid
		} else {
			log.Warnf("unsupported field type: %v - type name - %s", oid, typeName.Name)
			return qvalue.QValueKindInvalid
		}
	}
}

func parseFieldFromQValueKind(qvalueKind qvalue.QValueKind, value interface{}) (*qvalue.QValue, error) {
	var val *qvalue.QValue = nil

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
		t, err := time.Parse("15:04:05.999999-0700", timeVal)
		if err != nil {
			return nil, fmt.Errorf("failed to parse time: %w", err)
		}
		t = t.AddDate(1970, 0, 0)
		val = &qvalue.QValue{Kind: qvalue.QValueKindTime, Value: t}

	case qvalue.QValueKindBoolean:
		boolVal := value.(bool)
		val = &qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: boolVal}
	case qvalue.QValueKindJSON:
		// TODO: improve JSON support
		jsonVal := value.(map[string]interface{})
		jsonValStr, err := json.Marshal(jsonVal)
		if err != nil {
			return nil, fmt.Errorf("failed to parse json: %w", err)
		}
		val = &qvalue.QValue{Kind: qvalue.QValueKindJSON, Value: string(jsonValStr)}
	case qvalue.QValueKindInt16:
		intVal := value.(int16)
		val = &qvalue.QValue{Kind: qvalue.QValueKindInt16, Value: intVal}
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
		textVal := value.(string)
		val = &qvalue.QValue{Kind: qvalue.QValueKindString, Value: textVal}
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
	// TODO: check for handling of QValueKindBit
	case qvalue.QValueKindNumeric:
		numVal := value.(pgtype.Numeric)
		if numVal.Valid {
			rat, err := numericToRat(&numVal)
			if err != nil {
				return nil, fmt.Errorf("failed to convert numeric [%v] to rat: %w", value, err)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: rat}
		}
	default:
		log.Errorf("unhandled QValueKind => %v\n", qvalueKind)
		return nil, fmt.Errorf("unhandled QValueKind => %v", qvalueKind)
	}

	// parsing into pgtype failed.
	if val == nil {
		return nil, fmt.Errorf("failed to parse value %v into QValueKind %v", value, qvalueKind)
	}
	return val, nil
}

func parseFieldFromPostgresOID(oid uint32, value interface{}) (*qvalue.QValue, error) {
	return parseFieldFromQValueKind(getQValueKindForPostgresOID(oid), value)
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
