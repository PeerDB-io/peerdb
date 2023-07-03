package connpostgres

import (
	"database/sql"
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
		timestamp := value.(*pgtype.Timestamp)
		if timestamp.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindTimestamp, Value: timestamp.Time}
		}
	case qvalue.QValueKindTimestampTZ:
		timestamp := value.(*pgtype.Timestamptz)
		if timestamp.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindTimestampTZ, Value: timestamp.Time}
		}
	case qvalue.QValueKindDate:
		date := value.(*pgtype.Date)
		if date.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindDate, Value: date.Time}
		}
	case qvalue.QValueKindTime:
		timeVal := value.(*pgtype.Text)
		if timeVal.Valid {
			// edge case, only Postgres supports this extreme value for time
			timeVal.String = strings.Replace(timeVal.String, "24:00:00.000000", "23:59:59.999999", 1)
			t, err := time.Parse("15:04:05.999999", timeVal.String)
			if err != nil {
				return nil, fmt.Errorf("failed to parse time: %w", err)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindTime, Value: t}
		}
	case qvalue.QValueKindTimeTZ:
		timeVal := value.(*pgtype.Text)
		if timeVal.Valid {
			// edge case, Postgres supports this extreme value for time
			timeVal.String = strings.Replace(timeVal.String, "24:00:00.000000", "23:59:59.999999", 1)
			t, err := time.Parse("15:04:05.999999-07:00", timeVal.String)
			if err != nil {
				return nil, fmt.Errorf("failed to parse time: %w", err)
			}
			val = &qvalue.QValue{Kind: qvalue.QValueKindTime, Value: t}
		}
	case qvalue.QValueKindBoolean:
		boolVal := value.(*pgtype.Bool)
		if boolVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: boolVal.Bool}
		}
	case qvalue.QValueKindJSON:
		// TODO: improve JSON support
		strVal := value.(*pgtype.Text)
		if strVal != nil {
			val = &qvalue.QValue{Kind: qvalue.QValueKindJSON, Value: strVal.String}
		}
	case qvalue.QValueKindInt16:
		intVal, ok := value.(*pgtype.Int2)
		if ok && intVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal.Int16}
		} else if !ok {
			intVal2 := value.(int16)
			val = &qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal2}
		}
	case qvalue.QValueKindInt32:
		intVal, ok := value.(*pgtype.Int4)
		if ok && intVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal.Int32}
		} else if !ok {
			intVal2 := value.(int32)
			val = &qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal2}
		}
	case qvalue.QValueKindInt64:
		intVal, ok := value.(*pgtype.Int8)
		if ok && intVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal.Int64}
		} else if !ok {
			intVal2 := value.(int64)
			val = &qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal2}
		}
	// TODO: check for handling of QValueKindFloat16
	case qvalue.QValueKindFloat32:
		floatVal := value.(*pgtype.Float4)
		if floatVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindFloat32, Value: floatVal.Float32}
		}
	case qvalue.QValueKindFloat64:
		floatVal := value.(*pgtype.Float8)
		if floatVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindFloat64, Value: floatVal.Float64}
		}
	case qvalue.QValueKindString:
		textVal := value.(*pgtype.Text)
		if textVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindString, Value: textVal.String}
		}
	case qvalue.QValueKindUUID:
		uuidVal := value.(*pgtype.UUID)
		if uuidVal.Valid {
			val = &qvalue.QValue{Kind: qvalue.QValueKindUUID, Value: uuidVal.Bytes}
		}
	case qvalue.QValueKindBytes:
		rawBytes := value.(*sql.RawBytes)
		val = &qvalue.QValue{Kind: qvalue.QValueKindBytes, Value: []byte(*rawBytes)}
	// TODO: check for handling of QValueKindBit
	case qvalue.QValueKindNumeric:
		numVal := value.(*pgtype.Numeric)
		if numVal.Valid {
			rat, err := numericToRat(numVal)
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
