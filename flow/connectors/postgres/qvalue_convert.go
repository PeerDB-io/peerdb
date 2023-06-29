package connpostgres

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/big"

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
	case pgtype.TextOID, pgtype.VarcharOID:
		return qvalue.QValueKindString
	case pgtype.ByteaOID:
		return qvalue.QValueKindBytes
	case pgtype.JSONOID, pgtype.JSONBOID:
		return qvalue.QValueKindJSON
	case pgtype.UUIDOID:
		return qvalue.QValueKindUUID
	case pgtype.TimestampOID:
		return qvalue.QValueKindETime
	case pgtype.NumericOID:
		return qvalue.QValueKindNumeric
	default:
		typeName, ok := pgtype.NewMap().TypeForOID(oid)
		if !ok {
			log.Warnf("failed to get type name for oid: %v", oid)
			return qvalue.QValueKindInvalid
		} else {
			log.Warnf("unsupported field type: %v - type name - %s", oid, typeName.Name)
			return qvalue.QValueKindString
		}
	}
}

func parseFieldFromPostgresOID(oid uint32, value interface{}) (qvalue.QValue, error) {
	return parseFieldFromQValueKind(getQValueKindForPostgresOID(oid), value)
}

func parseFieldFromQValueKind(qvalueKind qvalue.QValueKind, value interface{}) (qvalue.QValue, error) {
	var val qvalue.QValue

	switch qvalueKind {
	case qvalue.QValueKindETime:
		timestamp := value.(*pgtype.Timestamp)
		var et *qvalue.ExtendedTime
		if timestamp.Valid {
			var err error
			et, err = qvalue.NewExtendedTime(timestamp.Time, qvalue.DateTimeKindType, "")
			if err != nil {
				return qvalue.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
			}
		}
		val = qvalue.QValue{Kind: qvalue.QValueKindETime, Value: et}
	// TODO: properly handle time types
	// case pgtype.TimestamptzOID:
	// 	timestamp := value.(*pgtype.Timestamptz)
	// 	var et *qvalue.ExtendedTime
	// 	if timestamp.Valid {
	// 		var err error
	// 		et, err = qvalue.NewExtendedTime(timestamp.Time, qvalue.DateTimeKindType, "")
	// 		if err != nil {
	// 			return qvalue.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
	// 		}
	// 	}
	// 	val = qvalue.QValue{Kind: qvalue.QValueKindETime, Value: et}
	// case pgtype.DateOID:
	// 	date := value.(*pgtype.Date)
	// 	var et *qvalue.ExtendedTime
	// 	if date.Valid {
	// 		var err error
	// 		et, err = qvalue.NewExtendedTime(date.Time, qvalue.DateKindType, "")
	// 		if err != nil {
	// 			return qvalue.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
	// 		}
	// 	}
	// 	val = qvalue.QValue{Kind: qvalue.QValueKindETime, Value: et}
	// case pgtype.TimeOID:
	// 	timeVal := value.(*pgtype.Text)
	// 	var et *qvalue.ExtendedTime
	// 	if timeVal.Valid {
	// 		t, err := time.Parse("15:04:05.999999", timeVal.String)
	// 		if err != nil {
	// 			return qvalue.QValue{}, fmt.Errorf("failed to parse time: %w", err)
	// 		}
	// 		et, err = qvalue.NewExtendedTime(t, qvalue.TimeKindType, "")
	// 		if err != nil {
	// 			return qvalue.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
	// 		}
	// 	}
	// 	val = qvalue.QValue{Kind: qvalue.QValueKindETime, Value: et}
	case qvalue.QValueKindBoolean:
		boolVal := value.(*pgtype.Bool)
		if boolVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: boolVal.Bool}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: nil}
		}
	case qvalue.QValueKindJSON:
		// TODO: improve JSON support
		strVal := value.(*pgtype.Text)
		if strVal != nil {
			val = qvalue.QValue{Kind: qvalue.QValueKindJSON, Value: strVal.String}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindJSON, Value: nil}
		}
	case qvalue.QValueKindInt16:
		intVal := value.(*pgtype.Int2)
		if intVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindInt16, Value: intVal.Int16}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindInt16, Value: nil}
		}
	case qvalue.QValueKindInt32:
		intVal := value.(*pgtype.Int4)
		if intVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: intVal.Int32}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: nil}
		}
	case qvalue.QValueKindInt64:
		intVal := value.(*pgtype.Int8)
		if intVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: intVal.Int64}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: nil}
		}
	// TODO: check for handling of QValueKindFloat16
	case qvalue.QValueKindFloat32:
		floatVal := value.(*pgtype.Float4)
		if floatVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindFloat32, Value: floatVal.Float32}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindFloat32, Value: nil}
		}
	case qvalue.QValueKindFloat64:
		floatVal := value.(*pgtype.Float8)
		if floatVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindFloat64, Value: floatVal.Float64}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindFloat64, Value: nil}
		}
	case qvalue.QValueKindString:
		textVal := value.(*pgtype.Text)
		if textVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindString, Value: textVal.String}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindString, Value: nil}
		}
	case qvalue.QValueKindUUID:
		uuidVal := value.(*pgtype.UUID)
		if uuidVal.Valid {
			val = qvalue.QValue{Kind: qvalue.QValueKindUUID, Value: uuidVal.Bytes}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindUUID, Value: nil}
		}
	case qvalue.QValueKindBytes:
		rawBytes := value.(*sql.RawBytes)
		val = qvalue.QValue{Kind: qvalue.QValueKindBytes, Value: []byte(*rawBytes)}
	// TODO: check for handling of QValueKindBit
	case qvalue.QValueKindNumeric:
		numVal := value.(*pgtype.Numeric)
		rat, err := numericToRat(numVal)
		if err != nil {
			log.Warnf("failed to convert numeric [%v] to rat: %v", value, err)
			val = qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: nil}
		} else {
			val = qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: rat}
		}
	default:
		fmt.Printf("unhandled QValueKind => %v\n", qvalueKind)
		val = qvalue.QValue{Kind: qvalue.QValueKindInvalid, Value: nil}
	}

	return val, nil
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
