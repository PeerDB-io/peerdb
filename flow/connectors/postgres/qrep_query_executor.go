package connpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type QRepQueryExecutor struct {
	pool *pgxpool.Pool
	ctx  context.Context
}

func NewQRepQueryExecutor(pool *pgxpool.Pool, ctx context.Context) *QRepQueryExecutor {
	return &QRepQueryExecutor{
		pool: pool,
		ctx:  ctx,
	}
}

func (qe *QRepQueryExecutor) ExecuteQuery(query string, args ...interface{}) (pgx.Rows, error) {
	rows, err := qe.pool.Query(qe.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func fieldDescriptionToQValueKind(fd pgconn.FieldDescription) model.QValueKind {
	switch fd.DataTypeOID {
	case pgtype.BoolOID:
		return model.QValueKindBoolean
	case pgtype.Int2OID:
		return model.QValueKindInt16
	case pgtype.Int4OID:
		return model.QValueKindInt32
	case pgtype.Int8OID:
		return model.QValueKindInt64
	case pgtype.Float4OID:
		return model.QValueKindFloat32
	case pgtype.Float8OID:
		return model.QValueKindFloat64
	case pgtype.TextOID, pgtype.VarcharOID:
		return model.QValueKindString
	case pgtype.ByteaOID:
		return model.QValueKindBytes
	case pgtype.JSONOID, pgtype.JSONBOID:
		return model.QValueKindJSON
	case pgtype.UUIDOID:
		return model.QValueKindUUID
	case pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.DateOID, pgtype.TimeOID:
		return model.QValueKindETime
	case pgtype.NumericOID:
		return model.QValueKindNumeric
	default:
		return model.QValueKindInvalid
	}
}

// FieldDescriptionsToSchema converts a slice of pgconn.FieldDescription to a QRecordSchema.
func fieldDescriptionsToSchema(fds []pgconn.FieldDescription) *model.QRecordSchema {
	qfields := make([]*model.QField, len(fds))
	for i, fd := range fds {
		cname := fd.Name
		ctype := fieldDescriptionToQValueKind(fd)
		// there isn't a way to know if a column is nullable or not
		// TODO fix this.
		cnullable := true
		qfields[i] = &model.QField{
			Name:     cname,
			Type:     ctype,
			Nullable: cnullable,
		}
	}
	return model.NewQRecordSchema(qfields)
}

func (qe *QRepQueryExecutor) ProcessRows(
	rows pgx.Rows,
	fieldDescriptions []pgconn.FieldDescription,
) (*model.QRecordBatch, error) {
	// Initialize the record slice
	records := make([]*model.QRecord, 0)

	// Iterate over the rows
	for rows.Next() {
		record, err := mapRowToQRecord(rows, fieldDescriptions)
		if err != nil {
			return nil, fmt.Errorf("failed to map row to QRecord: %w", err)
		}
		records = append(records, record)
	}

	// Check for any errors encountered during iteration
	if rows.Err() != nil {
		return nil, fmt.Errorf("row iteration failed: %w", rows.Err())
	}

	batch := &model.QRecordBatch{
		NumRecords: uint32(len(records)),
		Records:    records,
		Schema:     fieldDescriptionsToSchema(fieldDescriptions),
	}

	return batch, nil
}

func (qe *QRepQueryExecutor) ExecuteAndProcessQuery(
	query string,
	args ...interface{},
) (*model.QRecordBatch, error) {
	rows, err := qe.ExecuteQuery(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Use rows.FieldDescriptions() to get field descriptions
	fieldDescriptions := rows.FieldDescriptions()

	batch, err := qe.ProcessRows(rows, fieldDescriptions)
	if err != nil {
		return nil, fmt.Errorf("failed to process rows: %w", err)
	}

	return batch, nil
}

func mapRowToQRecord(row pgx.Row, fds []pgconn.FieldDescription) (*model.QRecord, error) {
	// make vals an empty array of QValue of size len(fds)
	record := model.NewQRecord(len(fds))

	scanArgs := make([]interface{}, len(fds))
	for i := range scanArgs {
		switch fds[i].DataTypeOID {
		case pgtype.BoolOID:
			scanArgs[i] = new(pgtype.Bool)
		case pgtype.TimestampOID:
			scanArgs[i] = new(pgtype.Timestamp)
		case pgtype.TimestamptzOID:
			scanArgs[i] = new(pgtype.Timestamptz)
		case pgtype.Int4OID:
			scanArgs[i] = new(pgtype.Int4)
		case pgtype.Int8OID:
			scanArgs[i] = new(pgtype.Int8)
		case pgtype.Float4OID:
			scanArgs[i] = new(pgtype.Float4)
		case pgtype.Float8OID:
			scanArgs[i] = new(pgtype.Float8)
		case pgtype.TextOID:
			scanArgs[i] = new(pgtype.Text)
		case pgtype.VarcharOID:
			scanArgs[i] = new(pgtype.Text)
		case pgtype.NumericOID:
			scanArgs[i] = new(pgtype.Numeric)
		case pgtype.UUIDOID:
			scanArgs[i] = new(pgtype.UUID)
		case pgtype.ByteaOID:
			scanArgs[i] = new(sql.RawBytes)
		default:
			scanArgs[i] = new(pgtype.Text)
		}
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, fd := range fds {
		tmp, err := parseField(fd.DataTypeOID, scanArgs[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse field: %w", err)
		}
		record.Set(i, tmp)
	}

	return record, nil
}

func parseField(oid uint32, value interface{}) (model.QValue, error) {
	var val model.QValue

	switch oid {
	case pgtype.TimestampOID:
		timestamp := value.(*pgtype.Timestamp)
		var et *model.ExtendedTime
		if timestamp.Valid {
			var err error
			et, err = model.NewExtendedTime(timestamp.Time, model.DateTimeKindType, "")
			if err != nil {
				return model.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
			}
		}
		val = model.QValue{Kind: model.QValueKindETime, Value: et}
	case pgtype.TimestamptzOID:
		timestamp := value.(*pgtype.Timestamptz)
		var et *model.ExtendedTime
		if timestamp.Valid {
			var err error
			et, err = model.NewExtendedTime(timestamp.Time, model.DateTimeKindType, "")
			if err != nil {
				return model.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
			}
		}
		val = model.QValue{Kind: model.QValueKindETime, Value: et}
	case pgtype.DateOID:
		date := value.(*pgtype.Date)
		var et *model.ExtendedTime
		if date.Valid {
			var err error
			et, err = model.NewExtendedTime(date.Time, model.DateKindType, "")
			if err != nil {
				return model.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
			}
		}
		val = model.QValue{Kind: model.QValueKindETime, Value: et}
	case pgtype.TimeOID:
		timeVal := value.(*pgtype.Time)
		var et *model.ExtendedTime
		if timeVal.Valid {
			var err error
			t := time.Unix(0, timeVal.Microseconds*int64(time.Microsecond))
			et, err = model.NewExtendedTime(t, model.TimeKindType, "")
			if err != nil {
				return model.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
			}
		}
		val = model.QValue{Kind: model.QValueKindETime, Value: et}
	case pgtype.BoolOID:
		boolVal := value.(*pgtype.Bool)
		if boolVal.Valid {
			val = model.QValue{Kind: model.QValueKindBoolean, Value: boolVal.Bool}
		} else {
			val = model.QValue{Kind: model.QValueKindBoolean, Value: nil}
		}
	case pgtype.JSONOID, pgtype.JSONBOID:
		// TODO: improve JSON support
		strVal := value.(*string)
		if strVal != nil {
			val = model.QValue{Kind: model.QValueKindJSON, Value: *strVal}
		} else {
			val = model.QValue{Kind: model.QValueKindJSON, Value: nil}
		}
	case pgtype.Int2OID:
		intVal := value.(*pgtype.Int2)
		if intVal.Valid {
			val = model.QValue{Kind: model.QValueKindInt16, Value: intVal.Int16}
		} else {
			val = model.QValue{Kind: model.QValueKindInt16, Value: nil}
		}
	case pgtype.Int4OID:
		intVal := value.(*pgtype.Int4)
		if intVal.Valid {
			val = model.QValue{Kind: model.QValueKindInt32, Value: intVal.Int32}
		} else {
			val = model.QValue{Kind: model.QValueKindInt32, Value: nil}
		}
	case pgtype.Int8OID:
		intVal := value.(*pgtype.Int8)
		if intVal.Valid {
			val = model.QValue{Kind: model.QValueKindInt64, Value: intVal.Int64}
		} else {
			val = model.QValue{Kind: model.QValueKindInt64, Value: nil}
		}
	case pgtype.Float4OID:
		floatVal := value.(*pgtype.Float4)
		if floatVal.Valid {
			val = model.QValue{Kind: model.QValueKindFloat32, Value: floatVal.Float32}
		} else {
			val = model.QValue{Kind: model.QValueKindFloat32, Value: nil}
		}
	case pgtype.Float8OID:
		floatVal := value.(*pgtype.Float8)
		if floatVal.Valid {
			val = model.QValue{Kind: model.QValueKindFloat64, Value: floatVal.Float64}
		} else {
			val = model.QValue{Kind: model.QValueKindFloat64, Value: nil}
		}
	case pgtype.TextOID, pgtype.VarcharOID:
		textVal := value.(*pgtype.Text)
		if textVal.Valid {
			val = model.QValue{Kind: model.QValueKindString, Value: textVal.String}
		} else {
			val = model.QValue{Kind: model.QValueKindString, Value: nil}
		}
	case pgtype.UUIDOID:
		uuidVal := value.(*pgtype.UUID)
		if uuidVal.Valid {
			val = model.QValue{Kind: model.QValueKindUUID, Value: uuidVal.Bytes}
		} else {
			val = model.QValue{Kind: model.QValueKindUUID, Value: nil}
		}
	case pgtype.ByteaOID:
		rawBytes := value.(*sql.RawBytes)
		val = model.QValue{Kind: model.QValueKindBytes, Value: []byte(*rawBytes)}
	case pgtype.NumericOID:
		numVal := value.(*pgtype.Numeric)
		rat, err := numericToRat(numVal)
		if err != nil {
			val = model.QValue{Kind: model.QValueKindInvalid, Value: nil}
		} else {
			val = model.QValue{Kind: model.QValueKindNumeric, Value: rat}
		}
	default:
		typ, _ := pgtype.NewMap().TypeForOID(oid)
		fmt.Printf("QValueKindInvalid => oid: %v, typename: %v\n", oid, typ)
		val = model.QValue{Kind: model.QValueKindInvalid, Value: nil}
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
