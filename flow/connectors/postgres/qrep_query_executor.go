package connpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/big"

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

	// get col names from fieldDescriptions
	colNames := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		colNames[i] = fd.Name
	}

	batch := &model.QRecordBatch{
		NumRecords:  uint32(len(records)),
		Records:     records,
		ColumnNames: colNames,
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
		case pgtype.TimestampOID, pgtype.TimestamptzOID:
			scanArgs[i] = new(pgtype.Timestamp)
		case pgtype.Int4OID, pgtype.Int8OID:
			scanArgs[i] = new(pgtype.Int8)
		case pgtype.Float4OID, pgtype.Float8OID:
			scanArgs[i] = new(pgtype.Float8)
		case pgtype.TextOID, pgtype.VarcharOID:
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
		tmp := parseField(fd.DataTypeOID, scanArgs[i])
		record.Set(i, tmp)
	}

	return record, nil
}

func parseField(oid uint32, value interface{}) model.QValue {
	var val model.QValue

	switch oid {
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		time := value.(*pgtype.Timestamp)
		if time.Valid {
			et := model.NewExtendedTime(time.Time, model.DateTimeKindType, "")
			val = model.QValue{Kind: model.QValueKindETime, Value: et}
		} else {
			val = model.QValue{Kind: model.QValueKindETime, Value: nil}
		}
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

	return val
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
