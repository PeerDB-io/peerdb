package connpostgres

import (
	"context"
	"database/sql"
	"fmt"

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

	batch := &model.QRecordBatch{
		NumRecords: uint32(len(records)),
		Records:    records,
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
	case pgtype.Int4OID, pgtype.Int8OID:
		intVal := value.(*pgtype.Int8)
		if intVal.Valid {
			val = model.QValue{Kind: model.QValueKindInteger, Value: intVal.Int64}
		} else {
			val = model.QValue{Kind: model.QValueKindInteger, Value: nil}
		}
	case pgtype.Float4OID, pgtype.Float8OID:
		floatVal := value.(*pgtype.Float8)
		if floatVal.Valid {
			val = model.QValue{Kind: model.QValueKindFloat, Value: floatVal.Float64}
		} else {
			val = model.QValue{Kind: model.QValueKindFloat, Value: nil}
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
		if numVal.Valid {
			str := numVal.Int.String()
			val = model.QValue{Kind: model.QValueKindNumeric, Value: str}
		} else {
			val = model.QValue{Kind: model.QValueKindNumeric, Value: nil}
		}
	default:
		typ, _ := pgtype.NewMap().TypeForOID(oid)
		fmt.Printf("QValueKindInvalid => oid: %v, typename: %v\n", oid, typ)
		val = model.QValue{Kind: model.QValueKindInvalid, Value: nil}
	}

	return val
}
