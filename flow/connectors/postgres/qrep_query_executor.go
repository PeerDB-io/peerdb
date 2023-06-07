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
) ([]*model.QRecord, error) {
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

	return records, nil
}

func mapRowToQRecord(row pgx.Row, fds []pgconn.FieldDescription) (*model.QRecord, error) {
	record := &model.QRecord{}

	scanArgs := make([]interface{}, len(fds))
	for i := range scanArgs {
		switch fds[i].DataTypeOID {
		case pgtype.BoolOID:
			scanArgs[i] = new(sql.NullBool)
		case pgtype.TimestampOID, pgtype.TimestamptzOID:
			scanArgs[i] = new(sql.NullTime)
		case pgtype.Int4OID, pgtype.Int8OID:
			scanArgs[i] = new(sql.NullInt64)
		case pgtype.Float4OID, pgtype.Float8OID:
			scanArgs[i] = new(sql.NullFloat64)
		case pgtype.TextOID, pgtype.VarcharOID:
			scanArgs[i] = new(sql.NullString)
		case pgtype.NumericOID:
			scanArgs[i] = new(sql.NullString)
		case pgtype.UUIDOID:
			scanArgs[i] = new(pgtype.UUID)
		case pgtype.ByteaOID:
			scanArgs[i] = new(sql.RawBytes)
		default:
			scanArgs[i] = new(sql.RawBytes)
		}
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, fd := range fds {
		(*record)[fd.Name] = parseField(fd.DataTypeOID, scanArgs[i])
	}

	return record, nil
}

func parseField(oid uint32, value interface{}) model.QValue {
	var val model.QValue

	switch oid {
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		nullTime := value.(*sql.NullTime)
		if nullTime.Valid {
			et := model.NewExtendedTime(nullTime.Time, model.DateTimeKindType, "")
			val = model.QValue{Kind: model.QValueKindETime, Value: et}
		} else {
			val = model.QValue{Kind: model.QValueKindETime, Value: nil}
		}
	case pgtype.BoolOID:
		nullBool := value.(*sql.NullBool)
		if nullBool.Valid {
			val = model.QValue{Kind: model.QValueKindBoolean, Value: nullBool.Bool}
		} else {
			val = model.QValue{Kind: model.QValueKindBoolean, Value: nil}
		}
	case pgtype.Int4OID, pgtype.Int8OID:
		nullInt := value.(*sql.NullInt64)
		if nullInt.Valid {
			val = model.QValue{Kind: model.QValueKindInteger, Value: nullInt.Int64}
		} else {
			val = model.QValue{Kind: model.QValueKindInteger, Value: nil}
		}
	case pgtype.Float4OID, pgtype.Float8OID:
		nullFloat := value.(*sql.NullFloat64)
		if nullFloat.Valid {
			val = model.QValue{Kind: model.QValueKindFloat, Value: nullFloat.Float64}
		} else {
			val = model.QValue{Kind: model.QValueKindFloat, Value: nil}
		}
	case pgtype.TextOID, pgtype.VarcharOID:
		nullStr := value.(*sql.NullString)
		if nullStr.Valid {
			val = model.QValue{Kind: model.QValueKindString, Value: nullStr.String}
		} else {
			val = model.QValue{Kind: model.QValueKindString, Value: nil}
		}
	case pgtype.UUIDOID:
		uuid := value.(*pgtype.UUID)
		if uuid.Valid {
			val = model.QValue{Kind: model.QValueKindUUID, Value: uuid.Bytes}
		} else {
			val = model.QValue{Kind: model.QValueKindUUID, Value: nil}
		}
	case pgtype.ByteaOID:
		rawBytes := value.(*sql.RawBytes)
		val = model.QValue{Kind: model.QValueKindBytes, Value: []byte(*rawBytes)}
	case pgtype.NumericOID:
		nullStr := value.(*sql.NullString)
		if nullStr.Valid {
			val = model.QValue{Kind: model.QValueKindNumeric, Value: nullStr.String}
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
