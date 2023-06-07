package connpostgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
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
		if fds[i].DataTypeOID == pgtype.BoolOID {
			scanArgs[i] = new(sql.NullBool)
		} else if fds[i].DataTypeOID == pgtype.TimestampOID || fds[i].DataTypeOID == pgtype.TimestamptzOID {
			scanArgs[i] = new(sql.NullTime)
		} else {
			scanArgs[i] = new(sql.RawBytes)
		}
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, fd := range fds {
		oid := fd.DataTypeOID
		var val model.QValue
		var err error

		switch oid {
		case pgtype.TimestampOID, pgtype.TimestamptzOID:
			nullTime := scanArgs[i].(*sql.NullTime)
			if nullTime.Valid {
				et, err := model.NewExtendedTime(nullTime.Time, model.DateTimeKindType, "")
				if err != nil {
					return nil, fmt.Errorf("failed to create extended time: %w", err)
				}
				val = model.QValue{Kind: model.QValueKindETime, Value: et}
			} else {
				val = model.QValue{Kind: model.QValueKindETime, Value: nil}
			}
		case pgtype.BoolOID:
			nullBool := scanArgs[i].(*sql.NullBool)
			if nullBool.Valid {
				val = model.QValue{Kind: model.QValueKindBoolean, Value: nullBool.Bool}
			} else {
				val = model.QValue{Kind: model.QValueKindBoolean, Value: nil}
			}
		default:
			rawBytes := scanArgs[i].(*sql.RawBytes)
			val, err = parseField(oid, rawBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse field: %w", err)
			}
		}

		(*record)[fd.Name] = val
	}

	return record, nil
}

func parseField(oid uint32, rawBytes *sql.RawBytes) (model.QValue, error) {
	switch oid {
	case pgtype.Int4OID, pgtype.Int8OID:
		val, err := strconv.ParseInt(string(*rawBytes), 10, 64)
		if err != nil {
			return model.QValue{}, err
		}
		return model.QValue{Kind: model.QValueKindInteger, Value: val}, nil
	case pgtype.Float4OID, pgtype.Float8OID:
		val, err := strconv.ParseFloat(string(*rawBytes), 64)
		if err != nil {
			return model.QValue{}, err
		}
		return model.QValue{Kind: model.QValueKindFloat, Value: val}, nil
	case pgtype.BoolOID:
		val, err := strconv.ParseBool(string(*rawBytes))
		if err != nil {
			return model.QValue{}, err
		}
		return model.QValue{Kind: model.QValueKindBoolean, Value: val}, nil
	case pgtype.TextOID, pgtype.VarcharOID:
		return model.QValue{Kind: model.QValueKindString, Value: string(*rawBytes)}, nil
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		val, err := time.Parse(time.RFC3339, string(*rawBytes))
		if err != nil {
			return model.QValue{}, err
		}
		et, err := model.NewExtendedTime(val, model.DateTimeKindType, "")
		if err != nil {
			return model.QValue{}, fmt.Errorf("failed to create extended time: %w", err)
		}
		return model.QValue{Kind: model.QValueKindETime, Value: et}, nil
	case pgtype.NumericOID:
		return model.QValue{Kind: model.QValueKindNumeric, Value: string(*rawBytes)}, nil
	case pgtype.UUIDOID:
		return model.QValue{Kind: model.QValueKindString, Value: string(*rawBytes)}, nil
	case pgtype.ByteaOID:
		return model.QValue{Kind: model.QValueKindBytes, Value: []byte(*rawBytes)}, nil
	default:
		typ, _ := pgtype.NewMap().TypeForOID(oid)
		fmt.Printf("QValueKindInvalid => oid: %v, typename: %v\n", oid, typ)
		return model.QValue{Kind: model.QValueKindInvalid, Value: nil}, nil
	}
}
