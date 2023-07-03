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
	log "github.com/sirupsen/logrus"
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

// FieldDescriptionsToSchema converts a slice of pgconn.FieldDescription to a QRecordSchema.
func fieldDescriptionsToSchema(fds []pgconn.FieldDescription) *model.QRecordSchema {
	qfields := make([]*model.QField, len(fds))
	for i, fd := range fds {
		cname := fd.Name
		ctype := getQValueKindForPostgresOID(fd.DataTypeOID)
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

	log.Infof("[postgres] pulled %d records", batch.NumRecords)

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
		case pgtype.DateOID:
			scanArgs[i] = new(pgtype.Date)
		default:
			scanArgs[i] = new(pgtype.Text)
		}
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, fd := range fds {
		tmp, err := parseFieldFromPostgresOID(fd.DataTypeOID, scanArgs[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse field: %w", err)
		}
		record.Set(i, *tmp)
	}

	return record, nil
}
