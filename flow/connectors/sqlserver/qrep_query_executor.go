package connsqlserver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/PeerDB-io/peer-flow/model"
	_ "github.com/microsoft/go-mssqldb"
)

type SQLServerQueryExecutor struct {
	db  *sql.DB
	ctx context.Context
}

func NewSQLServerQueryExecutor(db *sql.DB, ctx context.Context) *SQLServerQueryExecutor {
	return &SQLServerQueryExecutor{
		db:  db,
		ctx: ctx,
	}
}

func (qe *SQLServerQueryExecutor) ExecuteQuery(query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := qe.db.QueryContext(qe.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (qe *SQLServerQueryExecutor) ProcessRows(rows *sql.Rows) (*model.QRecordBatch, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	records := make([]*model.QRecord, 0)

	for rows.Next() {
		record, err := mapRowToQRecord(rows, columns)
		if err != nil {
			return nil, fmt.Errorf("failed to map row to QRecord: %w", err)
		}
		records = append(records, record)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("row iteration failed: %w", rows.Err())
	}

	batch := &model.QRecordBatch{
		NumRecords: uint32(len(records)),
		Records:    records,
		Schema:     columnsToSchema(columns),
	}

	return batch, nil
}

func (qe *SQLServerQueryExecutor) ExecuteAndProcessQuery(query string, args ...interface{}) (*model.QRecordBatch, error) {
	rows, err := qe.ExecuteQuery(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	batch, err := qe.ProcessRows(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to process rows: %w", err)
	}

	return batch, nil
}

func mapRowToQRecord(row *sql.Rows, columns []string) (*model.QRecord, error) {
	record := model.NewQRecord(len(columns))

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := row.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, val := range values {
		record.Set(i, val)
	}

	return record, nil
}

func columnsToSchema(columns []string) *model.QRecordSchema {
	qfields := make([]*model.QField, len(columns))
	for i, column := range columns {
		qfields[i] = &model.QField{
			Name:     column,
			Type:     model.QValueKindString, // TODO: adapt this to your needs
			Nullable: true, // TODO: adapt this to your needs
		}
	}
	return model.NewQRecordSchema(qfields)
}
