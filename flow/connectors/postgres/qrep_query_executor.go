package connpostgres

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/model"
	util "github.com/PeerDB-io/peer-flow/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
)

type QRepQueryExecutor struct {
	pool     *pgxpool.Pool
	ctx      context.Context
	snapshot string
	testEnv  bool
}

func NewQRepQueryExecutor(pool *pgxpool.Pool, ctx context.Context) *QRepQueryExecutor {
	return &QRepQueryExecutor{
		pool:     pool,
		ctx:      ctx,
		snapshot: "",
	}
}

func NewQRepQueryExecutorSnapshot(pool *pgxpool.Pool, ctx context.Context, snapshot string) *QRepQueryExecutor {
	return &QRepQueryExecutor{
		pool:     pool,
		ctx:      ctx,
		snapshot: snapshot,
	}
}

func (qe *QRepQueryExecutor) SetTestEnv(testEnv bool) {
	qe.testEnv = testEnv
}

func (qe *QRepQueryExecutor) ExecuteQuery(query string, args ...interface{}) (pgx.Rows, error) {
	rows, err := qe.pool.Query(qe.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (qe *QRepQueryExecutor) executeQueryInTx(tx pgx.Tx, cursorName string, fetchSize int) (pgx.Rows, error) {
	rows, err := tx.Query(qe.ctx, fmt.Sprintf("FETCH %d FROM %s", fetchSize, cursorName))
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
		ctype := postgresOIDToQValueKind(fd.DataTypeOID)
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

func (qe *QRepQueryExecutor) ProcessRowsStream(
	stream *model.QRecordStream,
	rows pgx.Rows,
	fieldDescriptions []pgconn.FieldDescription,
) (int, error) {
	numRows := 0
	const heartBeatNumRows = 5000

	// Iterate over the rows
	for rows.Next() {
		record, err := mapRowToQRecord(rows, fieldDescriptions)
		if err != nil {
			stream.Records <- &model.QRecordOrError{
				Err: fmt.Errorf("failed to map row to QRecord: %w", err),
			}
			return 0, fmt.Errorf("failed to map row to QRecord: %w", err)
		}

		stream.Records <- &model.QRecordOrError{
			Record: record,
			Err:    nil,
		}

		if numRows%heartBeatNumRows == 0 {
			qe.recordHeartbeat("fetched %d records", numRows)
		}

		numRows++
	}

	qe.recordHeartbeat("fetched %d records", numRows)

	return numRows, nil
}

func (qe *QRepQueryExecutor) recordHeartbeat(x string, args ...interface{}) {
	if qe.testEnv {
		log.Infof(x, args...)
		return
	}
	msg := fmt.Sprintf(x, args...)
	activity.RecordHeartbeat(qe.ctx, msg)
}

func (qe *QRepQueryExecutor) processFetchedRows(
	query string,
	tx pgx.Tx,
	cursorName string,
	fetchSize int,
	stream *model.QRecordStream,
) (int, error) {
	rows, err := qe.executeQueryInTx(tx, cursorName, fetchSize)
	if err != nil {
		stream.Records <- &model.QRecordOrError{
			Err: err,
		}
		log.Errorf("[pg_query_executor] failed to execute query in tx: %v", err)
		return 0, fmt.Errorf("[pg_query_executor] failed to execute query in tx: %w", err)
	}

	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	if !stream.IsSchemaSet() {
		schema := fieldDescriptionsToSchema(fieldDescriptions)
		_ = stream.SetSchema(schema)
	}

	numRows, err := qe.ProcessRowsStream(stream, rows, fieldDescriptions)
	if err != nil {
		log.Errorf("[pg_query_executor] failed to process rows: %v", err)
		return 0, fmt.Errorf("failed to process rows: %w", err)
	}

	rows.Close()

	if rows.Err() != nil {
		stream.Records <- &model.QRecordOrError{
			Err: rows.Err(),
		}
		log.Errorf("[pg_query_executor] row iteration failed '%s': %v", query, rows.Err())
		return 0, fmt.Errorf("[pg_query_executor] row iteration failed '%s': %w", query, rows.Err())
	}

	return numRows, nil
}

func (qe *QRepQueryExecutor) ExecuteAndProcessQuery(
	query string,
	args ...interface{},
) (*model.QRecordBatch, error) {
	stream := model.NewQRecordStream(1024)

	errors := make(chan error, 1)
	defer close(errors)

	go func() {
		_, err := qe.ExecuteAndProcessQueryStream(stream, query, args...)
		if err != nil {
			log.Errorf("[pg_query_executor] failed to execute and process query stream: %v", err)
			errors <- err
		}
	}()

	select {
	case err := <-errors:
		return nil, err
	case schema := <-stream.SchemaChan():
		if schema.Err != nil {
			return nil, fmt.Errorf("failed to get schema from stream: %w", schema.Err)
		}
		batch := &model.QRecordBatch{
			NumRecords: 0,
			Records:    make([]*model.QRecord, 0),
			Schema:     schema.Schema,
		}
		for record := range stream.Records {
			if record.Err == nil {
				batch.Records = append(batch.Records, record.Record)
			} else {
				return nil, fmt.Errorf("[pg] failed to get record from stream: %w", record.Err)
			}
		}
		batch.NumRecords = uint32(len(batch.Records))
		return batch, nil
	}
}

func (qe *QRepQueryExecutor) ExecuteAndProcessQueryStream(
	stream *model.QRecordStream,
	query string,
	args ...interface{},
) (int, error) {
	defer close(stream.Records)

	tx, err := qe.pool.BeginTx(qe.ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		log.Errorf("[pg_query_executor] failed to begin transaction: %v", err)
		return 0, fmt.Errorf("[pg_query_executor] failed to begin transaction: %w", err)
	}

	defer func() {
		err := tx.Rollback(qe.ctx)
		if err != nil && err != pgx.ErrTxClosed {
			log.Errorf("[pg_query_executor] failed to rollback transaction: %v", err)
		}
	}()

	if qe.snapshot != "" {
		_, err = tx.Exec(qe.ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", qe.snapshot))
		if err != nil {
			stream.Records <- &model.QRecordOrError{
				Err: fmt.Errorf("failed to set snapshot: %w", err),
			}
			log.Errorf("[pg_query_executor] failed to set snapshot: %v", err)
			return 0, fmt.Errorf("[pg_query_executor] failed to set snapshot: %w", err)
		}
	}

	randomUint, err := util.RandomUInt64()
	if err != nil {
		stream.Records <- &model.QRecordOrError{
			Err: fmt.Errorf("failed to generate random uint: %w", err),
		}
		return 0, fmt.Errorf("[pg_query_executor] failed to generate random uint: %w", err)
	}

	cursorName := fmt.Sprintf("peerdb_cursor_%d", randomUint)
	fetchSize := 1024 * 16 * 8

	cursorQuery := fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorName, query)
	_, err = tx.Exec(qe.ctx, cursorQuery, args...)
	if err != nil {
		stream.Records <- &model.QRecordOrError{
			Err: fmt.Errorf("failed to declare cursor: %w", err),
		}
		log.Errorf("[pg_query_executor] failed to declare cursor: %v", err)
		return 0, fmt.Errorf("[pg_query_executor] failed to declare cursor: %w", err)
	}

	log.Infof("[pg_query_executor] declared cursor '%s' for query '%s'", cursorName, query)

	totalRecordsFetched := 0
	numFetchOpsComplete := 0
	for {
		numRows, err := qe.processFetchedRows(query, tx, cursorName, fetchSize, stream)
		if err != nil {
			return 0, err
		}

		log.Infof("[pg_query_executor] fetched %d rows for query '%s'", numRows, query)
		totalRecordsFetched += numRows

		if numRows == 0 {
			break
		}

		numFetchOpsComplete++
		qe.recordHeartbeat("#%d fetched %d rows", numFetchOpsComplete, numRows)
	}

	err = tx.Commit(qe.ctx)
	if err != nil {
		stream.Records <- &model.QRecordOrError{
			Err: fmt.Errorf("failed to commit transaction: %w", err),
		}
		return 0, fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
	}

	log.Infof("[pg_query_executor] committed transaction for query '%s', rows = %d",
		query, totalRecordsFetched)
	return totalRecordsFetched, nil
}

func mapRowToQRecord(row pgx.Rows, fds []pgconn.FieldDescription) (*model.QRecord, error) {
	// make vals an empty array of QValue of size len(fds)
	record := model.NewQRecord(len(fds))

	values, err := row.Values()
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, fd := range fds {
		tmp, err := parseFieldFromPostgresOID(fd.DataTypeOID, values[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse field: %w", err)
		}
		record.Set(i, *tmp)
	}

	return record, nil
}
