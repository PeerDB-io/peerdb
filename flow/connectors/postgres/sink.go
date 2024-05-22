package connpostgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type QuerySinkWriter interface {
	Close(error)
	ExecuteQueryWithTx(context.Context, *QRepQueryExecutor, pgx.Tx, string, ...interface{}) (int, error)
}

type QuerySinkReader interface {
	GetColumnNames() []string
	CopyInto(context.Context, pgx.Tx, pgx.Identifier) (int64, error)
}

type RecordStreamSink struct {
	*model.QRecordStream
}

type PgCopyShared struct {
	schema      []pgconn.FieldDescription
	schemaSet   bool
	schemaLatch chan struct{}
	err         error
}

type PgCopyWriter struct {
	*io.PipeWriter
	schema *PgCopyShared
}

type PgCopyReader struct {
	*io.PipeReader
	schema *PgCopyShared
}

func NewPgCopyPipe() (PgCopyReader, PgCopyWriter) {
	read, write := io.Pipe()
	schema := PgCopyShared{}
	return PgCopyReader{PipeReader: read, schema: &schema},
		PgCopyWriter{PipeWriter: write, schema: &schema}
}

func (stream RecordStreamSink) ExecuteQueryWithTx(
	ctx context.Context,
	qe *QRepQueryExecutor,
	tx pgx.Tx,
	query string,
	args ...interface{},
) (int, error) {
	defer shared.RollbackTx(tx, qe.logger)

	if qe.snapshot != "" {
		_, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+QuoteLiteral(qe.snapshot))
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to set snapshot",
				slog.Any("error", err), slog.String("query", query))
			err := fmt.Errorf("[pg_query_executor] failed to set snapshot: %w", err)
			stream.Close(err)
			return 0, err
		}
	}

	randomUint, err := shared.RandomUInt64()
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to generate random uint", slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to generate random uint: %w", err)
		stream.Close(err)
		return 0, err
	}

	cursorName := fmt.Sprintf("peerdb_cursor_%d", randomUint)
	fetchSize := shared.FetchAndChannelSize
	cursorQuery := fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorName, query)
	qe.logger.Info(fmt.Sprintf("[pg_query_executor] executing cursor declaration for %v with args %v", cursorQuery, args))
	_, err = tx.Exec(ctx, cursorQuery, args...)
	if err != nil {
		qe.logger.Info("[pg_query_executor] failed to declare cursor",
			slog.String("cursorQuery", cursorQuery), slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to declare cursor: %w", err)
		stream.Close(err)
		return 0, err
	}

	qe.logger.Info(fmt.Sprintf("[pg_query_executor] declared cursor '%s' for query '%s'", cursorName, query))

	totalRecordsFetched := 0
	for {
		numRows, err := qe.processFetchedRows(ctx, query, tx, cursorName, fetchSize, stream.QRecordStream)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to process fetched rows", slog.Any("error", err))
			return 0, err
		}

		qe.logger.Info(fmt.Sprintf("[pg_query_executor] fetched %d rows for query '%s'", numRows, query))
		totalRecordsFetched += numRows

		if numRows == 0 {
			break
		}
	}

	qe.logger.Info("Committing transaction")
	if err := tx.Commit(ctx); err != nil {
		qe.logger.Error("[pg_query_executor] failed to commit transaction", slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
		stream.Close(err)
		return 0, err
	}

	qe.logger.Info(fmt.Sprintf("[pg_query_executor] committed transaction for query '%s', rows = %d",
		query, totalRecordsFetched))
	return totalRecordsFetched, nil
}

func (p PgCopyWriter) ExecuteQueryWithTx(
	ctx context.Context,
	qe *QRepQueryExecutor,
	tx pgx.Tx,
	query string,
	args ...interface{},
) (int, error) {
	defer shared.RollbackTx(tx, qe.logger)

	if qe.snapshot != "" {
		_, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+QuoteLiteral(qe.snapshot))
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to set snapshot",
				slog.Any("error", err), slog.String("query", query))
			err := fmt.Errorf("[pg_query_executor] failed to set snapshot: %w", err)
			p.Close(err)
			return 0, err
		}
	}

	schemaQuery := query + " limit 0"
	norows, err := tx.Query(ctx, schemaQuery, ...args)
	if err != nil {
		return 0, err
	}
	fieldDescriptions := norows.FieldDescriptions()
	cols := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		cols = append(cols, fd.Name)
	}
	if !p.schema.IsSchemaSet() {
		p.schema.SetSchema(fieldDescriptions)
	}
	norows.Close()
	copyQuery := fmt.Sprintf("COPY %s (%s) TO STDOUT", query, strings.Join(cols, ","))
	qe.logger.Info(fmt.Sprintf("[pg_query_executor] executing cursor declaration for %v with args %v", copyQuery, args))
	if _, err := qe.conn.PgConn().CopyTo(ctx, copyQuery, args...); err != nil {
		qe.logger.Info("[pg_query_executor] failed to declare cursor",
			slog.String("cursorQuery", cursorQuery), slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to declare cursor: %w", err)
		p.Close(err)
		return 0, err
	}

	qe.logger.Info(fmt.Sprintf("[pg_query_executor] declared cursor '%s' for query '%s'", cursorName, query))

	totalRecordsFetched := 0
	for {
		numRows, err := qe.processFetchedRows(ctx, query, tx, cursorName, fetchSize, stream.QRecordStream)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to process fetched rows", slog.Any("error", err))
			return 0, err
		}

		qe.logger.Info(fmt.Sprintf("[pg_query_executor] fetched %d rows for query '%s'", numRows, query))
		totalRecordsFetched += numRows

		if numRows == 0 {
			break
		}
	}

	qe.logger.Info("Committing transaction")
	if err := tx.Commit(ctx); err != nil {
		qe.logger.Error("[pg_query_executor] failed to commit transaction", slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
		p.Close(err)
		return 0, err
	}

	qe.logger.Info(fmt.Sprintf("[pg_query_executor] committed transaction for query '%s', rows = %d",
		query, totalRecordsFetched))
	return totalRecordsFetched, nil
}

func (stream RecordStreamSink) GetColumnNames() []string {
	return stream.Schema().GetColumnNames()
}

func (p PgCopyWriter) Close(err error) {
	p.PipeWriter.CloseWithError(err)
}
