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

func (stream RecordStreamSink) CopyInto(ctx context.Context, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	return tx.CopyFrom(ctx, table, stream.GetColumnNames(), model.NewQRecordCopyFromSource(stream.QRecordStream))
}

func (stream RecordStreamSink) GetColumnNames() []string {
	return stream.Schema().GetColumnNames()
}

func (p PgCopyWriter) SetSchema(schema []pgconn.FieldDescription) {
	if !p.schema.schemaSet {
		p.schema.schema = schema
		close(p.schema.schemaLatch)
		p.schema.schemaSet = true
	}
}

func (p PgCopyWriter) IsSchemaSet() bool {
	return p.schema.schemaSet
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
	norows, err := tx.Query(ctx, schemaQuery, args...)
	if err != nil {
		return 0, err
	}
	fieldDescriptions := norows.FieldDescriptions()
	cols := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		cols = append(cols, QuoteIdentifier(fd.Name))
	}
	if !p.IsSchemaSet() {
		p.SetSchema(fieldDescriptions)
	}
	norows.Close()

	// TODO use pgx simple query arg parsing code (it's internal, need to copy)
	// TODO correctly interpolate
	for i, arg := range args {
		query = strings.Replace(query, fmt.Sprintf("$%d", i), fmt.Sprint(arg), -1)
	}

	copyQuery := fmt.Sprintf("COPY %s (%s) TO STDOUT", query, strings.Join(cols, ","))
	qe.logger.Info(fmt.Sprintf("[pg_query_executor] executing cursor declaration for %v with args %v", copyQuery, args))
	if _, err := qe.conn.PgConn().CopyTo(ctx, p.PipeWriter, copyQuery); err != nil {
		qe.logger.Info("[pg_query_executor] failed to declare cursor",
			slog.String("copyQuery", copyQuery), slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to declare cursor: %w", err)
		p.Close(err)
		return 0, err
	}

	qe.logger.Info("Committing transaction")
	if err := tx.Commit(ctx); err != nil {
		qe.logger.Error("[pg_query_executor] failed to commit transaction", slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
		p.Close(err)
		return 0, err
	}

	// TODO row count, GET DIAGNOSTICS x = ROW_COUNT
	totalRecordsFetched := 0

	qe.logger.Info(fmt.Sprintf("[pg_query_executor] committed transaction for query '%s', rows = %d",
		query, totalRecordsFetched))
	return totalRecordsFetched, nil
}

func (p PgCopyWriter) Close(err error) {
	p.PipeWriter.CloseWithError(err)
}

func (p PgCopyReader) CopyInto(ctx context.Context, qe *QRepQueryExecutor, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	<-p.schema.schemaLatch
	cols := make([]string, 0, len(p.schema.schema))
	for _, fd := range p.schema.schema {
		cols = append(cols, QuoteIdentifier(fd.Name))
	}
	_, err := qe.conn.PgConn().CopyFrom(ctx, p.PipeReader, fmt.Sprintf("COPY %s (%s) FROM STDIN", table.Sanitize(), strings.Join(cols, ",")))
	return 0, err
}
