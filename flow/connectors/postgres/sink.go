package connpostgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type QuerySinkWriter interface {
	Close(error)
	ExecuteQueryWithTx(context.Context, *QRepQueryExecutor, pgx.Tx, string, ...interface{}) (int, error)
}

type QuerySinkReader interface {
	GetColumnNames() []string
	CopyInto(context.Context, *PostgresConnector, pgx.Tx, pgx.Identifier) (int64, error)
}

type RecordStreamSink struct {
	*model.QRecordStream
}

type PgCopyShared struct {
	schemaLatch chan struct{}
	schema      []string
	schemaSet   bool
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
	schema := PgCopyShared{schemaLatch: make(chan struct{}, 0)}
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

func (stream RecordStreamSink) CopyInto(ctx context.Context, _ *PostgresConnector, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	return tx.CopyFrom(ctx, table, stream.GetColumnNames(), model.NewQRecordCopyFromSource(stream.QRecordStream))
}

func (stream RecordStreamSink) GetColumnNames() []string {
	return stream.Schema().GetColumnNames()
}

func (p PgCopyWriter) SetSchema(schema []string) {
	if !p.schema.schemaSet {
		p.schema.schema = schema
		close(p.schema.schemaLatch)
		p.schema.schemaSet = true
	}
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

	norows, err := tx.Query(ctx, query+" limit 0", args...)
	if err != nil {
		return 0, err
	}

	fieldDescriptions := norows.FieldDescriptions()
	cols := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		cols = append(cols, QuoteIdentifier(fd.Name))
	}
	p.SetSchema(cols)
	norows.Close()

	// TODO use pgx simple query arg parsing code (it's internal, need to copy)
	// TODO correctly interpolate
	for i, arg := range args {
		query = strings.ReplaceAll(query, fmt.Sprintf("$%d", i), fmt.Sprint(arg))
	}

	copyQuery := fmt.Sprintf("COPY %s (%s) TO STDOUT", query, strings.Join(cols, ","))
	qe.logger.Info(fmt.Sprintf("[pg_query_executor] executing cursor declaration for %v with args %v", copyQuery, args))
	if _, err := qe.conn.PgConn().CopyTo(ctx, p.PipeWriter, copyQuery); err != nil {
		qe.logger.Info("[pg_query_executor] failed to copy",
			slog.String("copyQuery", copyQuery), slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to copy: %w", err)
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

func (p PgCopyReader) GetColumnNames() []string {
	return p.schema.schema
}

func (p PgCopyReader) CopyInto(ctx context.Context, c *PostgresConnector, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	<-p.schema.schemaLatch
	_, err := c.conn.PgConn().CopyFrom(
		ctx,
		p.PipeReader,
		fmt.Sprintf("COPY %s (%s) FROM STDIN", table.Sanitize(), strings.Join(p.schema.schema, ",")),
	)
	return 0, err
}
