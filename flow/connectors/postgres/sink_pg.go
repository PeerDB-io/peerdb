package connpostgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/postgres/sanitize"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PgCopyShared struct {
	schemaLatch chan struct{}
	err         error
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
	schema := PgCopyShared{schemaLatch: make(chan struct{})}
	return PgCopyReader{PipeReader: read, schema: &schema},
		PgCopyWriter{PipeWriter: write, schema: &schema}
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
	args ...any,
) (int64, int64, error) {
	defer shared.RollbackTx(tx, qe.logger)

	if qe.snapshot != "" {
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+utils.QuoteLiteral(qe.snapshot)); err != nil {
			qe.logger.Error("[pg_query_executor] failed to set snapshot",
				slog.Any("error", err), slog.String("query", query))
			return 0, 0, fmt.Errorf("[pg_query_executor] failed to set snapshot: %w", err)
		}
	}

	norows, err := tx.Query(ctx, query+" limit 0", args...)
	if err != nil {
		return 0, 0, err
	}

	fieldDescriptions := norows.FieldDescriptions()
	cols := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		cols = append(cols, fd.Name)
	}
	p.SetSchema(cols)
	norows.Close()

	query, err = sanitize.SanitizeSQL(query, args...)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to apply parameters to copy subquery: %w", err)
	}

	copyQuery := fmt.Sprintf("COPY (%s) TO STDOUT", query)
	qe.logger.Info("[pg_query_executor] executing copy", slog.String("query", copyQuery))
	ct, err := qe.conn.PgConn().CopyTo(ctx, p.PipeWriter, copyQuery)
	if err != nil {
		qe.logger.Info("[pg_query_executor] failed to copy",
			slog.String("copyQuery", copyQuery), slog.Any("error", err))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to copy: %w", err)
	}

	qe.logger.Info("Committing transaction")
	if err := tx.Commit(ctx); err != nil {
		qe.logger.Error("[pg_query_executor] failed to commit transaction", slog.Any("error", err))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
	}

	totalRecordsFetched := ct.RowsAffected()
	qe.logger.Info(fmt.Sprintf("[pg_query_executor] committed transaction for query '%s', rows = %d",
		query, totalRecordsFetched))
	return totalRecordsFetched, 0, nil
}

func (p PgCopyWriter) Close(err error) {
	p.PipeWriter.CloseWithError(err)
	p.schema.err = err
	p.SetSchema(nil)
}

func (p PgCopyReader) GetColumnNames() ([]string, error) {
	<-p.schema.schemaLatch
	return p.schema.schema, p.schema.err
}

func (p PgCopyReader) CopyInto(ctx context.Context, c *PostgresConnector, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	cols, err := p.GetColumnNames()
	if err != nil {
		return 0, err
	}
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, utils.QuoteIdentifier(col))
	}
	ct, err := tx.Conn().PgConn().CopyFrom(
		ctx,
		p.PipeReader,
		fmt.Sprintf("COPY %s (%s) FROM STDIN", table.Sanitize(), strings.Join(quotedCols, ",")),
	)
	return ct.RowsAffected(), err
}
